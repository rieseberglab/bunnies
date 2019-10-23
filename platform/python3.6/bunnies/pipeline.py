"""
  Tools to generate build pipelines.
  The utilities here take a pipeline specification and convert
  them into a list of tasks to execute.
"""
from . import runtime
from . import exc
from . import constants
from .version import __version__
from .graph import Cacheable, Transform, Target
from .environment import ComputeEnv
from .transfers import s3_streaming_put
from .config import config
from .scheduler import Scheduler

from datetime import datetime
import json
import logging
import io
import os.path

log = logging.getLogger(__name__)


def _get_default_region():
    if not _get_default_region.cached:
        import boto3
        sess = boto3.Session()
        _get_default_region.cached = sess.region_name
    return _get_default_region.cached


_get_default_region.cached = None


class PipelineException(Exception):
    pass


class BuildNode(object):
    """graph of buildable things with dependencies"""
    __slots__ = ("data", "deps", "_uid", "_output_ready", "_jobdef", "_sched_node", "_job", "_usage")

    def __init__(self, data):
        self.data = data  # Cacheable
        self.deps = []    # BuildNodes
        self._uid = None
        self._output_ready = None
        self._jobdef = None
        self._sched_node = None
        self._job = None
        self._usage = []

    @property
    def uid(self):
        if not self._uid:
            if isinstance(self.data, Cacheable):
                self._uid = self.data.canonical_id
                if not isinstance(self._uid, str):
                    raise ValueError("Node %s computes a non string canonical id: %s", self.data, self._uid)
            else:
                self._uid = id(self.data)

        return self._uid

    @property
    def job_id(self):
        if isinstance(self.data, Transform):
            return self.data.name + "-" + self.uid
        else:
            return self.uid

    def postorder(self, prune_fn=lambda x: False):
        """yield each node in DFS post order.
           use prune_fn() to prune out this node and its depencies

            - stop if prune_fn(self) == True
            - recurse into dependencies, yielding in turn
            - yield self
        """
        if prune_fn(self):
            return

        for dep in self.deps:
            for node in dep.postorder(prune_fn=prune_fn):
                yield node

        yield self

    @property
    def output_ready(self):
        """determines if the buildnode's output is readily available for consumption by downstream analyses"""
        if self._output_ready is None:
            if not isinstance(self.data, Target):
                raise TypeError("only valid on Targets")
            self._output_ready = self.data.exists()
        return self._output_ready

    def job_done(self, success):
        """
        must be called when the currently submitted job is completed.
        we update the total job usage, (and save logs).
        """
        run_usage = self._job.get_usage()
        self._usage.append(run_usage)

    def register_job_definition(self, compute_env):
        # FIXME -- the compute environment should register the job definition lazily.
        #          It's a bit odd to do it in two separate steps, especially if most
        #          resource settings are overridden in the first place.
        if isinstance(self.data, Transform):
            template = self.data.task_template(compute_env)
            if template.get('jobtype', None) != "batch":
                raise exc.BunniesException("job %s has an unknown template jobtype: %s", template.get('jobtype', None))
            if not template.get('image', None):
                raise exc.BunniesException("job %s has an invalid template image: %s", template.get('image', None))

            self._jobdef = compute_env.register_simple_batch_jobdef(self.data.name, template.get('image'))
            return
        log.warn("no job definition registered for build node: %s", self.data)

    def schedule(self, compute_env, scheduler_node):
        if not isinstance(self.data, Transform):
            raise NotImplementedError("cannot schedule non-Transform objects")

        # fixme calculate attempts:
        max_attempts = 1
        if len(scheduler_node.failures) >= max_attempts:
            scheduler_node.cancel()
            return

        assert self._job is None

        # this id is globally unique
        job_id = self.job_id

        # let the user's object calculate its resource requirements
        resources = self.data.task_resources() or {}

        remote_script_url = "s3://%(bucket)s/jobs/%(envname)s/%(jobid)s/jobscript" % {
            "bucket": config['storage']['tmp_bucket'],
            "envname": compute_env.name,
            "jobid": job_id
        }

        user_deps_prefix = "s3://%(bucket)s/user_context/%(envname)s/" % {
            "bucket": config['storage']['tmp_bucket'],
            "envname": compute_env.name,
            "jobid": job_id
        }

        user_deps_url = runtime.upload_user_context(user_deps_prefix)

        with io.BytesIO() as exec_fp:
            script_len = exec_fp.write(self.execution_transfer_script(resources).encode('utf-8'))
            exec_fp.seek(0)
            log.debug("uploading job script for job_id %s at %s ...", job_id, remote_script_url)
            s3_streaming_put(exec_fp, remote_script_url, content_type="text/x-python", content_length=script_len,
                             logprefix=job_id + " jobscript ")

        settings = {
            'vcpus': resources.get('vcpus', None),
            'memory': resources.get('memory', None),
            'timeout': resources.get('timeout', -1),
            'environment': {
                "BUNNIES_VERSION": __version__,
                "BUNNIES_SUBMIT_TIME": str(int(datetime.utcnow().timestamp()*1000)),
                "BUNNIES_TRANSFER_SCRIPT": remote_script_url,
                "BUNNIES_USER_DEPS": user_deps_url,
                "BUNNIES_JOBID": job_id,
                "BUNNIES_ATTEMPT": "%d %d" % (1, max_attempts),
                "BUNNIES_RESULT": os.path.join(self.data.output_prefix(), constants.TRANSFORM_RESULT_FILE)
            }
        }

        if settings.get('timeout') <= 0:
            settings['timeout'] = 24*3600*7 # 7 days

        self._job = compute_env.submit_simple_batch_job(job_id, self._jobdef, **settings)
        scheduler_node.submit()
        return

    def execution_transfer_script(self, resources):
        """
        Create a self-standing script that executes just the one node.
        """

        # fixme -- json is an inefficient pickle.
        #          - slow
        #          - nodes will need to be dealiased again.
        #
        manifest_s = repr(json.dumps(self.data.manifest()))
        canonical_s = repr(json.dumps(self.data.canonical()))
        resources_s = repr(resources)

        # this doesn't get carried on the other side
        default_region = _get_default_region()

        hooks = "\n".join(runtime.add_user_hook._hooks)

        return """#!/usr/bin/env python3
import bunnies.runtime
import bunnies.constants as C
from bunnies.unmarshall import unmarshall
import os, os.path
import json
import logging
log = logging.getLogger()

# USER HOOKS START
%(hooks)s
# USER HOOKS END

uid_s       = %(uid_s)s

canonical_s = %(canonical_s)s

manifest_s  = %(manifest_s)s

manifest_obj = json.loads(manifest_s)

canonical_obj = json.loads(canonical_s)

bunnies.setup_logging()

# this is for align binaries
os.environ['AWS_REGION'] = %(default_region)s
# this is for the boto client
os.environ['AWS_DEFAULT_REGION'] = %(default_region)s

transform = unmarshall(manifest_obj)
log.info("%%s", json.dumps(manifest_obj, indent=4))

params = {
        'workdir': os.environ.get('BUNNIES_WORKDIR'),
        'scriptdir': os.path.dirname(__file__),
        'job_id': os.environ.get('BUNNIES_JOBID'),
        'resources': %(resources_s)s
        }

bunnies.runtime.setenv_batch_metadata()
env_copy = dict(os.environ)

# core of the work
output = transform.run(**params)

# write results
result_path = os.path.join(transform.output_prefix(), C.TRANSFORM_RESULT_FILE)
bunnies.runtime.update_result(result_path,
        output=output,
        manifest=manifest_obj,
        canonical=canonical_obj,
        environment=env_copy)
""" % {
    'manifest_s': manifest_s,
    'canonical_s': canonical_s,
    'default_region': repr(default_region),
    'uid_s': repr(self.uid),
    'hooks': hooks,
    'resources_s': resources_s
}


class BuildGraph(object):
    """Traverse the pipeline graph and obtain a graph of data dependencies"""

    def __init__(self):

        # cache of build nodes, by uid
        self.by_uid = {}

        # roots to build
        self.targets = []

        self.scheduler = Scheduler()


    def _dealias(self, obj, path=None):
        """
           walk the graph, making sure there are no cycles,
           and dealias any Cacheable object along the way.
           The result is a BuildNode overlay on top of the data graph.
        """
        if path is None:
            path = set()

        def _path_string(p):
            return ", ".join([str(entry) for entry in p])

        # recurse in basic structures
        if isinstance(obj, list):
            return [self._dealias(o, path=path) for o in obj]
        if isinstance(obj, dict):
            return {k: self._dealias(v, path=path) for k, v in obj.items()}
        if isinstance(obj, tuple):
            return tuple([self._dealias(o, path=path) for o in obj])

        if not isinstance(obj, Cacheable):
            raise PipelineException("pipeline targets and their dependencies should be cacheable: %s (path=%s)" % (repr(obj), _path_string(path)))

        # dealias object based on its uid

        if obj in path:
            # produce ordered proof of cycle
            raise PipelineException("Cycle in dependency graph detected: %s", _path_string(path))

        path.add(obj)

        # fixme modularity -- need a "getDeps" interface
        if isinstance(obj, Transform):
            # recurse
            dealiased_deps = [self._dealias(obj.inputs[k].node, path=path) for k in obj.inputs]
        else:
            dealiased_deps = []

        path.remove(obj)

        build_node = BuildNode(obj)
        dealiased = self.by_uid.get(build_node.uid, None)

        if not dealiased:
            # first instance
            dealiased = build_node
            dealiased.deps = dealiased_deps

            self.by_uid[dealiased.uid] = dealiased

        return dealiased

    def add_targets(self, targets):
        if not isinstance(targets, (list, tuple)):
            targets = list([targets])

        all_targets = self.targets + self._dealias(targets)
        self.targets[:] = [x for x in set(all_targets)]

    def dependency_order(self):
        """generate the nodes of the graph in dependency order (if A depends on B, then B is yielded first).

           Nodes of the graph are visited once only.
        """
        seen = set()

        def _prune_visited(node):
            if node in seen:
                return True
            seen.add(node)
            return False

        for target in self.targets:
            if target in seen:
                continue
            for node in target.postorder(prune_fn=_prune_visited):
                yield node.data

    def build_order(self):
        """generate nodes in the graph that need to be built.
           if A depends on B, B is yielded first

           nodes that have already been built are skipped
        """
        seen = set()

        def _already_built(node):
            # visit only once
            if node in seen:
                return True
            seen.add(node)

            # prune if the result is already computed
            if node.output_ready:
                return True

            return False

        for target in self.targets:
            if target in seen:
                continue
            for node in target.postorder(prune_fn=_already_built):
                yield node

    def build(self, run_name):
        """run all the jobs that need to be run. managed resources created to build the chosen targets will be tagged with the
        given run_name. reusing the same name on a subsequent run will allow resources to be reused.
        """

        compute_env = ComputeEnv(run_name)

        nodei = -1
        for nodei, node in enumerate(self.build_order()):
            # check compatibility with compute_environment
            # wrap container images.
            # create schedulable entities
            job_def = node.register_job_definition(compute_env)

            sched_node = self.scheduler.add_node(node.job_id, node)
            node._sched_node = sched_node

            # inform scheduler of graph dependencies
            for dep in node.deps:
                # nodes are iterated in dependency order. Just consider
                # nodes which have to be built. skip those which aren't
                # part of build order
                if dep._sched_node:
                    sched_node.depends_on(dep._sched_node)

        num_jobs = nodei + 1
        log.info("current number of jobs: %d", num_jobs)

        log.info("initializing build graph...")
        self.scheduler.initialize()
        log.info("build graph initialized.")

        if num_jobs == 0:
            log.info("graph already built")
            return

        # we'll have to execute jobs
        compute_env.create()
        compute_env.wait_ready()

        def _print_execution_status(status_map):
            num_shown = 5
            for status in sorted(status_map.keys()):
                log.info("compute environment summary:")
                job_summary = [job_id for (job_id, _) in status_map[status][0:num_shown]]
                log.info("    %-10s (%-3d): %s ...", status.lower(), len(status_map[status]), job_summary)

        def _wait_for_all_jobs(status_map):
            """we're waiting for all submitted jobs to complete (success/failure)"""
            _print_execution_status(status_map)
            incomplete_statuses = [status for status in status_map.keys() if status not in ('SUCCEEDED', 'FAILED')]
            if incomplete_statuses:
                incomplete_count = 0
                for incomplete_status in incomplete_statuses:
                    incomplete_count += len(status_map[incomplete_status])
                log.info("waiting for %d more job(s) to complete...", incomplete_count)
                return False
            return True

        def _wait_for_some_jobs(status_map):
            """we're waiting for at least one job to complete (success/failure)"""
            _print_execution_status(status_map)
            if 'SUCCEEDED' in status_map or 'FAILED' in status_map:
                return True
            return False

        status = {}

        # build loop
        while True:
            status = self.scheduler.status()

            if not (status['ready'] or status['waiting'] or status['submitted']):
                # all done
                log.info("schedule complete")
                break

            # process ready nodes
            if status['ready']:
                for sched_node in status['ready']:
                    build_node = sched_node.data
                    build_node.schedule(compute_env, sched_node)
                # jobs have either been submitted or cancelled. states have
                # propagated
                continue

            log.info("job state summary:")
            for state_name in sorted(status.keys()):
                log.info("    %-10s: %d job(s)", state_name, len(status[state_name]))

            if status['submitted']:
                # check for status of completed jobs
                exec_completion = compute_env.wait_for_jobs(condition=_wait_for_some_jobs)

                success_jobs = exec_completion.get('SUCCEEDED', [])
                for update_job, _ in success_jobs:
                    sched_node = self.scheduler.get_node(update_job.name)
                    sched_node.data.job_done(True)
                    sched_node.done()

                failed_jobs = exec_completion.get('FAILED', [])
                for update_job, update_reason in failed_jobs:
                    sched_node = self.scheduler.get_node(update_job.name)
                    sched_node.data.job_done(False)
                    sched_node.failed(update_reason)

        if status['cancelled']:
            failed_build_nodes = [cancelled.data for cancelled in status['cancelled']
                                  if len(cancelled.failures) > 0]
            if failed_build_nodes:
                log.info("failed jobs (%3d):", len(failed_build_nodes))
                for failed_node in failed_build_nodes:
                    log.info("    job_id=%s name=%s reasons=%s",
                             failed_node._job.job_id, failed_node.job_id,
                             failed_node._sched_node.failures)

            raise exc.BuildException("one or more targets failed to build")
        else:
            log.info("all targets were successfully built")


def build_pipeline(roots):
    """
    Construct a pipeline execution schedule from a graph of
    data dependencies.
    """
    if not isinstance(roots, list):
        roots = [roots]

    build_graph = BuildGraph()
    build_graph.add_targets(roots)

    return build_graph
