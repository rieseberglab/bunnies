#!/usr/bin/env python3
import boto3
from .constants import PLATFORM
from .utils import data_files
from .containers import wrap_user_image
from .config import config

import json
import logging
import os.path
import botocore.waiter
import time
from datetime import datetime, timedelta

from botocore.exceptions import ClientError
logger = logging.getLogger(__name__)


class AWSBatchSimpleJobDef(object):
    def __init__(self, name, image_name):
        self.name = name
        self.src_image = image_name
        self.platform_image = None
        self.jobdef = None

    def register(self, compute_env):
        """register the job definition for this image on the given compute environment

           the job definition will be configured with the mount points defined in the compute environment.
        """

        #
        # mount all available volumes in the container
        #
        volumes = [{'name': disk['name'], 'host_src': disk['instance_mountpoint'], 'dst': os.path.join("/", disk['name'])}
                   for disk in compute_env.disks.values()]

        # wrap the user's image with the platform harness
        self.platform_image = wrap_user_image(self.src_image)

        role = config['job_role_arn']
        self.jobdef = make_jobdef(PLATFORM + "-" + self.name, role, self.platform_image, mounts=volumes, reuse=True)

    @property
    def arn(self):
        return self.jobdef['jobDefinitionArn']


class AWSBatchSimpleJob(object):
    def __init__(self, name, jobdef, **overrides):
        """
        Overrides (optional):
           command: [str, str, str]
           vcpus: int
           memory: int (MiB)
           environment: {k:v, k:v}
           attempts: int
           timeout: int (seconds)
        """
        self.name = name
        if isinstance(jobdef, str):
            self.jobdef_arn = jobdef
        elif isinstance(jobdef, AWSBatchSimpleJobDef):
            self.jobdef_arn = jobdef.arn

        self.overrides = overrides
        self.job = None

    @classmethod
    def from_job_id(cls, job_id):
        client = boto3.client('batch')
        job_descs = client.describe_jobs(
            jobs=[job_id]
        )['jobs']
        if not job_descs:
            return None
        job_desc = job_descs[0]
        job_name = job_desc['jobName']
        job_def = job_desc['jobDefinition']
        inst = cls(job_name, job_def)
        inst.job_id = job_id

        # FIXME extract overrides: memory, vcpu, timeout, etc.
        return inst

    def submit(self, queue_arn):
        self.job_id = submit_job(self.name, queue_arn, self.jobdef_arn, **self.overrides)['jobId']
        return self.job_id

    def get_status(self, attempt=-1):
        """
        {
                    "startedAt": 1566241967286,
                    "stoppedAt": 1566241967507,
                    "container": {
                        "logStreamName": "bunnies-align/default/fb360c99-f236-4dcb-b26b-a388f905763a",
                        "networkInterfaces": [],
                        "containerInstanceArn": "arn:aws:ecs:us-west-2:879518704116:container-instance/2bffc3ed-8bdf-4452-9297-978bdcfd572a",
                        "exitCode": 2,
                        "taskArn": "arn:aws:ecs:us-west-2:879518704116:task/fb360c99-f236-4dcb-b26b-a388f905763a"
                    },
                    "statusReason": "Essential container in task exited"
        }
        """
        client = boto3.client('batch')
        job_desc = client.describe_jobs(
            jobs=[self.job_id]
        )['jobs'][0]

        if not job_desc['attempts']:
            logger.debug("job-desc: %s", job_desc)
            return None

        attempt = job_desc['attempts'][attempt]
        attempt['createdAt'] = job_desc['createdAt']
        return attempt

    def log_stream(self, attempt=-1, startTime=None, endTime=None, startFromHead=False):
        """yields each log event of the job,

           the logstream is only available when the job reaches RUNNING state.

           startTime and endTime are in ms.
        """
        if self.job_id is None:
            return None

        client = boto3.client('batch')
        job_desc = client.describe_jobs(
            jobs=[self.job_id]
        )['jobs'][0]

        if not job_desc['attempts']:
            logger.info("no job attempts: %s", job_desc)
            return

        attempt = job_desc['attempts'][attempt]
        container_logs = attempt['container']['logStreamName']
        # containerInstanceArn would likely allow obtaining logs for the instance.

        client = boto3.client('logs')
        log_group_name = "/aws/batch/job"

        extra = {}
        if startTime is not None:
            extra['startTime'] = startTime
        if endTime is not None:
            extra['endTime'] = endTime

        extra['startFromHead'] = startFromHead

        token=None
        tokenKey = 'nextForwardToken' if startFromHead else 'nextBackwardToken'
        events = ['_']
        while True:
            resp = client.get_log_events(logGroupName=log_group_name, logStreamName=container_logs,
                                         **extra)
            if not resp['events']:
                return

            for event in resp['events']:
                yield event

            extra["nextToken"] = resp[tokenKey]

def _custom_waiters():
    if not _custom_waiters.model:
        waiters = {
            "JobQueueReady": {
                "delay": 15,
                "operation": "DescribeJobQueues",
                "maxAttempts": 40,
                "acceptors": [
                    {
                        "expected": "DISABLED",
                        "matcher": "pathAny",
                        "state": "failure",
                        "argument": "jobQueues[].state"
                    },
                    {
                        "expected": "CREATING",
                        "matcher": "pathAny",
                        "state": "retry",
                        "argument": "jobQueues[].status"
                    },
                    {
                        "expected": "UPDATING",
                        "matcher": "pathAny",
                        "state": "retry",
                        "argument": "jobQueues[].status"
                    },
                    {
                        "expected": "DELETING",
                        "matcher": "pathAny",
                        "state": "failure",
                        "argument": "jobQueues[].status"
                    },
                    {
                        "expected": "DELETED",
                        "matcher": "pathAny",
                        "state": "failure",
                        "argument": "jobQueues[].status"
                    },
                    {
                        "expected": "INVALID",
                        "matcher": "pathAny",
                        "state": "failure",
                        "argument": "jobQueues[].status"
                    },
                    {
                        "expected": "VALID",
                        "matcher": "pathAll",
                        "state": "success",
                        "argument": "jobQueues[].status"
                    },
                    {
                        "matcher": "path",
                        "expected": True,
                        "argument": "length(jobQueues[]) > `0`",
                        "state": "failure"
                    }
                ]
            },
        }
        model = botocore.waiter.WaiterModel({
            "version": 2,
            "waiters": waiters
        })
        _custom_waiters.model = model
    return _custom_waiters.model


_custom_waiters.model = None


def create_job_role():
    ecs_role_name = PLATFORM + "-ecs"

    jobs_ecs_trust = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "",
                "Effect": "Allow",
                "Principal": {
                    "Service": "ecs-tasks.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }

    client = boto3.client("iam")

    logger.info("creating IAM role %s", ecs_role_name)
    try:
        client.create_role(Path='/',
                           RoleName=ecs_role_name,
                           AssumeRolePolicyDocument=json.dumps(jobs_ecs_trust))
        logger.info("IAM role %s created", ecs_role_name)
    except ClientError as clierr:
        if clierr.response['Error']['Code'] == 'EntityAlreadyExists':
            logger.info("using existing role %s", ecs_role_name)
            pass
        else:
            raise

    for pfile in data_files(os.path.join("permissions", PLATFORM + "-ecs-*-permissions.json")):
        basename = os.path.basename(pfile)
        noext = os.path.splitext(basename)[0]
        logger.info("adding policy %s to role %s", os.path.basename(pfile), ecs_role_name)
        with open(pfile, "rb") as pfd:
            policy = pfd.read().decode('utf-8')
            client.put_role_policy(RoleName=ecs_role_name,
                                   PolicyName=noext,
                                   PolicyDocument=policy)
    return client.get_role(RoleName=ecs_role_name)


def get_jobqueue(name):
    client = boto3.client('batch')
    jq = client.describe_job_queues(
        jobQueues=[name]
    )
    return jq['jobQueues'][0]


def wait_queue_ready(queueNames):
    """wait for a job queue to be in the READY+VALID state"""
    logger.info("waiting for queue(s) %s to be ready...", queueNames)
    client = boto3.client('batch')
    waiter = botocore.waiter.create_waiter_with_client("JobQueueReady", _custom_waiters(), client)
    waiter.wait(jobQueues=queueNames)
    logger.info("queue(s) %s ready", queueNames)


def make_jobqueue(name, priority=100, compute_envs=()):
    """
       create a jobqueue with the given name. if it already exists
       it is returned.

       args:
           name of queue
       priority: int

       compute_envs:  ((str_name, int_order),...)
          the compute environemnts to associate with the queue
    """
    if not compute_envs or len(compute_envs) == 0:
        raise ValueError("must specify at least one compute environment")

    client = boto3.client('batch')
    try:
        jq = client.create_job_queue(state='ENABLED',
                                     jobQueueName=name,
                                     priority=priority,
                                     computeEnvironmentOrder=[
                                         {
                                             "order": order,
                                             "computeEnvironment": ce_name
                                         } for (ce_name, order) in compute_envs
                                     ])
        logger.info("created job queue %s (Arn=%s)", name, jq['jobQueueArn'])
        return jq
    except ClientError as clierr:
        if clierr.response['ResponseMetadata']['HTTPStatusCode'] == 409:
            # conflict -- already exists
            existing = get_jobqueue(name)
            logger.info("job queue %s (Arn=%s) already exists. returning existing", existing['jobQueueName'], existing['jobQueueArn'])
            return existing
        raise


def make_jobdef(name, job_role_arn, image, vcpus=1, memory=128, mounts=None, reuse=True):
    """create/update a new job definition for a simple (single-container)
    job. if there already exists at least one revision of the given name,
    then the existing job definition is returned (reuse=True), or it is
    updated with the settings passed in (reuse=False).

    On reuse=False, there is a possible race condition on creation
    which will cause two revisions to be created simultaneously. In this case
    one of the make_jobdef calls will return revision 1, and the other, revision 2.
    Subsequent calls would reuse the latest, revision 2.

      name: name of the job (128 chars, [-a-zA-Z0-9_])
      jobroleArn: role to assign the ECS container that will be started
      image: the name of the container image
      vcpus: default number of vcpus
      memory: default amount of memory in MB
      mounts: [{ name: "foo", "host_src": "/path/on/host", "dst": "/path/in/container" }, ...]
    """
    client = boto3.client('batch')

    def _deep_matches(expected, obj):
        """extracts fields of the job definition that should be compared for equality"""
        if isinstance(expected, dict):
            if not isinstance(obj, dict):
                return False
            for k in expected:
                if k not in obj:
                    return False
                if not _deep_matches(expected[k], obj[k]):
                    return False
            return True
        if isinstance(expected, (list, tuple)):
            if not isinstance(obj, (list, tuple)):
                return False
            if len(expected) != len(obj):
                return False
            for i in range(0, len(expected)):
                if not _deep_matches(expected[i], obj[i]):
                    return False
            return True
        return expected == obj

    def jobdef_exists(client, match_spec):
        paginator = client.get_paginator('describe_job_definitions')
        def_iterator = paginator.paginate(jobDefinitionName=name)
        found = None
        for page in def_iterator:
            page_defs = page['jobDefinitions']
            if len(page_defs) == 0:
                break

            found = [pdef for pdef in page_defs if _deep_matches(match_spec, pdef)]
            if found:
                break
        if not found:
            return None
        return found[0]

    volumes = [
        {
            'host': {
                'sourcePath': mnt['host_src']
            },
            'name': mnt['name'] + "vol"
        }
        for mnt in mounts
    ]
    mountPoints = [
        {
            'containerPath': mnt['dst'],
            'readOnly': False,
            'sourceVolume': mnt['name'] + "vol"
        }
        for mnt in mounts
    ]

    new_def = {
        'jobDefinitionName': name,
        'type': 'container',
        'containerProperties': {
            'image': image,
            'jobRoleArn': job_role_arn,
            'volumes': volumes,
            'mountPoints': mountPoints,
            'privileged': False,
            'ulimits': [
                { 'name': "core",
                  'hardLimit': 0,
                  'softLimit': 0
                }
            ],
            'user': 'root'
        }
    }

    if reuse:
        existing = jobdef_exists(client, new_def)
        if existing:
            logger.info("reusing existing job definition %s (Arn=%s)", name, existing['jobDefinitionArn'])
            return existing

    logger.info("creating job definition %s with image %s...", name, image)
    new_def['containerProperties'].update({
        'vcpus': vcpus,
        'memory': memory
    })
    new_def.update({
        "retryStrategy": {
            'attempts': 1
        },
        "timeout": {
            'attemptDurationSeconds': 600
        }
    })

    #
    # returns:
    # {
    #     'jobDefinitionName': 'string',
    #     'jobDefinitionArn': 'string',
    #     'revision': 123
    # }
    jd = client.register_job_definition(**new_def)

    logger.info("job definition %s created (Arn=%s)", name, jd['jobDefinitionArn'])
    return jd


def submit_job(name, queue, jobdef, command=None, vcpus=None, memory=None, environment=None, attempts=1, timeout=1000):
    """
    args:
      name: name of the job
      queue: queue name or arn
      jobdef: the name of the queue (name:revision) or arn
      command: [str, str, str, ...] to override the job definition command
      vcpus: int  (overrides job def)
      memory: int  (MiB. overrides job def)
      attempts: number of times to move the job into runnable state (1 <= n <= 10) (overrides job def)
      environment: key-value pairs. adds or redefines environment variables from job definition. keys must not start with AWS_BATCH.
    """
    logger.info("submitting job %(name)s/%(jobdef)s to queue=%(queue)s vcpus=%(vcpus)s mem=%(memory)sMiB cmd=%(command)s",
                {"name": name,
                 "queue": queue,
                 "jobdef": jobdef,
                 "vcpus": vcpus,
                 "memory": memory,
                 "command": command
                })
    client = boto3.client('batch')

    cont_overrides = {}
    if command is not None:
        cont_overrides['command'] = command
    if vcpus is not None:
        cont_overrides['vcpus'] = int(vcpus)
    if memory is not None:
        cont_overrides['memory'] = int(memory)
    if environment is not None:
        cont_overrides['environment'] = [{'name': k, 'value': v} for k,v in environment.items()]

    job_settings = {
        'jobName': name,
        'jobQueue': queue,
        'dependsOn': [],
        'jobDefinition': jobdef,
        'parameters': {},
        'containerOverrides': cont_overrides
    }
    if attempts is not None:
        job_settings['retryStrategy'] = {'attempts': int(attempts)}
    if timeout is not None:
        job_settings['timeout'] = {'attemptDurationSeconds': int(timeout)}

    submission = client.submit_job(**job_settings)
    logger.debug("job submitted %s", submission)
    return submission


def describe_jobs(jobs):
    # max 100 at a time
    all_jobs = []

    client = boto3.client("batch")

    for c in range(0, (len(jobs) + 99) // 100):
        jobids = jobs[100*c:100*c+100]
        res = client.describe_jobs(jobs=jobids)
        all_jobs += res['jobs']

    return all_jobs


def wait_for_completion(jobs, interval=2*60, num_shown=5):
    """wait for the given jobs to either be SUCCEEDED, or FAILED.
       this calls describe_jobs repeatedly.
    """
    while True:
        desc = describe_jobs(jobs)
        status_map = {}
        incomplete = 0
        for job in desc:
            status_map.setdefault(job['status'], []).append(job['jobId'])
            if job['status'] not in ('SUCCEEDED', 'FAILED'):
                incomplete += 1

            for status in sorted(status_map.keys()):
                logger.info("job summary:")
                logger.info("    %-10s (%-3d): %s ...", status, len(status_map[status]), status_map[status][0:num_shown])
        if incomplete > 0:
            logger.info("waiting for %d job(s) to complete (check interval=%ss)...", incomplete, interval)
            time.sleep(interval)
            continue
        # all jobs done (success/failure)
        break
    return status_map


def _cmd_test_jobs(**kwargs):
    role = config['job_role_arn']
    image = "879518704116.dkr.ecr.us-west-2.amazonaws.com/rieseberglab/analytics:5-2.3.2-bunnies"

    from bunnies import ComputeEnv
    ce = ComputeEnv("testfsx3")
    ce.create()

    volumes = [{'name': disk['name'], 'host_src': disk['instance_mountpoint'], 'dst': "/scratch"}
               for disk in ce.disks.values()]

    jobdef = make_jobdef("bunnies-test-jobdef", role, image, mounts=volumes, reuse=True)

    ce.wait_ready()

    result = ce.submit_job("simple-sleeper", jobdef['jobDefinitionArn'],
                           ['simple-test-job.sh', '600'], 1, 128)
    print(result)


def _cmd_show_job_logs(jobid, **kwargs):
    import sys

    job = AWSBatchSimpleJob.from_job_id(jobid)
    if not job:
        sys.stderr.write("no logs found for job id %s\n" % (jobid,))
        return 1

    def _get_time(ms):
        return datetime.fromtimestamp(ms/1000.0).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    for event in job.log_stream(startFromHead=True):
        print(_get_time(event['timestamp']), event['message'])

    if True:
        status = job.get_status()
        secs = (status.get('stoppedAt', 0) - status.get('startedAt', 0)) / 1000.0
        from_submit = (status.get('stoppedAt', 0) - status.get('createdAt', 0)) / 1000.0
        run_t = timedelta(seconds=secs)
        submit_t = timedelta(seconds=from_submit)
        print("exited with code: %d  reason: %s" % (status['container']['exitCode'], status['statusReason']))
        print("runtime: %6.3fs (%s)" % (secs, str(run_t)))
        print("total: %6.3fs (%s)" % (from_submit, str(submit_t)))


def configure_parser(main_subparsers):
    parser = main_subparsers.add_parser("jobs", help="commands concerning launched jobs")

    subparsers = parser.add_subparsers(help="specify an operation on jobs", dest="jobs_command")

    subp = subparsers.add_parser("test", help="setup entities needed for launching jobs")
    subp.set_defaults(func=_cmd_test_jobs)

    subp = subparsers.add_parser("logs", help="inspect job logs")
    subp.set_defaults(func=_cmd_show_job_logs)
    subp.add_argument("jobid", metavar="JOBID", type=str, help="the id of the job")
    subp.add_argument("--reverse", action="store_true", default=False, help="show the logs in reverse order")
