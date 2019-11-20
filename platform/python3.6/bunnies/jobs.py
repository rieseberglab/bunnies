#!/usr/bin/env python3

import boto3
from .constants import PLATFORM, JOB_LOGS_PREFIX, JOB_USAGE_FILE
from .utils import data_files, read_log_stream, get_blob_meta, hash_data
from .containers import wrap_user_image
from .config import config
from .exc import BunniesException, NoSuchFile
from .transfers import s3_streaming_put, s3_streaming_put_simple

import os
import json
import logging
import os.path
import botocore.waiter
import time
import io

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
        self.job_id = None

    def _get_desc(self, client=None):
        if client is None:
            client = boto3.client('batch')

        jobs = client.describe_jobs(
            jobs=[self.job_id]
        )
        if not jobs or not jobs['jobs']:
            return None
        return jobs['jobs'][0]

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

    def terminate(self, reason=None, client=None):
        """terminate a STARTING/RUNNING job. if the job hasn't reached the STARTING stage, it is cancelled"""
        if not self.job_id:
            return False

        if not client:
            client = boto3.client('batch')
        client.cancel_job(jobId=self.job_id, reason=reason if reason is not None else "cancelled by user")
        return True

    def cancel(self, reason=None, terminate=True, client=None):
        """ cancel a submitted job"""
        if not self.job_id:
            return False

        if not client:
            client = boto3.client('batch')
        resp = client.cancel_job(jobId=self.job_id, reason=reason if reason is not None else "cancelled by user")
        return True

    def get_status(self, attempt=-1):
        """
        returns the job status for the matching attempt.

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
        job_desc = self._get_desc()
        if not job_desc:
            return None, None

        if not job_desc['attempts']:
            # logger.debug("no attempts: %s", job_desc)
            return None, job_desc

        attempt = job_desc['attempts'][attempt]
        return attempt, job_desc

    def save_usage(self, dest_url=None):
        """extracts usage information and saves it in the folder designateg by dest_url (s3 folder)
           if the destination url is omitted, it is extracted from the bunnies output directory for
           the job.

           Returns:
                usage_url, usage_info, is_written

           is_written is True if the usage information was written to usage_url (if the usage information already
           exists, we avoid overwriting if the usage information retrieved is incomplete)

        """
        job_desc = self._get_desc()
        if not job_desc:
            raise BunniesException("cannot retrieve job information %s" % (self.job_id,))

        if dest_url is None:
            job_env = job_desc['container']['environment']
            result_var = [var for var in job_env if var['name'] == "BUNNIES_RESULT"]
            if not result_var:
                raise BunniesException("no location to save logs could be determined from the environment")
            dest_url = os.path.split(result_var[0]['value'])[0]

        def _attempt_has_instance_info(attempt):
            if not attempt or not attempt['instance']:
                return False
            for instance_info in attempt['instance']:
                if not instance_info or not instance_info.get('instanceType', None):
                    return False
            return True

        usage = self.get_usage()
        no_instance_info = [attempt for attempt in usage['attempts']
                            if not _attempt_has_instance_info(attempt)]

        # avoid race
        if no_instance_info:
            overwrite = False
        else:
            overwrite = True

        logdest = os.path.join(dest_url, JOB_USAGE_FILE)

        if not overwrite:
            # FIXME there's a race condition here. we want put with O_CREAT | O_EXCL.
            try:
                _ = get_blob_meta(logdest)
                logger.info("usage information already present. leaving as-is.")
                return logdest, usage, False
            except NoSuchFile:
                pass

        with io.BytesIO() as usage_fp:
            data = json.dumps(usage, indent=4).encode('utf-8')
            cmd5 = hash_data(data, encoding="hex", algo="md5")
            write_len = usage_fp.write(data)
            usage_fp.seek(0)
            s3_streaming_put_simple(usage_fp, logdest, content_md5=cmd5,
                                    content_type="application/json", content_length=write_len,
                                    logprefix=os.path.basename(logdest))
        return logdest, usage, True

    def save_logs(self, dest_url=None):
        """saves all job logs for all attempts in the folder designated by dest_url prefix (s3 folder).
           if the destination url is omitted, it is extracted from the bunnies output directory for the
           job
        """
        job_desc = self._get_desc()
        if not job_desc:
            raise BunniesException("cannot retrieve job information %s" % (self.job_id,))

        if dest_url is None:
            job_env = job_desc['container']['environment']
            result_var = [var for var in job_env if var['name'] == "BUNNIES_RESULT"]
            if not result_var:
                raise BunniesException("no location to save logs could be determined from the environment")
            dest_url = os.path.split(result_var[0]['value'])[0]

        def _container_name(logName):
            return logName.split("/")[1]

        def _get_containers(attempt):
            return [{
                'name': _container_name(container['logStreamName']),
                'logstream': container['logStreamName'],
                'code': container['exitCode'],
            } for container in [attempt['container']]]

        def _get_time(ms):
            return datetime.fromtimestamp(ms/1000.0).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        all_dests = []

        for attempti, attempt in enumerate(job_desc['attempts']):
            containers = _get_containers(attempt)
            for details in containers:
                if attempti == len(job_desc['attempts']) - 1 and job_desc['status'] == "SUCCEEDED":
                    basename = "%s.success.log" % (details['name'],)
                else:
                    basename = "%s.attempt-%d.log" % (details['name'], attempti)
                logdest = os.path.join(dest_url, JOB_LOGS_PREFIX + basename)

                all_dests.append(logdest)
                # write log events to binary file
                with io.BytesIO() as log_fp:
                    for event in read_log_stream("/aws/batch/job", details['logstream'], startFromHead=True):
                        logmsg = _get_time(event['timestamp']) + " " + event['message'] + "\n"
                        log_fp.write(logmsg.encode('utf-8'))

                    secs = (attempt["stoppedAt"] - attempt["startedAt"]) / 1000.0
                    from_submit = (attempt['stoppedAt'] - job_desc['createdAt']) / 1000.0
                    run_t = timedelta(seconds=secs)
                    submit_t = timedelta(seconds=from_submit)
                    tailmsgs = ["exit code: %d\n" % (details['code']),
                                "run time: %6.3fs (%s)\n" % (secs, str(run_t)),
                                "from submission: %6.3f (%s)\n" % (from_submit, str(submit_t))]
                    for tailmsg in tailmsgs:
                        log_fp.write(tailmsg.encode("utf-8"))
                    file_len = log_fp.tell()
                    log_fp.seek(0)
                    s3_streaming_put(log_fp, logdest, content_type="text/plain", content_length=file_len,
                                     logprefix=os.path.basename(logdest))
        return all_dests

    def get_usage(self):
        """obtain a dictionary of information about this job.

           Call this while the job is running, between attempts, or shortly after the job is complete.
           This call will access information about the ec2 instance, which is only available for a short
           amount of time after the instance/ecs agent is terminated.

          Permissions needed:
           batch:DescribeJobs
           batch:DescribeJobQueues
           batch:DescribeComputeEnvironments
           ecs:DescribeContainerInstances
           ec2:DescribeInstances
        """
        batch = boto3.client('batch')
        job_desc = self._get_desc(client=batch)
        if not job_desc:
            raise BunniesException("cannot retrieve job information %s" % (self.job_id,))

        def _container_name(logName):
            return logName.split("/")[1]

        def _attempt_info(attempt, job_desc):
            resources = [{"vcpus": job_desc['container']['vcpus'], "memory": job_desc['container']['memory']}]
            return {
                "containerInstanceArn": [attempt['container']['containerInstanceArn']],
                "resources": resources,
                "startedAt": attempt['startedAt'],
                "stoppedAt": attempt['stoppedAt'],
                "logs": [attempt['container']['logStreamName']]
            }

        attempt_info = [_attempt_info(attempt, job_desc) for attempt in job_desc['attempts']]

        job_queue_arn = job_desc['jobQueue']

        job_queue = get_jobqueue(job_queue_arn)
        compute_env_arns = [item['computeEnvironment'] for item in job_queue['computeEnvironmentOrder']]

        compute_envs = [batch.describe_compute_environments(computeEnvironments=[name])['computeEnvironments'][0]
                        for name in compute_env_arns]
        cluster_arns = [compute_env['ecsClusterArn'] for compute_env in compute_envs]

        ecs = boto3.client('ecs')
        ec2 = boto3.client('ec2')

        def _container_instance_info(ecs_instance_arn, cluster_arns):
            if ecs_instance_arn not in _container_instance_info.cache:
                for cluster_arn in cluster_arns:
                    instances = ecs.describe_container_instances(
                        containerInstances=[ecs_instance_arn],
                        cluster=cluster_arn
                    )['containerInstances']
                    if not instances:
                        continue
                    _container_instance_info.cache[ecs_instance_arn] = instances[0]
                    return instances[0]
                _container_instance_info.cache[ecs_instance_arn] = None
                return None
            return _container_instance_info.cache[ecs_instance_arn]

        _container_instance_info.cache = {}

        def _ec2_instance_info(ec2_instance_id):
            if ec2_instance_id not in _ec2_instance_info.cache:
                instances = ec2.describe_instances(
                    InstanceIds=[ec2_instance_id])
                if not instances or not instances['Reservations']:
                    logger.info("cannot obtain details on instance %s", ec2_instance_id)
                    _ec2_instance_info.cache[ec2_instance_id] = None
                    return None
                res = instances['Reservations'][0]
                if not res['Instances']:
                    logger.info("cannot obtain details on instance %s", ec2_instance_id)
                    _ec2_instance_info.cache[ec2_instance_id] = None
                    return None
                _ec2_instance_info.cache[ec2_instance_id] = res['Instances'][0]
            return _ec2_instance_info.cache[ec2_instance_id]

        _ec2_instance_info.cache = {}

        container_instance_info = {}

        for attempt in attempt_info:
            attempt['instance'] = []
            for container_instance_arn in attempt['containerInstanceArn']:
                container_instance_info = _container_instance_info(container_instance_arn, cluster_arns)
                if not container_instance_info:
                    logger.info("could not obtain container instance info for arn %s",
                                container_instance_arn)
                    attempt['instance'].append(None)
                    continue

                instance_id = container_instance_info['ec2InstanceId']
                instance_info = {'instanceId': instance_id,
                                 'instanceType': None,
                                 'coreCount': None,
                                 'threadsPerCore': None}
                attempt['instance'].append(instance_info)

                ec2_info = _ec2_instance_info(instance_id)
                if ec2_info:
                    instance_info['instanceType'] = ec2_info['InstanceType']
                    instance_info['startedAt'] = int(ec2_info['LaunchTime'].timestamp() * 1000)
                    instance_info['coreCount'] = ec2_info['CpuOptions']['CoreCount']
                    instance_info['threadsPerCore'] = ec2_info['CpuOptions']['ThreadsPerCore']

        def _update_totals(info):
            vcpu_s = 0
            memory_s = 0
            compute_time = 0
            min_time = job_desc['createdAt']
            min_start_time = float("+inf")
            max_time = 0

            for attempt in info['attempts']:
                if attempt['startedAt'] < min_time:
                    min_time = attempt['startedAt']
                if attempt['startedAt'] < min_start_time:
                    min_start_time = attempt['startedAt']
                if attempt['stoppedAt'] > max_time:
                    max_time = attempt['stoppedAt']
                attempt_duration = (attempt['stoppedAt'] - attempt['startedAt'])/1000.0
                attempt_vcpus = sum([cont['vcpus'] for cont in attempt['resources']])
                attempt_mem = sum([cont['memory'] for cont in attempt['resources']])
                compute_time += attempt_duration
                vcpu_s += attempt_vcpus * attempt_duration
                memory_s += attempt_mem * attempt_duration

            total = info['total']
            if min_time == float("+inf"):
                min_time = 0
                max_time = 0
            total.update(vcpu_secs=vcpu_s,
                         memory_secs=memory_s,
                         compute_time_s=compute_time,
                         total_time_s=(max_time - min_time)/1000.0)
            info.update(finishedAt=max_time,
                        startedAt=min_start_time)

        usage_obj = {
            'total': {
                'vcpu_secs': 0,      # cpu*seconds
                'memory_secs': 0,    # memory(mb)*seconds
                'compute_time_s': 0, # time spent computing (not counting scheduling time and time between attempts)
                'total_time_s': 0,   # overall time spent (between submission and end of last attempt)
                'network': {},       # data transferred over net
                'credits': 0         # estimation of $ costs
            },
            'createdAt': job_desc['createdAt'],
            'startedAt': 0,
            'finishedAt': 0,
            'attempts': attempt_info
        }
        _update_totals(usage_obj)
        return usage_obj

    def log_stream(self, attempt=-1, startTime=None, endTime=None, startFromHead=False):
        """yields each log event of the job,

           the logstream is only available when the job reaches RUNNING state.

           startTime and endTime are in ms.
        """
        job_desc = self._get_desc()
        if not job_desc:
            return None

        if not job_desc['attempts']:
            if attempt != -1:
                return
            container_logs = job_desc['container'].get('logStreamName', "")
            if not container_logs:
                return
            logger.debug("attempt is in progress...: %s", container_logs)
        else:
            attempt = job_desc['attempts'][attempt]
            container_logs = attempt['container']['logStreamName']
            logger.debug("showing logs for completed attempt...: %s", container_logs)

        for event in read_log_stream("/aws/batch/job", container_logs,
                                     startTime=startTime, endTime=endTime, startFromHead=startFromHead):
            yield event


def _custom_waiters():
    if not _custom_waiters.model:
        waiters = {
            "JobQueueDisabled": {
                "delay": 15,
                "operation": "DescribeJobQueues",
                "maxAttempts": 40,
                "acceptors": [
                    {
                        "expected": "ENABLED",
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
                    },
                    {
                        "matcher": "path",
                        "expected": True,
                        "argument": "length(jobQueues[]) == `0`",
                        "state": "success"
                    }
                ]
            },
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
    """role assumed by batch (ecs) jobs"""

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
            },
            {
                "Sid": "",
                "Effect": "Allow",
                "Principal": {
                    "Service": "ecs.amazonaws.com"
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


def get_jobqueues():
    """gets all job queues"""
    client = boto3.client('batch')
    paginator = client.get_paginator('describe_job_queues')
    def_iterator = paginator.paginate()
    found = []
    for page in def_iterator:
        page_defs = page['jobQueues']
        if len(page_defs) == 0:
            break

        found += page_defs
    return found

def get_jobqueue(name):
    client = boto3.client('batch')
    jq = client.describe_job_queues(
        jobQueues=[name]
    )
    return jq['jobQueues'][0]


def wait_queue_disabled(queueNames):
    """wait for all job queues to be in the DISABLED+VALID state"""
    if not queueNames:
        return

    logger.info("waiting for queue(s) %s to be disabled...", queueNames)
    client = boto3.client('batch')
    waiter = botocore.waiter.create_waiter_with_client("JobQueueDisabled", _custom_waiters(), client)
    waiter.wait(jobQueues=queueNames)
    logger.info("queue(s) %s disabled", queueNames)

def wait_queue_ready(queueNames):
    """wait for a job queue to be in the ENABLED+VALID state"""
    if not queueNames:
        return

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

            #
            # skip those that are INACTIVE
            page_defs = [pdef for pdef in page_defs if pdef['status'] == "ACTIVE"]
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


def wait_for_jobs(jobs, interval=2*60, condition=None):
    """wait for the job ids in `jobs` to satisfy some condition

       jobs: [ jobid0, jobid1, ... ]

       The returned value is a mapping from job state to the job object val. e.g.:
       (possible states are SUBMITTED | PENDING | RUNNABLE | STARTING | RUNNING | SUCCEEDED | FAILED)
            {
                'SUCCESS': [ (jobid0, reason), (jobid1, reason) ],
                'FAILED': [(jobid2, reason)],
                ...
            }
    """
    if not condition:
        def condition(x): return True

    job_ids = [jobid for jobid in jobs]
    while True:
        desc = describe_jobs(job_ids)
        status_map = {}
        for job in desc:
            if not job['status'] in status_map:
                status_map[job['status']] = []
            status_map[job['status']].append((job['jobId'], job.get('statusReason', '')))

        if condition(status_map):
            break

        time.sleep(interval)
        continue

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


def _cmd_save_job_usage(jobid, **kwargs):
    import sys

    job = AWSBatchSimpleJob.from_job_id(jobid)
    if not job:
        sys.stderr.write("job not found %s\n" % (jobid,))
        return 1

    dest, _, _ = job.save_usage()
    print("usage in:")
    print(" ", dest)


def _cmd_save_job_logs(jobid, **kwargs):
    import sys

    job = AWSBatchSimpleJob.from_job_id(jobid)
    if not job:
        sys.stderr.write("job not found %s\n" % (jobid,))
        return 1

    destinations = job.save_logs()
    print("logs saved:")
    for dest in destinations:
        print(" ", dest)


def _cmd_cancel_job(jobid, reason=None, terminate=False, **kwargs):
    import sys
    job = AWSBatchSimpleJob.from_job_id(jobid)
    if not job:
        sys.stderr.write("job not found %s\n" % (jobid,))
        return 1

    if terminate:
        return 0 if job.terminate(reason=reason) else 1
    return 0 if job.cancel(reason=reason) else 1


def _cmd_show_job_logs(jobid, ts=False, **kwargs):
    import sys

    job = AWSBatchSimpleJob.from_job_id(jobid)
    if not job:
        sys.stderr.write("job not found %s\n" % (jobid,))
        return 1

    def _get_time(ms):
        return datetime.fromtimestamp(ms/1000.0).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    lines_printed = 0
    for event in job.log_stream(startFromHead=True):
        lines_printed += 1
        if ts:
            print(_get_time(event['timestamp']), event['message'])
        else:
            print(event['message'])

    status, job_desc = job.get_status()

    if not status:
        sys.stderr.write("status information not yet available\n")
        if not job_desc.get('startedAt', 0):
            return 1
        started_at = job_desc.get('startedAt', 0)
        stopped_at = job_desc.get('stoppedAt', 0)
    else:
        started_at = status.get('startedAt', 0)
        stopped_at = status.get('stoppedAt', 0)

    if stopped_at == 0:
        endtime = time.time() * 1000
    else:
        endtime = stopped_at

    if 'timeout' in job_desc:
        timeout_ms = job_desc.get('timeout').get("attemptDurationSeconds") * 1000
    else:
        timeout_ms = -1

    secs = (endtime - started_at) / 1000.0
    from_submit = (endtime - job_desc.get('createdAt', 0)) / 1000.0
    run_t = timedelta(seconds=secs)
    submit_t = timedelta(seconds=from_submit)

    if status:
        print("exited code: %d  reason: %s" % (status['container']['exitCode'], status['statusReason']))

    print("run time: %6.3fs (%s)" % (secs, str(run_t)))
    print("from submission: %6.3fs (%s)" % (from_submit, str(submit_t)))
    if timeout_ms > 0 and endtime < (started_at + timeout_ms):
        timeout_secs = (started_at + timeout_ms - endtime) / 1000.0
        timeout_t = timedelta(seconds=timeout_secs)
        print("time left: %6.3fs (%s)" % (timeout_secs, str(timeout_t)))


def _cmd_show_job_usage(jobid, **kwargs):
    import sys

    job = AWSBatchSimpleJob.from_job_id(jobid)
    if not job:
        sys.stderr.write("job not found %s\n" % (jobid,))
        return 1
    usage = job.get_usage(jobid)
    if not usage:
        return 1
    print(json.dumps(usage, indent=4))


def configure_parser(main_subparsers):
    parser = main_subparsers.add_parser("jobs", help="commands concerning launched jobs")

    subparsers = parser.add_subparsers(help="specify an operation on jobs", dest="jobs_command")

    subp = subparsers.add_parser("test", help="setup entities needed for launching jobs")
    subp.set_defaults(func=_cmd_test_jobs)

    subp = subparsers.add_parser("logs", help="inspect job logs")
    subp.set_defaults(func=_cmd_show_job_logs)
    subp.add_argument("jobid", metavar="JOBID", type=str, help="the aws batch id of the job")
    subp.add_argument("--reverse", action="store_true", default=False, help="show the logs in reverse order")
    subp.add_argument("--ts", action="store_true", default=False)

    subp = subparsers.add_parser("cancel", help="cancel job")
    subp.add_argument("jobid", metavar="JOBID", type=str, help="the aws batch id of the job")
    subp.set_defaults(func=_cmd_cancel_job)
    subp.add_argument("--reason", default=None, help="the reason given -- this will be recorded in the job status")
    subp.add_argument("--terminate", default=False, action="store_true", dest="terminate",
                      help="transition to FAILED if the job is RUNNING/STARTING, cancel it if it hasn't transitioned to STARTING yet.")

    subp = subparsers.add_parser("usage", help="show job usage")
    subp.set_defaults(func=_cmd_show_job_usage)
    subp.add_argument("jobid", metavar="JOBID", type=str, help="the aws batch id of the job")

    subp = subparsers.add_parser("saveusage", help="save job usage")
    subp.set_defaults(func=_cmd_save_job_usage)
    subp.add_argument("jobid", metavar="JOBID", type=str, help="the aws batch id of the job")

    subp = subparsers.add_parser("savelogs", help="save the logs of a job to its output S3 folder")
    subp.set_defaults(func=_cmd_save_job_logs)
    subp.add_argument("jobid", metavar="JOBID", type=str, help="the aws batch id of the job")
