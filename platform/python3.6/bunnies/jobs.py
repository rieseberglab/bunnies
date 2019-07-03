#!/usr/bin/env python3
import boto3
from .constants import PLATFORM
import json
import logging
import os.path
import glob
from botocore.exceptions import ClientError

logger = logging.getLogger(__package__)


def permissions_files(rolename):
    here = os.path.dirname(__file__)
    perms_dir = os.path.join(here, "data", "permissions")
    matches = [permfile for permfile in glob.glob(os.path.join(perms_dir, "*-permissions.json"))
               if os.path.basename(permfile).startswith(rolename)]
    return matches

def setup_ecs_role():
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
            
    for pfile in permissions_files(PLATFORM + "-ecs"):
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


def make_jobdef(name, jobroleArn, image, vcpus=1, memory=128, reuse=True):
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
      memory: default amount of memory

    """
    client = boto3.client('batch')

    def jobdef_exists(client, name):
        paginator = client.get_paginator('describe_job_definitions')
        def_iterator = paginator.paginate(jobDefinitionName=name)
        found = None
        for page in def_iterator:
            page_defs = page['jobDefinitions']
            if len(page_defs) == 0:
                break
            found = [pdef for pdef in page_defs if
                     pdef['jobDefinitionName'] == name]
            if found:
                break
        if not found:
            return None
        return found[0]

    if reuse:
        existing = jobdef_exists(client, name)
        if existing:
            logger.info("reusing existing job definition %s (Arn=%s)", name, existing['jobDefinitionArn'])
            return existing

    logger.info("creating job definition %s with image %s...", name, image)
    jd = client.register_job_definition(
        jobDefinitionName=name,
        type='container',
        containerProperties={
            'image': image,
            'vcpus': vcpus,
            'memory': memory,
            'jobRoleArn': jobroleArn,
            'volumes': [
                {
                    'host': {
                        'sourcePath': "/mnt/fsx1"
                    },
                    'name': 'scratchvol'
                }
            ],
            'mountPoints': [
                {
                    'containerPath': "/data",
                    'readOnly': False,
                    'sourceVolume': 'scratchvol'
                }
            ],
            'privileged': False,
            'ulimits': [
                { 'name': "core",
                  'hardLimit': 0,
                  'softLimit': 0
                }
            ],
            'user': 'root'
        },
        retryStrategy={
            'attempts': 1
        },
        timeout={
            'attemptDurationSeconds': 1000
        }
    )
    logger.info("job definition %s created (Arn=%s)", name, jd['jobDefinitionArn'])
    return jd


def submit_job(name, queue, jobdef, command, vcpu, memory):
    """
    args:
      name: name of the job
      queue: queue name or arn
      jobdef: the name of the queue (name:revision) or arn
      command: [str, str, str, ...] to override the job definition command
      vcpu: int  (overrides job def)
      memory: int  (overrides job def)
    """
    logger.info("submitting job %(name)s/%(jobdef)s to queue %(queue)s: %(vcpu)s vcpus %(memory)sMB %(command)s",
                {"name": name,
                 "queue": queue,
                 "jobdef": jobdef,
                 "vcpu": vcpu,
                 "memory": memory,
                 "command": command
             })
    client = boto3.client('batch')
    submission = client.submit_job(
        jobName=name,
        jobQueue=queue,
        dependsOn=[],
        jobDefinition=jobdef,
        parameters={},
        containerOverrides={
            'vcpus': vcpu,
            'memory': memory,
            'command': command,
            'environment': [
                {'name': 'BATCH_FILE_TYPE', 'value': 'script'},
                {'name': 'BATCH_FILE_S3_URL', 'value': 's3://reprod-test-bucket/simple-test-job.sh'}
            ]
        },
        retryStrategy={
            'attempts': 1
        },
        timeout={
            'attemptDurationSeconds': 1000
        }
    )
    logger.info("job %s submitted", submission)
    return submission


def _setup_jobs(**kwargs):
    role = setup_ecs_role()
    image = "879518704116.dkr.ecr.us-west-2.amazonaws.com/rieseberglab/analytics:5-2.3.2-bunnies"
    jobdef = make_jobdef("bunnies-test-jobdef", role['Role']['Arn'], image, reuse=True)
    jobqueue = make_jobqueue("bunnies-test-queue", compute_envs=[
        ("testfsx3", 100)
    ])
    test_job = submit_job("simple-sleeper", jobqueue['jobQueueArn'], jobdef['jobDefinitionArn'],
                          ['simple-test-job.sh', '600'], 1, 128)
    print(test_job)


def main():
    import argparse
    import sys
    import bunnies

    bunnies.setup_logging()
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(help="sub-command help", dest="command")

    subp = subparsers.add_parser("setup", help="setup entities needed for launching jobs")

    subp = subparsers.add_parser("delete", help="delete/teardown a compute environment")
    subp.add_argument("envname", metavar="ENVNAME", type=str, help="the name of the environment")

    args = parser.parse_args(sys.argv[1:])

    if args.command is None:
        sys.stderr.write("No subcommand specified.\n")
        sys.stderr.write(parser.format_usage() + "\n")
        sys.exit(1)

    func = {
        'setup': _setup_jobs,
    }.get(args.command)
    retcode = func(**vars(args))
    sys.exit(int(retcode) if retcode is not None else 0)

if __name__ == "__main__":
    main()
