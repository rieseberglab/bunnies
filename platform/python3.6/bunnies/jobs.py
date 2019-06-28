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


def _make_jobdef(name, jobroleArn, image, vcpu, memory):
    client = boto3.client('batch')
    jd = client.register_job_definition(
        jobDefinition=name,
        type='container',
        containerProperties={
            'image': image,
            'vcpu': 1,
            'memory': 128,
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
                { 'name': "CORE",
                  'hardlimit': 0,
                  'softlimit': 0
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


def _test_submit(name, queue, jobdef, command):
    client = boto3.client('batch')
    submission = client.submit_job(
        jobName = "test",
        jobQueue = queue,
        dependsOn = [],
        jobDefinition=jobdef,
        parameters={},
        containerOverrides={
            'vcpus': 1,
            'memory': 256,
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


def _setup_jobs(**kwargs):
    setup_ecs_role()

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
