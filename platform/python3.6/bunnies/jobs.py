#!/usr/bin/env python3
import boto3
from .constants import PLATFORM
import json
import logging
import os.path

def permissions_files():

    here = os.path.dirname(__file__
    datadir = os.path.join

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

def create_ecs_role():
    ecs_role_name = PLATFORM + "-ecs-role"
    client = boto3.client("iam")
    found = client.get_role(RoleName=ecs_role_name)
    if not found:
        client.create_role(Path='/',
                           RoleName=ecs_role_name,
                           AssumeRolePolicyDocument=json.dumps(jobs_ecs_trust))
        client.put_role_policy(
            RoleName=ecs_role_name,
            PolicyName="")


# for fil in "$HERE"/../tasks/reprod-ecs-*-permissions.json; do
#     aws iam put-role-policy \
# 	--role-name reprod-ecs-role \
# 	--policy-name "$(basename "$fil" -permissions.json)" \
# 	--policy-document file://"$fil"
# done


def _make_jobdef(name, image, vcpu, memory):
    client = boto3.client('batch')
    jd = client.register_job_definition(
        jobDefinition=name,
        type='container',
        containerProperties={
            'image': image,
            'vcpu': 1,
            'memory': 128,
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


def main():
    import argparse
    import sys
    import bunnies

    bunnies.setup_logging()
    parser = argparse.ArgumentParser()

    import os.path
    
    open(sys.pred
    subparsers = parser.add_subparsers(help="sub-command help", dest="command")

    subp = subparsers.add_parser("create", help="create a new environment and wait for it to be ready.",
                                 description="Create a new environment and wait for it to be ready. "
                                 "If the environment already exists, the program will wait for it to be"
                                 " ready.")
    subp.add_argument("envname", metavar="ENVNAME", type=str, help="the name of the new environment")

    subp = subparsers.add_parser("delete", help="delete/teardown a compute environment")
    subp.add_argument("envname", metavar="ENVNAME", type=str, help="the name of the environment")

    args = parser.parse_args(sys.argv[1:])

    if args.command is None:
        sys.stderr.write("No subcommand specified.\n")
        sys.stderr.write(parser.format_usage() + "\n")
        sys.exit(1)

    func = {
        'create': _create_env,
        'delete': _delete_env
    }.get(args.command)
    retcode = func(**vars(args))
    sys.exit(int(retcode) if retcode is not None else 0)

if __name__ == "__main__":
    main()
