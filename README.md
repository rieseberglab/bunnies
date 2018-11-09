# reprod

Framework for orchestrating reproducible data analysis in the cloud.


Installation
============

1. create virtualenv:

       virtualenv -p python3 --prompt="(reprod) " .venv

   _Note: if you don't have virtualenv, you can install it first with
        `pip install virtualenv`, or use the builtin module in python3,
	i.e. `python3 -m venv .reprod`_

1. activate env

       source .venv/bin/activate

1. install python dependencies (includes awscli tools)

       # optional, but recommended before you install deps:
       pip install --upgrade pip

       # platform dependencies
       pip install -r requirements.txt


1. Configure your AWS credentials. This is detailed [elsewhere](https://docs.aws.amazon.com/cli/latest/userguide/cli-config-files.html), but here's one way:

       mkdir -p ~/.aws

   Add a section in `~/.aws/config`:
   
       [profile reprod]
       region=ca-central-1
       output=json

   Update your credentials file `~/.aws/credentials` (section header syntax differs from config file):

       [reprod]
       aws_access_key_id=AKIAIOSFODNN7EXAMPLE
       aws_secret_access_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

   It's a good idea to `chmod go-rw ~/.aws/credentials` too.

Setup
========

While you're working on `reprod`, you may then wish to export the
AWS_PROFILE environment variable to pick the desired account. If this
is not custmized, the aws cli tools will use the default account.

       export AWS_PROFILE=reprod

Resources
----------

To get started, a few platform resources need to be created and configured in your AWS account.

   - IAM roles and permissions for S3, EC2, ECS.
   - S3 buckets
   - EC2 VPCs
   - API Gateway pipeline management endpoints.

These resources are created using the scripts provided in
`./scripts/`. FIXME provide more detailed description.

More resources will be generated when the pipeline definitions are converted into AWS concepts:

   - Lambdas
   - ECS Tasks
   - S3 Buckets to store temporary data
