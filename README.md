# reprod

Framework for orchestrating reproducible data analysis in the cloud.


Installation
============

1. create virtualenv:

       virtualenv -p python3 --prompt="(reprod) " .venv

   _Note: if you don't have virtualenv, you can install it first with
        `pip install virtualenv`, or use the builtin module in python3,
	i.e. `python3 -m venv .venv`_

   _Note: If you're on an older distribution with a different default python3
        and/or you don't have root access to install packages,
        you can bootstrap its installation as a regular user with conda_

            ~/miniconda3/bin/conda env create -f environment.yml -n python36
            source ~/miniconda3/bin/activate python36

            # inside the conda environment, you have python3.6
	    (python36) $ pip install --upgrade pip
            (python36) $ pip install virtualenv

            # create a python3.6 virtual env
            (python36) $ virtualenv --prompt="(reprod) " -p python3.6 .venv

            # from this point on you no longer need the conda environment.
            # a copy of the python3.6 runtime was added to the .venv virtualenv
            # folder
            source ~/miniconda3/bin/deactivate

1. activate env

       source .venv/bin/activate

1. install python dependencies (includes awscli tools)

       # optional, but recommended before you install deps:
       pip install --upgrade pip

       # platform dependencies
       pip install -r requirements.txt


1. Configure your AWS credentials. This is detailed [elsewhere](https://docs.aws.amazon.com/cli/latest/userguide/cli-config-files.html), but here's one way:

       mkdir -p ~/.aws

   Add a section in `~/.aws/config`.:
   
       [profile reprod]
       region=us-west-2
       output=json

   Update your credentials file `~/.aws/credentials` (section header syntax differs from config file):

       [reprod]
       aws_access_key_id=AKIAIOSFODNN7EXAMPLE
       aws_secret_access_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

   It's a good idea to `chmod go-rw ~/.aws/credentials` too.

   _Note: The region you pick here should be one where FARGATE is supported._

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

More resources will be generated when the pipeline definitions are converted into AWS concepts:

   - Lambdas
   - ECS Tasks
   - S3 Buckets to store temporary data

These resources are created using the scripts provided in
`./scripts/`. FIXME provide more detailed description.

   - `./scripts/setup-lambda.sh`  creates roles with permissions for platform-created lambdas. Creates `./lambda-settings.json`.

   - `./scripts/setup-network.sh` creates network configuration usable by tasks. outputs created ids in `./network-settings.json`.

   - `./scripts/setup-tasks.sh` creates task configuration based on available tasks. Currently using mostly hardcoded values
      sufficient to drive the example. The created entities are saved in `cluster-settings.json`

   - You will need to create `./storage-settings.json` with the name of a bucket you intend to use as temporary storage. Example contents:

         {
           "tmp_bucket": "reprod-temp-bucket",
	   "build_bucket": "reprod-build-bucket"
         }

   - `./scripts/setup-key-pair.sh` creates the keypair that will be associated with the new instances. This will be the key to use
     to ssh into the created VMs or containers. Outputs `./key-pair-settings.json` and `key-pair.pem` private key.

   - `python -m bunnies.environment setup` will create amazon roles and permissions necessary for scheduling instances and submit
      jobs in the context of a compute environment.
