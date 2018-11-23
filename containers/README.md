
FARGATE intro
==============

AWS fargate is a new way of launching containers into Amazon ECS.
ECS is the service which schedules docker-based tasks into your EC2 VMs
that are configured with the amazon ecs agent (think amazon dockerd).
Fargate is a "Launch Type" for ECS.

Rather than having to provision a an ECS cluster of EC2 VMs (each
running the aws ecs agent (i.e. pre-configured AMI or custom image +
ECS agent tools), you simply deploy your containers into a per-zone pool of resources.
This saves you from having to scale up and down your VM resources as tasks pile in.

The terminology (nouns):

  - Cluster:  an ECS term. a named group of VM resources. A cluster can span
          both EC2 resources, and FARGATE resources. Whenever a task is run,
	  with ECS RunTask, you must specify a cluster name, and a launch type
	  (EC2 or FARGATE).

  - Task: ECS term. defines a collection of containers meant to run as a single unit.
          the task is scheduled on one particular cluster, but the
	  task definition itself can be deployed on any cluster (i.e. it's not a sub-concept
	  of the cluster).
	  The task has configured aggregate resource caps chosen by
	  the user defining it (max cpus, max memory). The Task is just a specification.
	  You get running containers only when you call RunTask, and you can run many instances
	  of the same task (but each task instance will have an independent lifecycle).

  - Container: ECS term. Maps directly to the docker container concept.
          Each task has one or more container definitions. container definitions
	  define an image, an entry point (think init process),
	  a start command, an environment (key values), storage (docker volumes),
	  and things like log drivers (i.e. send the logs to cloudwatch). You can also
	  define lifecycle settings, such as whether or not a container should restart
	  if it terminates, or even how many instances of a container should be run
	  within a task.

To work with FARGATE, your task definition has some restrictions:

  - it needs to be defined in zones: US (Northern Virginia), US
    (Ohio), US (Oregon), and EU (Ireland) AWS regions (as of April
    2016). Not available in Canada (checked in Nov 2018).

  - it needs to have `requiresCompatibilities: ["FARGATE"]` in its definition.

  - it needs to fall within one of the following vCPU/memory requirement
    brackes. Those values are maximum _total_ values for all the conainers
    in your task:

      - 256 (.25 vCPU)	512 (0.5GB), 1024 (1GB), 2048 (2GB)
      - 512 (.5 vCPU)	1024 (1GB), 2048 (2GB), 3072 (3GB), 4096 (4GB)
      - 1024 (1 vCPU)	2048 (2GB), 3072 (3GB), 4096 (4GB), 5120 (5GB), 6144 (6GB), 7168 (7GB), 8192 (8GB)
      - 2048 (2 vCPU)	Between 4096 (4GB) and 16384 (16GB) in increments of 1024 (1GB)
      - 4096 (4 vCPU)	Between 8192 (8GB) and 30720 (30GB) in increments of 1024 (1GB)

  - Your task, when provisioned, will have access to only 10GB of Docker layer storage (writes to "UnionFS" top layer).

  - Task storage is ephemeral. After a Fargate task stops, the storage is deleted. (That can be a good thing).

  - You task can access only an additional 4 GB for volume mounts. This can
    be mounted and shared among containers using the volumes,
    mountPoints and volumesFrom parameters in the task definition.

  - If more storage is needed, of course there is S3, but there is
    also EFS (ElasticFileSystem), which will need to be mounted in the
    container (and will need the container to configure an NFS
    client). See:
    https://forums.aws.amazon.com/thread.jspa?threadID=268391

Launching in FARGATE
====================

To launch an image into FARGATE, you would run the ECS RunTask method,
with the FARGATE launch type. (Note: there is also a StartTask method,
which lets you micromanage which VM will receive your task. RunTask
figures out placement for you).

Example bunnies call (boto3):

    ecs.run_task(**{
        'taskDefinition': "align-task",  # name of task definition
        'cluster': "reprod",             # name of cluster
        'launchType': "FARGATE",         # how it should be launched. "EC2" or "FARGATE"
        'overrides': {
            "containerOverrides": [
                {
                    'name': 'aligner',                 # Name of container config to override (the name as listed in task definition).
		    #'command': ["my-cmd", "my-arg"],  # You can also override the command specified in the container.
                    'environment': [
                        {
                            'name': "JOBSPEC",              # An S3 Blob containing the parameters of the JOB.
                            'value': "s3://reprod-bucket/job-123412341234/jobspec.json"
                        }
                    ],
                }
            ]
        },
        'networkConfiguration': {
            'awsvpcConfiguration': {
                'subnets': [bunnies.config['subnet_id']],
                'assignPublicIp': 'ENABLED' # FIXME Even if your VPC has a working internet gateway,
		                            # You need ENABLED here for the ECS agent to pull the docker image.
            }
        }
    })

The command (assuming the task launch succeeds) will give you a unique
handle to the task (`taskArn`), which can then be polled for state
changes, or can be stopped. From that point on, there is essentially
no difference between an ECS task and a FARGATE task at the API
level. There might be a runtime difference however, because rules for
placement have different availability guarantees (obviously if all
your EC2 VMs are shut down, tasks will stay pending).


Commands vs EntryPoints
=========================

In a bunnies pipeline, the user provides the image name in which the step will run.

    e.g. rieseberglab/analytics:3

This image contains all the binaries that the user requires to perform the computation,
and is configured ahead of time.

The user writing the bunnies pipeline provides a script that will invoke the binaries,

    e.g. /usr/local/bin/align {input} {output.bam}

This script is the COMMAND to the container. For practical reasons,
the user shouldn't need to update the docker image every time the
command parameters change. So the command is the perfect place to
pass this script.

Bunnies also needs a shim layer to provide the user's container
with a binary to run platform functions. In other words, there
is a binary interface between the user's script, and bunnies.
This can be achieved with a Docker layer and a new "shimmed"
container image. e.g. :

   # start from the user's container
   FROM rieseberglab/analytics:3

   # install platform tools
   COPY ./platform  /bunnies/
   ENTRYPOINT ["/bunnies/bin/bunny-init"]


At build time, bunnies recomputes the shimmed docker image,
and updates the ECS task definitions to point to the shimmed
container:

   # give the shimmed image a new name, derived from the user's
   docker build -t bunnies_rieseberglab_3:latest  .

The COMMAND _can_ be changed when the container is run,
but the ENTRYPOINT _cannot_. Running the container with command
`"ls -l"`, would, for instance be equivalent to:

   /bunnies/bin/bunny-init "ls -l"

This allows the platform to take care of bootstrapping the task to the
point where the script defined in the pipeline can be run.
(e.g. downloading the job specification, creating /input and /output
folders, automatically copying the output files out to s3,
communicating script exit codes to the orchestrator, recording/timing the user's
code, setting up uniform logging for the task, restarting the task, etc).

**_Note on binary dependencies_**: This technique introduces platform
dependencies into the user's container.  For instance, the current
platform tools require python3.6. In the future, this can be
alleviated with a statically-compiled Go binary (compiled for Linux 64
bit) could be simply copied into the container.

**_Overwriting the user's entrypoint_** There can also only be one
entrypoint defined in a container, so this would override any
entrypoint defined in the user's container. There are multiple ways of
solving this, the first (easiest) is to make the pipeline script start
by invoking the initial entry point.  The second, arguably better, is
to inspect the user's container at build time, pull out the original
entry point string in the metadata, and insert it at the end of the
bunny-init.


Developer Notes
===============

# Instructions for pushing a new image to AWS ECR

1. install docker

2. create an ECS container registry (or use the default one associated
    with the account). boto doesn't support creating a repository -- do
    it from the console.
  
    https://ca-central-1.console.aws.amazon.com/ecs/home?region=ca-central-1#/repositories/create/new

3. Login to the registry

    Your docker client will need to login to the registry:

    # run the command that docker will need to use
    $(aws ecr get-login --no-include-email --region ca-central-1)

    # note that the aws binary shipped with ubuntu is old and doesn't support `get-login`
    # use the one installed by pip. (see requirements). The first time, you'll have
    # to run `aws configure`


4. inside the registry, create one ECS container repository per pipeline rule.

   - Use docker CLI to push to the ECR repository of choice. (docker build, docker push, etc.)

        docker build -t test .

        # make the new local image an alias for a remote name
        docker tag test:latest 458136422280.dkr.ecr.ca-central-1.amazonaws.com/test:latest

        # push the local image to its repository
        docker push 458136422280.dkr.ecr.ca-central-1.amazonaws.com/test:latest


   - You can also create empty repos from the console:

        aws ecr create-repository --repository-name <reponame> | tee repo-ubuntu.json

        # there will be a key called: repositoryArn which can be used to associate local images with it.
        docker tag localimage:localtag <repositoryArn>:remotetag

# Creating tasks
================

Before you can invoke your containers using ECS, you need to define tasks. See `scripts/setup-tasks.sh`.

Other commands: 
===============

1. List current ECR repositories:

       aws ecr describe-repositories

    Full instructions are [here](https://docs.aws.amazon.com/AmazonECR/latest/userguide/ECR_AWSCLI.html).

3. Delete a repo and all its images

       aws ecr delete-repository --repository-name <reponame> --force
