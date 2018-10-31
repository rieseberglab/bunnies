

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

Other commands: 
===============

1. List current ECR repositories:

    aws ecr describe-repositories

    Full instructions are [here](https://docs.aws.amazon.com/AmazonECR/latest/userguide/ECR_AWSCLI.html).

3. Delete a repo and all its images

      aws ecr delete-repository --repository-name <reponame> --force
