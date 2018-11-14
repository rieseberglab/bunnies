
# Creating lambdas

Info: https://aws.amazon.com/blogs/compute/better-together-amazon-ecs-and-aws-lambda/

You will need to setup a few things before the platform-created lambdas have sufficient
permissions to orchestrate compute tasks.

The lambdas created by the platform are assigned a common role.

## Initial setup

Make sure your AWS_PROFILE is set appropriately, and run
`<repodir>/scripts/setup-lambda.sh` to create the roles and trust
permissions for lambdas to use.

## Deployment

_Note: For the moment, the only lambdas created are platform lambdas for orchestration_

You may place lambdas you have as tasks in directories, with a special metadata file named `.metadata.json` at the root.
This is a deployment "recipe" for the lambda directory. It contains an array of entry points. One lambda will be created per
entry point.

    [
        {
            "FunctionName": "start-align-task",
            "Runtime": "python3.6",
            "Handler": "main.lambda_handler",
            "Description": "starts an alignment container in ecs",
            "Timeout": 300,
            "Role": "reprod-lambda-role",
            "Environment": {
                "Variables": {}
            },
            "MemorySize": 128
        }
    ]
