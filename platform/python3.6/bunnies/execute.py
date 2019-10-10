import logging
import boto3
from .config import config

log = logging.getLogger(__package__)


def ecs_describe_tasks(tasks):
    ecs = boto3.client('ecs')
    cluster = config['cluster_arn']
    log.info("ECS-describe-tasks %s", ",".join(tasks))
    return ecs.describe_tasks(cluster=cluster, tasks=tasks)


def ecs_wait_for_tasks(tasks):
    if not isinstance(tasks, (list, tuple)):
        tasks = [tasks]

    ecs = boto3.client('ecs')
    cluster = config['cluster_arn']
    waiter = ecs.get_waiter('tasks_stopped')
    waiter.wait(cluster=cluster, tasks=tasks, WaiterConfig={
        'Delay': 6,
        'MaxAttempts': 500})


def ecs_run_task(task_name, overrides=None):
    """
    Run an ECS task of the given name, overriding
    settings of one or more containers using `overrides`

    returns the created task

    overrides allows changing settings from the task definition. See
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.run_task

    e.g.:
    {'containerOverrides': [
      {
        'name': 'main',  # name of container config to override from definition
        'environment': [
           {
              'name': "JOBID",
              'value': "123456789"
           }
        ],
      }
    ]}
    """
    log.debug("running container task: %s", task_name)

    if overrides is None:
        overrides = []

    ecs = boto3.client('ecs')
    cluster = config['cluster_arn']

    req = {
        'taskDefinition': task_name,
        'cluster': cluster,
        'launchType': "FARGATE",
        'overrides': overrides,
        'networkConfiguration': {
            'awsvpcConfiguration': {
                'subnets': [config['subnet_id']],
                # FIXME Despite the vpc having an internet gateway,
                # ENABLED is necessary for the ECS agent to pull the docker image.
                'assignPublicIp': 'ENABLED'
            }
        }
    }
    resp = ecs.run_task(**req)
    return resp
