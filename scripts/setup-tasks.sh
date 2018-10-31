#!/bin/bash

# default cluster creation
aws ecs create-cluster \
    --cluster-name 'reprod' > "cluster_config.json"


response = client.register_task_definition(**{
	'family': "some_family",
	'taskRoleArn': "foo",
	'executionRoleArn': "foo",
	'networkMode': 'awsvpc', # needed for FARGATE
	'containerDefinitions': [
	    {
		'name': 'aligner',
		'image': "458136422280.dkr.ecr.ca-central-1.amazonaws.com/align",
		'cpu': 32,
		'memory': 000,
		'memoryReservation': 000,
		'links': [],
		'portMappings': [],
		'essential': True,
		'entryPoint': [],
		'command': [],
		'environment': [{'name': "fookey", 'value': "fooval"}],
		'mountPoints': [],
		'volumesFrom': [],
		'linuxParameters': {},
		'user': "root",
		'privileged': False,
	    }
	],
	'volumes': [],
	'requiresCompatibilities': "FARGATE",
	'cpu': "foo",
	'memory': "foo",
    })
