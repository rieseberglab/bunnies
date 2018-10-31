
import json
import os
import boto3
import botocore

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    print("value1 = " + event['key1'])
    print("value2 = " + event['key2'])
    print("value3 = " + event['key3'])
    print("os.environ", os.environ)
    
    ecs = boto3.client('ecs')
    resp = ecs.run_task(**{
        'taskDefinition': "align-task",
        'cluster': "reprod",
        'launchType': "FARGATE",
        'networkConfiguration': {
            'awsvpcConfiguration': {
                'subnets': ['my-subnet'],
                'assignPublicIp': 'DISABLED'
            }
        }
    })
    print(resp)
    console.log('ECS has been triggered.')
    return event['key1']  # Echo back the first key value
    #raise Exception('Something went wrong')
