#!/bin/bash

HERE="$(readlink -f $(dirname "$0"))"

set -ex

# create ecs role
if ! aws iam list-roles | grep -q reprod-ecs-role; then
    aws iam create-role \
	--role-name reprod-ecs-role \
	--path / \
	--description "role for ecs tasks spawned by reprod" \
	--assume-role-policy-document file://"$HERE"/../tasks/reprod-ecs-trust-relationship.json
fi

for fil in "$HERE"/../tasks/reprod-ecs-*-permissions.json; do
    aws iam put-role-policy \
	--role-name reprod-ecs-role \
	--policy-name "$(basename "$fil" -permissions.json)" \
	--policy-document file://"$fil"
done


# default cluster creation
aws ecs create-cluster --cluster-name 'reprod' > "cluster_config.json"

# create test task definition for align container

# FIXME -- the execution role for dockerd is the same as the one for the container.
#          they could, and should be different.
aws ecs register-task-definition \
    --cli-input-json file://"$HERE"/../tasks/align-task-definition.json \
    --execution-role-arn reprod-ecs-role \
    --task-role-arn reprod-ecs-role

    
    

