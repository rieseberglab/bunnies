#!/bin/bash

HERE="$(readlink -f $(dirname "$0"))"

set -ex

OUTFILE="${OUTFILE:-cluster-settings.json}"

if [[ -e "$OUTFILE" ]]; then
    echo "file $OUTFILE already exists. aborting." >&2
    exit 1
fi

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
# this can be run several times with the same name. oddly enough.
#
create_cluster_response=$(aws ecs create-cluster --cluster-name 'reprod')

cluster_name=$("$HERE"/read_json_key cluster.clusterName <<<"$create_cluster_response")
cluster_arn=$("$HERE"/read_json_key cluster.clusterArn <<<"$create_cluster_response")

echo \
"{
\"cluster_name\": \"${cluster_name}\",
\"cluster_arn\": \"${cluster_arn}\"
}" | tee "${OUTFILE}"

# create test task definition for align container

function placeholders () {
    local subst
    local region=$(aws configure get region)
    osed "s@{REGION}@$(aws configure get region)@g"
}
# FIXME -- the execution role for dockerd is the same as the one for the container.
#          they could, and should be different.
for definition in "$HERE"/../tasks/*-task-definition.json; do
    tmpdef=$(mktemp --tmpdir setup-tasks-XXXXXXXX.json)
    placeholders < "$definition" > "$tmpdef"

    aws ecs register-task-definition \
	--cli-input-json file://$(readlink -f "$tmpdef") \
	--execution-role-arn reprod-ecs-role \
	--task-role-arn reprod-ecs-role
    rm -- "$tmpdef"
done
