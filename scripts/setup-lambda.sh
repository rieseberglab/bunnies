#!/bin/bash

set -ex

#
# Creates Roles and permissions for the lambda
#

OUTFILE="${OUTFILE:-lambda-settings.json}"
PFX=reprod
ROLENAME="$PFX-lambda-role"

HERE=$(readlink -f "$(dirname "$0")")

function json_key ()
{
    "$HERE"/read_json_key "$1"
}

function undo () {
    local policynames
    local name

    policynames=$(aws iam list-role-policies --role-name "$ROLENAME" | json_key PolicyNames)
    for name in "${policynames[@]}"; do
	aws iam delete-role-policy --role-name "$ROLENAME" --policy-name "$name"
    done

    aws iam delete-role --role-name "$ROLENAME"
}

case "$UNDO" in
    y|Y|1)
	undo
	exit $?
	;;
    *)
	:
	;;
esac


:
: set up IAM role for "$PFX" lambdas
:
aws iam create-role \
    --role-name "$ROLENAME" \
    --path / \
    --description "role for $PFX lambdas" \
    --assume-role-policy-document file://"${HERE}/../lambdas/$PFX-lambda-trust-relationship.json" || {
    err=$?
    set +x
    echo "The role may exist already. If this is the case, you may delete it with:

    UNDO=1 $0
" >&2
    exit $err
}

:
: set up Lambda permissions for "$PFX" lambdas
:
aws iam put-role-policy \
    --role-name "$ROLENAME" \
    --policy-name "$PFX"_lambda_permissions \
    --policy-document file://"${HERE}/../lambdas/$PFX-lambda-policy.json"

# 1. set up S3 bucket:
#   aws s3 mb s3://my-bucket
