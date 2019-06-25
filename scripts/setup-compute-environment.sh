#!/usr/bin/env bash

cename="$1"
fsxdns="$2"

cerolename="bunnies-ecs-instance-role"
cespotrolename="bunnies-ec2-spot-fleet-role"
cebatchservicerolename="bunnies-batch-service-role"
cetype="EC2" # EC2 | SPOT
ceinstanceprofilename="bunnies-batch-instance-profile"

# grab subnet id (i.e. subnet-0f...)
subnetid=$(python -c "from bunnies.config import config as C; print(C['subnet_id'])")
# grab security group id(s)
sgid=$(python -c "from bunnies.config import config as C; print(C['security_group_id'])")


SCRIPTSDIR=$(dirname "$(readlink -f "$0")")
REPODIR=$(readlink -f "$SCRIPTSDIR/..")

set -exo pipefail

usage () {
    echo "$(basename "$0") CENAME LUSTREDNS

    CENAME name of the environment to create
    LUSTREDNS dns endpoint of the lustre fsx to mount
"
}

key_pair_name () {
    python -c "import bunnies.environment as E; print(E.get_key_name())"
}

tmpdir=$(mktemp -d tmp-ce-setup-XXXXX)
cleanup () {
    if [[ -d "$tmpdir" ]]; then rm -rf --one-file-system -- "$tmpdir" || :; fi
}
trap cleanup EXIT


[[ -n "$cename" ]] || {
    echo "missing compute environment name" >&2
    usage >&2;
    exit 1
}

keypair=$(key_pair_name)
[[ -n "$keypair" ]] || {
    echo "missing key pair name" >&2
    usage >&2;
    exit 1
}

[[ -n "$fsxdns" ]] || {
    echo "missing lustre dns name">&2
    usage >&2;
    exit 1
}

# create launch template
launchuserdata='MIME-Version: 1.0
Content-Type: multipart/mixed; boundary="==MYBOUNDARY=="

--==MYBOUNDARY==
Content-Type: text/cloud-config; charset="us-ascii"

runcmd:
- amazon-linux-extras install -y lustre2.10
- fsxdir=/mnt/fsx1
- fsxdns='${fsxdns}'
- mkdir -p ${efs_directory}
- echo ${fsxdns}@tcp:/fsx ${fsxdir} lustre defaults,_netdev 0 0 >> /etc/fstab
- mount -a -t lustre defaults

--==MYBOUNDARY==
'
# -w 0 disables line wrapping in output -- we want single line
launchb64=$(echo "$launchuserdata" | base64 -w 0)
launchtemplatename="bunnies-launch-template-$cename"
launchtemplatedata='{
    "LaunchTemplateName": "'$launchtemplatename'",
    "VersionDescription": "adds lustre filesystems to default environment",
    "LaunchTemplateData": {
        "UserData": "'$launchb64'",
        "TagSpecifications": [
            {
                "ResourceType": "launch-template",
                "Tags": [
                    {
                        "Key": "platform",
                        "Value": "bunnies"
                    }
                ]
            }
        ]
    }
}'

tee <<<"$launchtemplatedata" "$tmpdir/launch-template-data.json"

template_json="$REPODIR/bunnies-launch-template-$cename.json"
if [[ -f "${template_json}" ]]; then
    echo "template ${template_json} already exists..."
else
    echo "creating launch template..."
    aws ec2 create-launch-template --cli-input-json "file://$tmpdir/launch-template-data.json" > "$template_json".tmp
    mv "$template_json"{.tmp,}
fi

launchtemplateid=$("$SCRIPTSDIR/read_json_key" LaunchTemplate.LaunchTemplateId < "${template_json}")

# create ecs instance role
if ! aws iam list-roles | egrep -q "\b${cerolename}\b"; then
    aws iam create-role \
	--role-name "${cerolename}" \
	--path / \
	--description "Role to assign ECS instances spawned by bunnies platform" \
	--assume-role-policy-document file://"$SCRIPTSDIR"/../roles/bunnies-ecs-instance-trust-relationship.json \
	--tags Key=Platform,Value=bunnies
fi

instancerolearn=$(aws iam get-role --role-name "${cerolename}" --query Role.Arn --output text)

aws iam attach-role-policy \
    --role-name "${cerolename}" \
    --policy-arn "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"

# create EC2 spot fleet role
if ! aws iam list-roles | egrep -q "\b${cespotrolename}\b"; then
    aws iam create-role \
	--role-name "${cespotrolename}" \
	--path / \
	--description "allow bunnies ec2 instances to join spot fleets" \
	--assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Sid":"","Effect":"Allow","Principal":{"Service":"spotfleet.amazonaws.com"},"Action":"sts:AssumeRole"}]}' \
	--tags Key=Platform,Value=bunnies
fi
spotrolearn=$(aws iam get-role --role-name "${cespotrolename}" --query Role.Arn --output text)

aws iam attach-role-policy \
    --policy-arn arn:aws:iam::aws:policy/service-role/AmazonEC2SpotFleetTaggingRole \
    --role-name "${cespotrolename}"

# create service role to allow aws to make Batch calls on your behalf.
if ! aws iam list-roles | egrep -q "\b${cebatchservicerolename}\b"; then
    aws iam create-role \
	--role-name "${cebatchservicerolename}" \
	--path "/service-role/" \
	--description "allow aws to issue batch calls on behalf of user" \
	--assume-role-policy-document file://"${SCRIPTSDIR}"/../roles/bunnies-batch-service-role-trust-relationship.json
fi

aws iam attach-role-policy \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole \
    --role-name "${cebatchservicerolename}"

servicerolearn=$(aws iam get-role --role-name "${cebatchservicerolename}" --query Role.Arn --output text)

if ! aws iam get-instance-profile --instance-profile-name "${ceinstanceprofilename}"; then
    aws iam create-instance-profile --instance-profile-name "${ceinstanceprofilename}" --path="/"
fi

profilearn=$(aws iam get-instance-profile --instance-profile-name "${ceinstanceprofilename}" --query InstanceProfile.Arn --output text)

add_role_resp=$(aws iam add-role-to-instance-profile --instance-profile-name "${ceinstanceprofilename}" \
		    --role-name "${cerolename}" 2>&1 || echo "failed.")
if grep -q "failed." <<<"${add_role_resp}"; then
    if ! grep -q "Cannot exceed quota" <<<"${add_role_resp}"; then
	echo "${add_role_resp}" >&2
	exit 1
    fi
    # idempotent already there -- pass
fi

# "imageId": "",
# computeResources."placementGroup": "",
cedef='{
    "computeEnvironmentName": "'$cename'",
    "type": "MANAGED",
    "state": "ENABLED",
    "computeResources": {
        "type": "'$cetype'",
        "minvCpus": 0,
        "maxvCpus": 256,
        "desiredvCpus": 0,
        "instanceTypes": [
            "optimal"
        ],
        "subnets": [
            "'$subnetid'"
        ],
        "securityGroupIds": [
            "'$sgid'"
        ],
        "ec2KeyPair": "'$keypair'",
        "instanceRole": "'$profilearn'",
        "tags": {
            "platform": "bunnies"
        },
        "bidPercentage": 100,
        "spotIamFleetRole": "'$spotrolearn'",
        "launchTemplate": {
            "launchTemplateId": "'$launchtemplateid'"
        }
    },
    "serviceRole": "'${servicerolearn}'"
}'

tmpjson=$(mktemp -p "$tmpdir" "ce-input-XXXX.json")
echo "$cedef" > "$tmpjson"
aws batch create-compute-environment \
    --compute-environment-name "$cename" \
    --cli-input-json file://"$tmpjson"

