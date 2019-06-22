#!/usr/bin/env bash

cename="$1"
fsxdns="$2"

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

# {
#     "LaunchTemplate": {
#         "LaunchTemplateId": "LaunchTemplateId",
#         "LaunchTemplateName": "LaunchTemplateName",
#         "CreateTime": "1970-01-01T00:00:00",
#         "CreatedBy": "CreatedBy",
#         "DefaultVersionNumber": 0,
#         "LatestVersionNumber": 0,
#         "Tags": [
#             {
#                 "Key": "Key",
#                 "Value": "Value"
#             }
#         ]
#     }
# }
cat "${template_json}"
launchtemplateid=$("$SCRIPTSDIR/read_json_key" LaunchTemplate.LaunchTemplateId < "${template_json}")

# grab subnet id (i.e. subnet-0f...)
subnetid=$(python -c "from bunnies.config import config as C; print(C['subnet_id'])")
# grab security group id(s)
sgid=$(python -c "from bunnies.config import config as C; print(C['security_group_id'])")

cetype="spot" # EC2 | SPOT

# FIXME create instance role
cerole="arn:aws:iam::879518704116:instance-profile/ecsInstanceRole"

# FIXME create EC2 spot fleet role
aws iam create-role --role-name AmazonEC2SpotFleetRole \
    --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Sid":"","Effect":"Allow","Principal":{"Service":"spotfleet.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
aws iam attach-role-policy \
    --policy-arn arn:aws:iam::aws:policy/service-role/AmazonEC2SpotFleetTaggingRole \
    --role-name AmazonEC2SpotFleetRole
spotrole=""

# "imageId": "",
# computeResources."placementGroup": "",
cedef='
{
    "computeEnvironmentName": "'$cename'",
    "type": "MANAGED",
    "state": "ENABLED",
    "computeResources": {
        "type": "EC2",
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
        "instanceRole": "'$cerole'",
        "tags": {
            "platform": "bunnies"
        },
        "bidPercentage": 100,
        "spotIamFleetRole": "'$spotrole'",
        "launchTemplate": {
            "launchTemplateId": "'$launchtemplateid'",
        }
    },
    "serviceRole": ""
}
'

aws batch create-compute-environment \
    --compute-environment-name "$cename" \
    --type "managed" \
    --state "ENABLED" \
    --compute-resources type=$cetype,minvCpus=0,maxvCpus=256,desiredvCpus=0,instanceTypes=optimal
