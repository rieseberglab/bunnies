#!/bin/bash

set -ex

#
# Creates a VPC called reprod-vpc
#

VPCNAME=reprod-vpc


#
# more steps here (esp for internet routing)
# https://medium.com/@brad.simonin/create-an-aws-vpc-and-subnet-using-the-aws-cli-and-bash-a92af4d2e54b
#

HERE=$(readlink -f "$(dirname "$0")")

if ! aws ec2 describe-vpcs --filters Name=tag:Name,Values="$VPCNAME" | grep -q "CidrBlock"; then
    # doesn't exist
    resp=$(aws ec2 create-vpc --cli-input-json file://"${HERE}/../tasks/reprod-vpc.json")

    vpc_id=$(echo "$resp" | grep 'VpcId":' | egrep -o 'vpc-[^\"]+')
    echo VPC created: "${vpc_id}"

    # name it
    aws ec2 create-tags --resources "$vpc_id" --tags Key=Name,Value="$VPCNAME"
else
    echo "vpc already exists" >&2
fi

