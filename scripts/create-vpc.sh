#!/bin/bash

set -ex

#
# Creates a VPC, with a subnet, and a security group
# outputs the new object ids to file network-settings.json
#
OUTFILE="${OUTFILE:-network-settings.json}"
PFX=reprod
VPCNAME=$PFX-vpc
GATEWAYNAME=$PFX-gateway
SUBNETNAME=$PFX-subnet
SECGROUPNAME=$PFX-security-group
ROUTETABLENAME=$PFX-route-table

INGRESS_PORTS=(
    "tcp 22 0.0.0.0/0"
    "tcp 8000 0.0.0.0/0"
)

#
# more steps here (esp for internet routing)
# https://medium.com/@brad.simonin/create-an-aws-vpc-and-subnet-using-the-aws-cli-and-bash-a92af4d2e54b
#

HERE=$(readlink -f "$(dirname "$0")")

function json_key ()
{
    "$HERE"/read_json_key "$1"
}

if [[ -f "$OUTFILE" ]]; then
    echo  "output file $OUTFILE already exists" >&2
    exit 1
fi

vpcs=$(aws ec2 describe-vpcs --filters Name=tag:Name,Values="$VPCNAME")
vpc_id=$(echo "$vpcs" | json_key Vpcs.0.VpcId || :)

if [[ -z "$vpc_id" ]]; then
    :
    : create vpc
    :
    resp=$(aws ec2 create-vpc --cli-input-json file://"${HERE}/../tasks/$PFX-vpc.json")

    vpc_id=$(echo "$resp" | grep 'VpcId":' | egrep -o 'vpc-[^\"]+')
    echo VPC created: "${vpc_id}"

    # name it
    aws ec2 create-tags --resources "$vpc_id" --tags Key=Name,Value="$VPCNAME"

    :
    : let aws translate our hostnames to ips
    :
    modify_response=$(aws ec2 modify-vpc-attribute --vpc-id "$vpc_id" \
			  --enable-dns-support "{\"Value\":true}")

    :
    : enable dns hostnames
    :
    # instances launched in the vpc get hostnames
    modify_response=$(aws ec2 modify-vpc-attribute --vpc-id "$vpc_id" \
			  --enable-dns-hostnames "{\"Value\":true}")

    :
    : create gateway
    :
    gateway_response=$(aws ec2 create-internet-gateway --output json)
    gateway_id=$(echo "$gateway_response" | json_key 'InternetGateway.InternetGatewayId')

    # name gateway
    aws ec2 create-tags --resources "$gateway_id" \
	--tags Key=Name,Value="$GATEWAYNAME"

    # attach gateway to vpc
    aws ec2 attach-internet-gateway --internet-gateway-id "$gateway_id" --vpc-id "$vpc_id"

    :
    : create subnet in vpc
    :
    subnet_response=$(aws ec2 create-subnet --cli-input-json file://"${HERE}/../tasks/$PFX-vpc-subnet.json" \
			  --vpc-id "$vpc_id" --output json)
    subnet_id=$(echo "$subnet_response" | json_key Subnet.SubnetId)

    # name subnet
    aws ec2 create-tags --resources "$subnet_id" \
	--tags Key=Name,Value="$SUBNETNAME"

    :
    : no public ip assigned by default in subnet
    :
    # instances started on this subnet do not receive a public ip by default
    modify_response=$(aws ec2 modify-subnet-attribute --subnet-id "$subnet_id" \
			  --no-map-public-ip-on-launch)

    :
    : create security group
    :
    security_response=$(aws ec2 create-security-group \
			    --group-name "$SECGROUPNAME" \
			    --description "Group for $PFX tasks" \
			    --vpc-id "$vpc_id" --output json)
    secgroup_id=$(echo "$security_response" | json_key GroupId)

    aws ec2 create-tags --resources "$secgroup_id" \
	--tags Key=Name,Value="$SECGROUPNAME"

    :
    : allow ingress traffic
    :
    for ingress in "${INGRESS_PORTS[@]}"; do
	read proto port_in cidr_in rest <<< "$ingress"
	security_response2=$(aws ec2 authorize-security-group-ingress \
			     --group-id "$secgroup_id" \
			     --protocol "$proto" --port "$port_in" --cidr "$cidr_in")
    done

    :
    : create routing table for vpc
    :
    route_table_response=$(aws ec2 create-route-table --vpc-id "$vpc_id" --output json)
    route_table_id=$(echo "$route_table_response" | json_key RouteTable.RouteTableId)

    aws ec2 create-tags --resources "$route_table_id" \
	--tags Key=Name,Value="$ROUTETABLENAME"

    :
    : add default route to internet gateway
    :
    route_response=$(aws ec2 create-route --route-table-id "$route_table_id" \
			 --destination-cidr-block 0.0.0.0/0 \
			 --gateway-id "$gateway_id")

    :
    : associate subnet to route table
    :
    associate_response=$(aws ec2 associate-route-table \
			     --subnet-id "$subnet_id" \
			     --route-table-id "$route_table_id"
		      )
    echo \
"{
\"vpc_id\": \"${vpc_id}\",
\"subnet_id\": \"${subnet_id}\",
\"security_group_id\": \"${secgroup_id}\"
}" | tee "${OUTFILE}"

    echo "See file network-settings.json to see ids of new network." >&2
else
    echo "vpc already exists. use aws ec2 delete-vpc --vpc-id $vpc_id to remove." >&2
fi

