#!/usr/bin/env python3


"""
  Tools for managing a bunnies compute environment
"""
from uuid import uuid4
import logging
import os.path

import base64
import boto3
import botocore
import botocore.waiter
from botocore.exceptions import ClientError

from .config import config
from . import constants
from .utils import data_files

logger = logging.getLogger(__package__)


def get_key_name():
    return config["KeyName"]


def get_subnet_id():
    return config["subnet_id"]


def get_security_group_id():
    return config["security_group_id"]


def _custom_waiters():
    if not _custom_waiters.model:
        waiters = {
            "FileSystemDeleted": {
                "delay": 15,
                "operation": "DescribeFileSystems",
                "maxAttempts": 40,
                "acceptors": [
                    {
                        "expected": "AVAILABLE",
                        "matcher": "pathAny",
                        "state": "failure",
                        "argument": "FileSystems[].Lifecycle"
                    },
                    {
                        "expected": "DELETING",
                        "matcher": "pathAny",
                        "state": "retry",
                        "argument": "FileSystems[].Lifecycle"
                    },
                    {
                        "expected": "CREATING",
                        "matcher": "pathAny",
                        "state": "failure",
                        "argument": "FileSystems[].Lifecycle"
                    },
                    {
                        "expected": "UPDATING",
                        "matcher": "pathAny",
                        "state": "failure",
                        "argument": "FileSystems[].Lifecycle"
                    },
                    {
                        "matcher": "error",
                        "expected": "FileSystemNotFound",
                        "state": "success"
                    }
                ]
            },

            "FileSystemReady": {
                "delay": 15,
                "operation": "DescribeFileSystems",
                "maxAttempts": 40,
                "acceptors": [
                    {
                        "expected": "AVAILABLE",
                        "matcher": "pathAll",
                        "state": "success",
                        "argument": "FileSystems[].Lifecycle"
                    },
                    {
                        "expected": "FAILED",
                        "matcher": "pathAny",
                        "state": "failure",
                        "argument": "FileSystems[].Lifecycle"
                    },
                    {
                        "expected": "DELETING",
                        "matcher": "pathAny",
                        "state": "failure",
                        "argument": "FileSystems[].Lifecycle"
                    },
                    {
                        "matcher": "error",
                        "expected": "FileSystemNotFound",
                        "state": "failure"
                    }
                ]
            }
        }
        model = botocore.waiter.WaiterModel({
            "version": 2,
            "waiters": waiters
        })
        _custom_waiters.model = model
    return _custom_waiters.model


_custom_waiters.model = None


class FSxDisk(object):
    def __init__(self, name, size_gb):
        self.name = name
        self.size_gb = ((size_gb + 3599) // 3600) * 3600
        if self.size_gb < 3600:
            raise ValueError("disk size should be larger than 0")
        self.__token = str(uuid4())
        self.__fs = None

    @property
    def capacity(self):
        return self.size_gb

    def retrieve_existing(self):
        # FIXME -- the caller should inspect the state of the returned filesystem
        client = boto3.client('fsx')
        resp = {'NextToken': ""}

        def _tags_match(name, tags):
            name_tags = [tag for tag in tags if
                         tag['Key'] == 'Name' and tag['Value'] == name]
            if not name_tags:
                return False

            platform_tags = [tag for tag in tags if
                             tag['Key'] == "Platform" and
                             tag['Value'] == constants.PLATFORM]
            return (len(platform_tags) > 0)

        while 'NextToken' in resp:
            kwargs = {'MaxResults': 10}
            if resp['NextToken']:
                kwargs['NextToken'] = resp['NextToken']
            logger.info("listing file systems...")
            page = client.describe_file_systems(**kwargs)
            logger.info("retrieved page with %d fs(es)", len(page['FileSystems']))
            matches = [candidate for candidate in page['FileSystems']
                       if _tags_match(self.name, candidate['Tags'])]
            if matches:
                return matches[0]
            if len(page['FileSystems']) == 0:
                return None
        return None

    @property
    def dns_name(self):
        if not self.__fs:
            self.__fs = self.retrieve_existing()
        return self.__fs['DNSName']

    def fstab(self, target):
        """returns the line that should be added to fstab to mount this filesystem onto the target"""
        return "%(dns)s@tcp:/fsx %(target)s lustre defaults,_netdev 0 0" % {
            'dns': self.dns_name,
            'target': target
        }

    @property
    def fsid(self):
        if not self.__fs:
            self.__fs = self.retrieve_existing()
        return self.__fs['FileSystemId']

    def delete(self):
        """
        delete the filesystem
        """
        if self.__fs:
            fsid = self.__fs['FileSystemId']
        else:
            fs = self.retrieve_existing()
            if fs is None:
                logger.info("File system %s not found. Nothing to delete", self.name)
                return None
            fsid = fs['FileSystemId']

        client = boto3.client('fsx')
        logger.info("Deleting file system %s (id=%s)...", self.name, fsid)
        client.delete_file_system(FileSystemId=fsid, ClientRequestToken=self.__token)
        self.__fs=None


    def create(self):
        """
        creates a new empty disk
        """
        exists = self.retrieve_existing()
        if exists:
            logger.info("reusing existing filesystem: %s", exists['ResourceARN'])
            self.__fs = exists
        else:
            logger.info("creating Lustre FSx filesystem... Name=%s", self.name)
            client = boto3.client('fsx')
            resp = client.create_file_system(
                ClientRequestToken=self.__token,
                FileSystemType="LUSTRE",
                StorageCapacity=self.size_gb,
                SubnetIds=[get_subnet_id()],
                SecurityGroupIds=[get_security_group_id()],
                Tags=[{'Key': "Name", 'Value': self.name},
                      {'Key': "Platform", 'Value': constants.PLATFORM}],
                LustreConfiguration={
                    'WeeklyMaintenanceStartTime': "7:08:15"
                }
            )
            self.__fs = resp['FileSystem']
            logger.info("filesystem %s (id=%s) created", self.name, self.fsid)

    def wait_ready(self):
        """wait for the filesystem to be in the READY state"""
        logger.info("waiting for filesystem %s (id=%s) to be ready...", self.name, self.fsid)
        client = boto3.client('fsx')
        waiter = botocore.waiter.create_waiter_with_client("FileSystemReady", _custom_waiters(), client)
        waiter.wait(FileSystemIds=[self.fsid])

    def wait_deleted(self):
        """wait for the filesystem to be deleted completely"""
        client = boto3.client('fsx')
        waiter = botocore.waiter.create_waiter_with_client("FileSystemReady", _custom_waiters(), client)
        waiter.wait(FileSystemIds=[self.fsid])


def _create_ecs_instance_role():
    # create ecs instance role
    client = boto3.client("iam")
    ecs_role_name = constants.CE_ECS_INSTANCE_ROLE
    logger.info("creating IAM role %s", ecs_role_name)
    try:
        with open("../roles/bunnies-ecs-instance-trust-relationship.json", "r") as fd:
            jobs_ecs_trust = fd.read()

        client.create_role(Path='/',
                           RoleName=ecs_role_name,
                           Description="Role to assign ECS instances spawned by %s platform" % (constants.PLATFORM,),
                           AssumeRolePolicyDocument=jobs_ecs_trust,
                           Tags=[{'Key': 'platform', 'Value': constants.PLATFORM}])
        logger.info("IAM role %s created", ecs_role_name)
    except ClientError as clierr:
        if clierr.response['Error']['Code'] == 'EntityAlreadyExists':
            logger.info("using existing role %s", ecs_role_name)
            pass
        else:
            raise

    client.attach_role_policy(RoleName=ecs_role_name,
                              PolicyArn="arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role")

    return client.get_role(RoleName=ecs_role_name)


def _create_ec2_spot_fleet_role():
    # allow bunnies ec2 to join spot fleets
    client = boto3.client("iam")
    role_name = constants.CE_SPOT_ROLE
    logger.info("creating IAM role %s", role_name)
    try:
        policy_document = '{"Version":"2012-10-17","Statement":[{"Sid":"","Effect":"Allow","Principal":{"Service":"spotfleet.amazonaws.com"},"Action":"sts:AssumeRole"}]}'

        client.create_role(Path='/',
                           RoleName=role_name,
                           Description="allow %s ec2 instances to join spot fleets" % (constants.PLATFORM,),
                           AssumeRolePolicyDocument=policy_document,
                           Tags=[{'Key': 'platform', 'Value': constants.PLATFORM}])
        logger.info("IAM role %s created", role_name)
    except ClientError as clierr:
        if clierr.response['Error']['Code'] == 'EntityAlreadyExists':
            logger.info("using existing role %s", role_name)
            pass
        else:
            raise

    # you can attach the same role multiple times without effect
    client.attach_role_policy(RoleName=role_name,
                              PolicyArn="arn:aws:iam::aws:policy/service-role/AmazonEC2SpotFleetTaggingRole")
    return client.get_role(RoleName=role_name)


def _create_batch_service_role():
    # allow aws to issue batch calls for bunnies
    client = boto3.client("iam")
    role_name = constants.CE_BATCH_SERVICE_ROLE

    logger.info("creating IAM role %s", role_name)
    try:
        trustfile = data_files(os.path.join("permissions", role_name + "-trust-relationship.json"))[0]
        with open(trustfile, "r") as fd:
            policy_document = fd.read()

        client.create_role(Path='/service-role/',
                           RoleName=role_name,
                           Description="allow aws to issue batch calls on behalf of %s user" % (constants.PLATFORM,),
                           AssumeRolePolicyDocument=policy_document,
                           Tags=[{'Key': 'platform', 'Value': constants.PLATFORM}])
        logger.info("IAM role %s created", role_name)
    except ClientError as clierr:
        if clierr.response['Error']['Code'] == 'EntityAlreadyExists':
            logger.info("using existing role %s", role_name)
            pass
        else:
            raise

    # you can attach the same role multiple times without effect
    client.attach_role_policy(RoleName=role_name,
                              PolicyArn="arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole")

    return client.get_role(RoleName=role_name)


def _create_batch_instance_profile(instance_role_name):
    profile_name = constants.CE_INSTANCE_PROFILE
    client = boto3.client("iam")
    try:
        logger.info("creating instance profile %s", profile_name)
        client.create_instance_profile(InstanceProfileName=profile_name, Path="/")
    except ClientError as clierr:
        if clierr.response['Error']['Code'] == 'EntityAlreadyExists':
            logger.info("using existing instance profile %s", profile_name)
            pass
        else:
            raise

    try:
        logger.info("adding role %s to instance profile %s", instance_role_name, profile_name)
        client.add_role_to_instance_profile(InstanceProfileName=profile_name,
                                            RoleName=instance_role_name)
        logger.info("role added")
    except ClientError as clierr:
        if clierr.response['Error']['Code'] == "LimitExceeded":
            logger.info("skipped. instance profile already has a role attached")
        else:
            raise

    return client.get_instance_profile(InstanceProfileName=profile_name)


class ComputeEnv(object):
    def __init__(self, name, scratch_size_gb=3600):
        self.name = name
        self.disks = {}
        self.instance_role = None
        self.spot_role = None
        self.batch_service_role = None
        self.instance_profile = None
        self.launch_template = None
        self.batch_ce = None

        if scratch_size_gb > 0:
            self.disks['scratch'] = {
                'name': "scratch",
                'obj': FSxDisk(name + "-scratch", scratch_size_gb),
                'instance_mountpoint': "/mnt/" + name + "-scratch"
            }

    def _generate_instance_boot_script(self):
        """generates a cloud config script which mounts configured filesystems"""

        def _cmdsplit(lis):
            return "\n".join(["- %s" % (x,) for x in lis])

        mount_targets = []
        fstab_lines = []
        for diskname, disk in self.disks.items():
            mount_targets.append(disk['instance_mountpoint'])
            fstab_lines.append(disk['obj'].fstab(disk['instance_mountpoint']))

        script = """MIME-Version: 1.0
Content-Type: multipart/mixed; boundary="==MYBOUNDARY=="

--==MYBOUNDARY==
Content-Type: text/cloud-config; charset="us-ascii"

runcmd:
- amazon-linux-extras install -y lustre2.10
%(mkdirs)s
%(fstabs)s
%(mount)s

--==MYBOUNDARY==
""" % {
        "mkdirs": _cmdsplit(["mkdir -p %s" % (mtpoint,) for mtpoint in mount_targets]),
        "fstabs": _cmdsplit(["echo %s >> /etc/fstab" % (fstab,) for fstab in fstab_lines]),
        "mount": "- :" if len(self.disks) == 0 else "- mount -a -t lustre defaults"
      }

        logger.debug("using the following instance launch script: %s", script)
        return script

    def _create_launch_template(self):
        """returns launch template id and version to use"""

        def _matching_template(client, name, tags, userdata_b64):
            """iterate through template versions with that name, and find the first one where
               tags match exactly and the userdata is the same.
            """
            paginator = client.get_paginator("describe_launch_template_versions")
            template_iterator = paginator.paginate(LaunchTemplateName=name)
            found = None

            tag_dict = {x[0]: x[1] for x in tags}
            for page in template_iterator:
                versions = page['LaunchTemplateVersions']
                if len(versions) == 0:
                    break
                for version in versions:
                    check_data = version['LaunchTemplateData']['UserData']
                    if userdata_b64 is not None and check_data != userdata_b64:
                        continue

                    instance_tags = [specs['Tags'] for specs in version['LaunchTemplateData']['TagSpecifications']
                                     if specs['ResourceType'] == "instance"][0]

                    check_dict = {x['Key']: x['Value'] for x in instance_tags}
                    if check_dict == tag_dict:
                        found = version
                        break
                    else:
                        logger.debug("launch template mismatch. skipping. found %s, but query is %s",
                                     check_dict, tag_dict)
                return found

        lt_name = "%s-ce-launch-template-%s" % (constants.PLATFORM, self.name)

        instance_tags = [("platform", constants.PLATFORM),
                         ("compute_environment", lt_name)]

        for diskname, disk in self.disks.items():
            dnstag = ("disk-dns-%s" % (diskname,), disk['obj'].dns_name)
            dirtag = ("disk-dir-%s" % (diskname,), disk['instance_mountpoint'])
            instance_tags += [dnstag, dirtag]

        client = boto3.client("ec2")
        lt_userdata = self._generate_instance_boot_script()

        logger.info("creating ec2 launch template %s for environment %s", lt_name, self.name)

        b64data = base64.b64encode(lt_userdata.encode("ascii")).decode('ascii')
        call_params = {
            "LaunchTemplateName": lt_name,
            "VersionDescription": "adds lustre filesystems to default environment",
            "LaunchTemplateData": {
                "UserData": b64data,
                "TagSpecifications": [
                    {
                        "ResourceType": "instance",
                        "Tags": [{'Key': x[0], 'Value': x[1]} for x in instance_tags]
                    }
                ]
            }
        }

        version_number = 0

        try:
            template = client.create_launch_template(**call_params)
            info = template['LaunchTemplate']
            logger.debug("created new template: %s", info)
            version_number = 1
        except ClientError as clierr:
            if clierr.response['Error']['Code'] == "InvalidLaunchTemplateName.AlreadyExistsException":
                logger.info("a template already exists with name %s", lt_name)
                template = None
            else:
                logger.error("can't create template %s", lt_name, exc_info=clierr)
                raise

        if template is None:
            # see if a compatible template exists with the same name
            template = _matching_template(client, lt_name, instance_tags, b64data)
            if template is not None:
                info = template
                version_number = info['VersionNumber']
                logger.info("reusing existing compatible template %s (id=%s version=%s)",
                            lt_name, template['LaunchTemplateId'], version_number)

        if template is None:
            # make a new version of the same template
            logger.info("none of the existing name=%s templates are compatible. creating new version", lt_name)
            template = client.create_launch_template_version(**call_params)
            info = template['LaunchTemplateVersion']
            logger.debug("created new template version: %s", template)
            version_number = info['VersionNumber']

        logger.info("using launch template %s (id=%s version=%s)",
                    lt_name, info['LaunchTemplateId'], version_number)
        return info['LaunchTemplateId'], version_number

    def _create_batch_ce(self):
        client = boto3.client("batch")

        def _find_matching(name, top_level_match, comp_res_match):
            # find a compute environment which matches the given settings
            paginator = client.get_paginator("describe_compute_environments")
            iterator = paginator.paginate()
            logger.info("listing compute environments matching %s settings", name)
            found = None
            for page in iterator:
                cenvs = page['computeEnvironments']
                if len(cenvs) == 0:
                    break
                for cenv in cenvs:
                    # match
                    pass
            return found

        ce_type = "EC2" # "SPOT"
        instance_profile_arn = self.instance_profile['InstanceProfile']['Arn']
        spot_role_arn = self.spot_role['Role']['Arn']
        service_role_arn = self.batch_service_role['Role']['Arn']

        lt_id, lt_version = self._create_launch_template()

        comp_resources = {
                "type": ce_type,
                "minvCpus": 0,
                "maxvCpus": 256,
                "desiredvCpus": 0,
                "instanceTypes": [
                    "optimal"
                ],
                "subnets": [
                    get_subnet_id()
                ],
                "securityGroupIds": [
                    get_security_group_id()
                ],
                "ec2KeyPair": get_key_name(),
                "instanceRole": instance_profile_arn,
                "tags": {
                    "platform": constants.PLATFORM,
                    "ce_name": self.name
                },
                "bidPercentage": 100,
                "spotIamFleetRole": spot_role_arn,
                "launchTemplate": {
                    "launchTemplateId": lt_id,
                    "version": str(lt_version)
                }
        }

        existing = _find_matching(self.name, {"serviceRole": service_role_arn, "type": "MANAGED"}, comp_resources)
        if existing:
            return existing

        random_name = self.name + "-" + str(uuid4())[0:8]
        client.create_compute_environment(**{
            "computeEnvironmentName": random_name,
            "type": "MANAGED",
            "state": "ENABLED",
            "computeResources": comp_resources,
            "serviceRole": service_role_arn
        })
        ces = client.describe_compute_environments(
            computeEnvironments=[random_name]
        )
        return ces['computeEnvironments'][0]


    def create(self):
        """ensure all the entities are created"""
        logger.info("creating compute environment %s", self.name)

        # start block -- following is common to all envs
        self.instance_role = _create_ecs_instance_role()
        self.spot_role = _create_ec2_spot_fleet_role()
        self.batch_service_role = _create_batch_service_role()
        self.instance_profile = _create_batch_instance_profile(self.instance_role['Role']['RoleName'])
        # end block

        # create disks
        for name, ddict in self.disks.items():
            dobj = ddict['obj']
            dobj.create()

        self.batch_ce = self._create_batch_ce()

        logger.info("compute environment %s created", self.name)

    def delete(self):
        """ delete all entities associated with this compute environment
            this includes filesystems created for this compute environment.
        """
        for name, ddict in self.disks.items():
            dobj = ddict['obj']
            dobj.delete()

    def wait_ready(self):
        """ensure all the entities are VALID and ready to execute things"""
        for name, ddict in self.disks.items():
            dobj = ddict['obj']
            dobj.wait_ready()

    def wait_deleted(self):
        """ensure all the entities are deleted completely"""
        for name, ddict in self.disks.items():
            dobj = ddict['obj']
            dobj.delete()


def _create_env(envname='', **kwargs):
    """create an environment and wait for it to be ready"""
    myenv = ComputeEnv(envname)
    myenv.create()
    myenv.wait_ready()


def _delete_env(envname='', **kwargs):
    """tear down an environment"""
    myenv = ComputeEnv(envname)
    myenv.delete()
    myenv.wait_deleted()


def main():
    import argparse
    import sys
    import bunnies

    bunnies.setup_logging()
    boto3.set_stream_logger('boto3.resources', logging.INFO)

    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(help="sub-command help", dest="command")

    subp = subparsers.add_parser("create", help="create a new environment and wait for it to be ready.",
                                 description="Create a new environment and wait for it to be ready. "
                                 "If the environment already exists, the program will wait for it to be"
                                 " ready.")
    subp.add_argument("envname", metavar="ENVNAME", type=str, help="the name of the new environment")

    subp = subparsers.add_parser("delete", help="delete/teardown a compute environment")
    subp.add_argument("envname", metavar="ENVNAME", type=str, help="the name of the environment")

    args = parser.parse_args(sys.argv[1:])

    if args.command is None:
        sys.stderr.write("No subcommand specified.\n")
        sys.stderr.write(parser.format_usage() + "\n")
        sys.exit(1)

    func = {
        'create': _create_env,
        'delete': _delete_env
    }.get(args.command)
    retcode = func(**vars(args))
    sys.exit(int(retcode) if retcode is not None else 0)

if __name__ == "__main__":
    main()
