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
import time

from botocore.exceptions import ClientError

from .config import config
from . import constants
from .utils import data_files
from . import jobs

logger = logging.getLogger(__name__)


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
            },

            "ComputeEnvironmentReady": {
                "delay": 15,
                "operation": "DescribeComputeEnvironments",
                "maxAttempts": 40,
                "acceptors": [
                    {
                        "expected": "DISABLED",
                        "matcher": "pathAny",
                        "state": "failure",
                        "argument": "computeEnvironments[].state"
                    },
                    {
                        "expected": "CREATING",
                        "matcher": "pathAny",
                        "state": "retry",
                        "argument": "computeEnvironments[].status"
                    },
                    {
                        "expected": "UPDATING",
                        "matcher": "pathAny",
                        "state": "retry",
                        "argument": "computeEnvironments[].status"
                    },
                    {
                        "expected": "DELETING",
                        "matcher": "pathAny",
                        "state": "failure",
                        "argument": "computeEnvironments[].status"
                    },
                    {
                        "expected": "DELETED",
                        "matcher": "pathAny",
                        "state": "failure",
                        "argument": "computeEnvironments[].status"
                    },
                    {
                        "expected": "VALID",
                        "matcher": "pathAll",
                        "state": "success",
                        "argument": "computeEnvironments[].status"
                    },
                    {
                        "expected": "INVALID",
                        "matcher": "pathAny",
                        "state": "failure",
                        "argument": "computeEnvironments[].status"
                    },
                    {
                        "matcher": "path",
                        "expected": True,
                        "argument": "length(computeEnvironments[]) > `0`",
                        "state": "failure"
                    }
                ]
            },
        }
        model = botocore.waiter.WaiterModel({
            "version": 2,
            "waiters": waiters
        })
        _custom_waiters.model = model
    return _custom_waiters.model


_custom_waiters.model = None


def wait_batch_ce_ready(ce_names):
    """wait for the given batch environments to be valid and ready ready"""
    logger.info("waiting for batch environments %s to be ready...", ce_names)
    client = boto3.client('batch')
    waiter = botocore.waiter.create_waiter_with_client("ComputeEnvironmentReady", _custom_waiters(), client)
    waiter.wait(computeEnvironments=ce_names)
    logger.info("batch environments %s are ready", ce_names)


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
        page = {'NextToken': None}

        def _tags_match(name, tags):
            name_tags = [tag for tag in tags if
                         tag['Key'] == 'Name' and tag['Value'] == name]
            if not name_tags:
                return False

            platform_tags = [tag for tag in tags if
                             tag['Key'] == "Platform" and
                             tag['Value'] == constants.PLATFORM]
            return (len(platform_tags) > 0)

        while "NextToken" in page:
            kwargs = {'MaxResults': 10}
            if page['NextToken']:
                kwargs['NextToken'] = page['NextToken']

            logger.debug("listing file systems...")
            page = client.describe_file_systems(**kwargs)
            logger.debug("retrieved page with %d fs(es) %s", len(page['FileSystems']), page)
            if len(page['FileSystems']) == 0:
                return None
            matches = [candidate for candidate in page['FileSystems']
                       if _tags_match(self.name, candidate['Tags'])]
            if matches:
                return matches[0]

            if 'NextToken' in page:
                time.sleep(0.1)
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

        return self.__fs['FileSystemId'] if self.__fs else None

    def delete(self):
        """
        delete the filesystem
        """
        if self.__fs:
            fsid = self.__fs['FileSystemId']
        else:
            fs = self.retrieve_existing()
            print("retrieved: ", fs)
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
        logger.info("filesystem(s) %s (id=%s) ready", self.name, self.fsid)

    def wait_deleted(self):
        """wait for the filesystem to be deleted completely"""
        client = boto3.client('fsx')
        if self.fsid:
            waiter = botocore.waiter.create_waiter_with_client("FileSystemDeleted", _custom_waiters(), client)
            waiter.wait(FileSystemIds=[self.fsid])
        else:
            logger.info("filesystem not found")


class ComputeEnv(object):

    def __init__(self, name, scratch_size_gb=3600):

        #NAME_RE = re.compile("^[-a-zA-Z0-9_]{1,128}$")
        valid_name = "".join([x if (x.isalnum() or x in "_-") else "_" for x in name])
        valid_name = valid_name[0:128]

        self.name = valid_name
        self.disks = {}
        self.launch_template = None
        self.batch_ce = None
        self.submissions = {} # job_name: submitted_job_obj
        self.job_definitions = {} # keyed by (name, image)

        if scratch_size_gb > 0:
            self.disks['scratch'] = {
                'name': "scratch",
                'obj': FSxDisk(self.name + "-scratch", scratch_size_gb),
                'instance_mountpoint': "/mnt/" + self.name + "-scratch"
            }

    def register_simple_batch_jobdef(self, name, container_image):
        if (name, container_image) not in self.job_definitions:
            batch_def = jobs.AWSBatchSimpleJobDef(name, container_image)
            batch_def.register(self)
            self.job_definitions[(name, container_image)] = batch_def
        return self.job_definitions[(name, container_image)]

    def submit_simple_batch_job(self, job_name, job_def, **job_params):
        if job_name in self.submissions:
            raise ValueError("a job with that name has already been submitted: %s", job_name)

        job_obj = jobs.AWSBatchSimpleJob(job_name, job_def, **job_params)
        queue_arn = self.job_queue['jobQueueArn']
        job_obj.submit(queue_arn)
        self.submissions[job_name] = job_obj
        return job_obj

    def get_disk(self, diskname):
        if diskname in self.disks:
            return dict(self.disks[diskname])
        else:
            return None

    def _generate_instance_boot_script(self):
        """generates a cloud config script which mounts configured filesystems"""

        def _cmdsplit(lis):
            return "\n".join(["- %s" % (x,) for x in lis])

        mount_targets = []
        fstab_lines = []
        for diskname, disk in self.disks.items():
            mount_targets.append(disk['instance_mountpoint'])
            fstab_lines.append(disk['obj'].fstab(disk['instance_mountpoint']))

        # this stuff is run by a tool called cloud-init. it's not exactly obvious to debug because it happens early on
        # in the game. You can see the logs in the instance at /var/log/cloud-init-output.log And the script ends up
        # being re-formatted into: /var/lib/cloud/instance/scripts/runcmd
        #
        # the syntax is a bit arcane (it's yaml). contents are extracted and then "shellified" by a python
        # program and written to the runcmd script, which is then run by /bin/sh as root.
        #
        # FIXME: disallow whitespace and escapes from names provided by the user.
        script = """MIME-Version: 1.0
Content-Type: multipart/mixed; boundary="==MYBOUNDARY=="

--==MYBOUNDARY==
Content-Type: text/cloud-config; charset="us-ascii"

runcmd:
- [ "set", "-x"]
- [ "env" ]
- [ ":", "support both Amazon Linux 1 and 2" ]
- [ "sh", "-c", "amazon-linux-extras install -y lustre2.10 || yum install -y lustre-client" ]
%(mkdirs)s
%(fstabs)s
%(mount)s

--==MYBOUNDARY==
""" % {
        "mkdirs": _cmdsplit(["[mkdir, -p, %s]" % (mtpoint,) for mtpoint in mount_targets]),
        "fstabs": _cmdsplit(["""[sh, -c, "echo %s >> /etc/fstab"]""" % (fstab,) for fstab in fstab_lines]),
        "mount": "- [\":\"]" if len(self.disks) == 0 else """- [mount, "-a", "-t", lustre, defaults]"""
      }

        logger.debug("using the following instance launch script: %s", script)
        return script

    def _launch_template_name(self):
        return "%s-ce-launch-template-%s" % (constants.PLATFORM, self.name)

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

        lt_name = self._launch_template_name()

        instance_tags = [("platform", constants.PLATFORM),
                         ("compute_environment", self.name)]

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
                logger.debug("a template already exists with name %s", lt_name)
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

    @staticmethod
    def _find_matching_ce(name, top_level_match, comp_res_match, client=None):
        if not client:
            client = boto3.client("batch")
        # find a compute environment which matches the given settings
        paginator = client.get_paginator("describe_compute_environments")
        iterator = paginator.paginate()
        logger.debug("listing compute environments matching %s settings", name)
        found = None

        top_level_match = top_level_match if top_level_match else {}
        comp_res_match = comp_res_match if comp_res_match else {}

        for page in iterator:
            cenvs = page['computeEnvironments']
            if len(cenvs) == 0:
                break
            for cenv in cenvs:
                if not cenv['computeEnvironmentName'].startswith(name + "-"):
                    continue

                check_top_level = {k: cenv[k] for k in top_level_match}
                if top_level_match != check_top_level:
                    logger.debug("cannot use compute env %s because of mismatch: got %s, expected %s",
                                 check_top_level, top_level_match)
                    continue

                cr_mismatches = [key for key in comp_res_match
                                 if key not in cenv['computeResources'] or
                                 cenv['computeResources'][key] != comp_res_match[key]]
                if len(cr_mismatches) > 0:
                    logger.debug("cannot use compute env %s because of mismatch in compute resources: has %s but needs %s",
                                 cenv['computeResources'], comp_res_match)
                    continue

                found = cenv
                break

        if found:
            logger.info("batch compute environment %s (arn=%s) matches requirements for %s",
                        found['computeEnvironmentName'], found['computeEnvironmentArn'],
                        name)
        return found


    def _create_batch_ce(self):
        client = boto3.client("batch")

        ce_type = "SPOT" # "EC2"

        instance_profile_arn = config['batch_instance_profile_arn']
        spot_role_arn = config['spot_fleet_role_arn']
        service_role_arn = config['batch_service_role_arn']

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

        existing = self._find_matching_ce(self.name, {"serviceRole": service_role_arn, "type": "MANAGED"}, comp_resources)
        if existing:
            return existing

        logger.info("creating new batch compute environment associated to name %s", self.name)
        random_name = self.name + "-" + str(uuid4())[0:8]
        client.create_compute_environment(**{
            "computeEnvironmentName": random_name,
            "type": "MANAGED",
            "state": "ENABLED",
            "computeResources": comp_resources,
            "serviceRole": service_role_arn
        })
        new_env = client.describe_compute_environments(
            computeEnvironments=[random_name]
        )['computeEnvironments'][0]

        logger.info("created new batch compute environment name %s (arn=%s)",
                    new_env['computeEnvironmentName'],
                    new_env['computeEnvironmentArn'])
        return new_env

    def _create_job_queue(self):
        ce_name = self.batch_ce['computeEnvironmentName']
        jq = jobs.make_jobqueue(ce_name + "-jq",
                                compute_envs=[(ce_name, 100)])
        return jq

    def create(self):
        """ensure all the entities are created"""
        logger.info("creating compute environment %s", self.name)

        # create disks
        for name, ddict in self.disks.items():
            dobj = ddict['obj']
            dobj.create()

        self.batch_ce = self._create_batch_ce()

        # the compute environment has to be VALID in order for a jobqueue
        # to be associated with it.
        wait_batch_ce_ready([self.batch_ce['computeEnvironmentName']])

        self.job_queue = self._create_job_queue()

        logger.info("compute environment %s created", self.name)

    def delete(self):
        """ delete all entities associated with this compute environment
            this includes filesystems created for this compute environment.
        """

        # find all compute-environments defined with that name
        def _find_matching_envs(name):
            client = boto3.client('batch')
            paginator = client.get_paginator("describe_compute_environments")
            iterator = paginator.paginate()
            found = []
            for page in iterator:
                cenvs = page['computeEnvironments']
                if len(cenvs) == 0:
                    break
                for cenv in cenvs:
                    if not cenv['computeEnvironmentName'].startswith(name + "-"):
                        continue
                    found.append(cenv)
            return found

        def _find_matching_launch_templates(name, tags):
            """find templates by name and given tags"""
            client = boto3.client("ec2")
            paginator = client.get_paginator("describe_launch_template_versions")
            template_iterator = paginator.paginate(LaunchTemplateName=name)
            found = []

            tag_dict = {x[0]: x[1] for x in tags}
            for page in template_iterator:
                versions = page['LaunchTemplateVersions']
                if len(versions) == 0:
                    break
                for version in versions:
                    print(version)
                    instance_tags = [specs['Tags'] for specs in version['LaunchTemplateData']['TagSpecifications']
                                     if specs['ResourceType'] == "instance"][0]

                    check_dict = {x['Key']: x['Value'] for x in instance_tags}
                    if any([True for xk in tag_dict if
                            xk not in check_dict or check_dict[xk] != tag_dict[xk]]):
                        continue
                    found.append(version)
                return found

        for name, ddict in self.disks.items():
            dobj = ddict['obj']
            dobj.delete()

        matching_ces = []

        if self.batch_ce:
            matching_ces = [self.batch_ce]
        else:
            matching_ces = _find_matching_envs(self.name)
        if not matching_ces:
            logger.error("could not find compute environment matching name: %s", self.name)
            return None

        ce_names = [ce['computeEnvironmentName'] for ce in matching_ces]
        job_queue_names = [ce_name + "-jq" for ce_name in ce_names]

        batch = boto3.client('batch')

        # - set job queue to disabled
        for jq_name in job_queue_names:
            logger.info("disabling job queue %s", jq_name)
            batch.update_job_queue(jobQueue=jq_name, state='DISABLED')

        # - set compute environment to disabled
        for ce_name in ce_names:
            logger.info("disabling compute environment %s", ce_name)
            batch.update_compute_environment(computeEnvironment=ce_name, state='DISABLED')

        # - wait for job queue to settle.
        if job_queue_names:
            jobs.wait_queue_disabled(job_queue_names)

        # - delete job queue
        for jq_name in job_queue_names:
            logger.info("deleting job queue %s", jq_name)
            #batch.delete_job_queue(jobQueue=jq_name)
        self.job_queue = None

        # - delete launch template(s)
        instance_tags = [("platform", constants.PLATFORM),
                         ("compute_environment", self.name)]
        lt_name = self._launch_template_name()
        job_template_versions = _find_matching_launch_templates(lt_name, instance_tags)
        ec2 = boto3.client('ec2')
        # ec2.delete_launch_template_versions(
        #     DryRun=True|False,
        #     LaunchTemplateId='string',
        #     LaunchTemplateName='string',
        #     Versions=[
        #         'string',
        #     ]
        # )
        # response = client.delete_launch_template(
        #     DryRun=True|False,
        #     LaunchTemplateId='string',
        #     LaunchTemplateName='string'
        # )
        for version in job_template_versions:
            logger.info("job template: %s", version)

        # delete compute env

    def wait_ready(self):
        """ensure all the entities are VALID and ready to execute things"""
        for name, ddict in self.disks.items():
            dobj = ddict['obj']
            dobj.wait_ready()

        wait_batch_ce_ready([self.batch_ce['computeEnvironmentName']])
        jobs.wait_queue_ready([self.job_queue['jobQueueArn']])

    def wait_for_jobs(self, condition=None, interval=2*60):
        """
        poll submitted jobs repeatedly (every `interval` seconds), until a condition is satisfied.

        condition(status_map) -> bool   (True if and only if the condition is satisfied)
        status_map is a dictionary of job states:

        {
         "SUBMITTED": [ (id0, reason),  (id1, reason), (id2, reason) ],
         ...
        }

        each key is a state in the set SUBMITTED | PENDING | RUNNABLE | STARTING | RUNNING | SUCCEEDED | FAILED
        each value is a list of AWS job ids and associated statusReason. states are omitted if there are no jobs
        in that state.

        the return value of this call is the same as the status_map, but with job objects instead of submissionids.
        """

        id_map = {obj.job_id: obj for obj in self.submissions.values()}

        if not condition:
            def condition(status_map):
                return True

        for job_name, job_obj in self.submissions.items():
            logger.debug("waiting for job name=%s job_id=%s", job_name, job_obj.job_id)

        if self.submissions:
            id_status = jobs.wait_for_jobs([job_id for job_id in id_map], condition=condition, interval=interval)

            # convert Ids back into job objects
            obj_status = {}
            for state in id_status:
                obj_status[state] = [(id_map[job_id], reason) for (job_id, reason) in id_status[state]]

            # clear submitted jobs from list of submissions.
            #
            # XXX Rather than rely on self.submissions, we could retrieve the list of all jobs associated with the
            #     compute environment.  But we risk removing jobs that were run by a different process under a compute
            #     environment of the same name.
            completed = obj_status.get('SUCCEEDED', []) + obj_status.get('FAILED', [])
            if len(completed) > 0:
                logger.debug("clearing %d submission(s)...", len(completed))
            for (job_obj, status) in completed:
                del self.submissions[job_obj.name]

            return obj_status

        else:
            return {}

    def wait_deleted(self):
        """ensure all the entities are deleted completely"""
        for name, ddict in self.disks.items():
            dobj = ddict['obj']
            dobj.wait_deleted()


def _create_ecs_instance_role():
    # create ecs instance role
    client = boto3.client("iam")
    ecs_role_name = constants.CE_ECS_INSTANCE_ROLE
    logger.info("creating IAM role %s", ecs_role_name)
    try:
        trust_file = data_files("permissions/%s-ecs-instance-trust-relationship.json" % (constants.PLATFORM,))[0]
        with open(trust_file, "r") as fd:
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


def _setup_roles(**kwargs):
    instance_role = _create_ecs_instance_role()
    spot_role = _create_ec2_spot_fleet_role()
    batch_service_role = _create_batch_service_role()
    instance_profile = _create_batch_instance_profile(instance_role['Role']['RoleName'])
    job_role = jobs.create_job_role()

    role_settings = {
        'instance_role_name': instance_role['Role']['RoleName'],
        'instance_role_arn': instance_role['Role']['Arn'],
        'batch_service_role_name': batch_service_role['Role']['RoleName'],
        'batch_service_role_arn': batch_service_role['Role']['Arn'],
        'spot_fleet_role_name': spot_role['Role']['RoleName'],
        'spot_fleet_role_arn': spot_role['Role']['Arn'],
        'batch_instance_profile_name': instance_profile['InstanceProfile']['InstanceProfileName'],
        'batch_instance_profile_arn': instance_profile['InstanceProfile']['Arn'],
        'job_role_name': job_role['Role']['RoleName'],
        'job_role_arn': job_role['Role']['Arn']
    }
    import json
    outfile = "environment-settings.json"
    with open(outfile, "w") as envfd:
        envfd.write(json.dumps(role_settings, indent=4, sort_keys=True, separators=(',', ': ')))
    logger.info("environment settings written to %s: %s", outfile,
                role_settings)

def _list_env(**kwargs):
    client = boto3.client('batch')

    def _find_matching_envs():
        paginator = client.get_paginator("describe_compute_environments")
        iterator = paginator.paginate()
        found = []
        for page in iterator:
            cenvs = page['computeEnvironments']
            if len(cenvs) == 0:
                break
            for cenv in cenvs:
                cenv_tags = cenv['computeResources']['tags']
                if not cenv_tags.get("platform", None) == constants.PLATFORM:
                    continue
                found.append(cenv)
        return found

    found = _find_matching_envs()

    summary = []
    for cenv in found:
        tags = cenv['computeResources']['tags']
        if 'ce_name' in tags:
            summary.append((tags['ce_name'], cenv['computeEnvironmentName']))
    if summary:
        print("#ENV\tCENAME")
    for item in sorted(summary):
        print("%s\t%s" % item)

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


def configure_parser(main_subparsers):
    parser = main_subparsers.add_parser("env", help="commands concerning compute environments")

    subparsers = parser.add_subparsers(help="specify an operation on compute environments", dest="env_command")

    subp = subparsers.add_parser("create", help="create a new environment and wait for it to be ready.",
                                 description="Create a new environment and wait for it to be ready. "
                                 "If the environment already exists, the program will wait for it to be"
                                 " ready.")
    subp.set_defaults(func=_create_env)
    subp.add_argument("envname", metavar="ENVNAME", type=str, help="the name of the new environment")

    subp = subparsers.add_parser("delete", help="delete/teardown a compute environment")
    subp.set_defaults(func=_delete_env)
    subp.add_argument("envname", metavar="ENVNAME", type=str, help="the name of the environment")

    subp = subparsers.add_parser("list", help="list existing compute environments")
    subp.set_defaults(func=_list_env)

    subp = subparsers.add_parser("setup", help="setup roles and permissions to support compute environments",
                                 description="This creates the roles and permissions to create compute environments. "
                                 "You would call this once before using the platform, and forget about it.")
    subp.set_defaults(func=_setup_roles)
