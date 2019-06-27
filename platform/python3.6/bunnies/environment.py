#!/usr/bin/env python3


"""
  Tools for managing a bunnies compute environment
"""
import boto3
from uuid import uuid4
import logging

from .config import config
from . import constants
import botocore
import botocore.waiter

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
            logger.info("retrieved page with %d fses", len(page['FileSystems']))
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

class ComputeEnv(object):
    def __init__(self, name, scratch_size_gb=3600):
        self.name = name
        self.disks = {}
        if scratch_size_gb > 0:
            self.disks['scratch'] = {
                'name': name + "-scratch",
                'obj': FSxDisk(name + "-scratch", scratch_size_gb),
                'instance_mountpoint': "/mnt/" + name + "-scratch"
            }

    def create(self):
        """ensure all the entities are created"""
        logger.info("creating compute environment %s", self.name)

        for name, ddict in self.disks.items():
            dobj = ddict['obj']
            dobj.create()

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
