#!/usr/bin/env python3


"""
  Tools for managing a bunnies compute environment
"""
import boto3
from uuid import uuid4
import logging

from .config import config
from . import constants

logger = logging.getLogger(__package__)


def get_key_name():
    return config["KeyName"]


def get_subnet_id():
    return config["subnet_id"]


def get_security_group_id():
    return config["security_group_id"]


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
            page = client.describe_file_systems(**kwargs)
            matches = [candidate for candidate in page['FileSystems']
                       if _tags_match(self.name, candidate['Tags'])]
            if matches:
                return matches[0]
        return None

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
            logger.info("Reusing existing filesystem: %s", exists['ResourceARN'])
            self.__fs = exists
        else:
            logger.info("Creatign Lustre FSx filesystem... Name=%s", self.name)
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

    def ready(self):
        """wait for the filesystem to be in the READY state"""
        # FIXME custom boto waiter

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
        logger.info("Creating compute environment %s", self.name)

        for name, ddict in self.disks.items():
            dobj = ddict['obj']
            dobj.create()

    def ready(self):
        """ensure all the entities are VALID and ready to execute things"""
        for name, ddict in self.disks.items():
            dobj = ddict['obj']
            dobj.ready()

    def delete(self):
        """ delete all entities associated with this compute environment
            this includes filesystems created for this compute environment.
        """
        for name, ddict in self.disks.items():
            dobj = ddict['obj']
            dobj.delete()
