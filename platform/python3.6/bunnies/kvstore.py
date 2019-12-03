#!/usr/bin/env python3

"""
   Helper functions to work with lambda submission and log collection
"""
import boto3
from .constants import PLATFORM
from .version import __version__
from python_dynamodb_lock.python_dynamodb_lock import DynamoDBLockClient
from botocore.exceptions import ClientError

from contextlib import contextmanager

import time
import logging
log = logging.getLogger(__name__)

TABLE_VERSION = "1"
TABLE_NAME = PLATFORM + "_" + TABLE_VERSION


def lock_client():
    if not lock_client.client:
        # get a reference to the DynamoDB resource
        dynamodb_resource = boto3.resource('dynamodb')
        # create the lock-client
        lock_client.client = DynamoDBLockClient(dynamodb_resource)

        # FIXME close the lock_client on shutdown
        #lock_client.close()
    return lock_client.client


lock_client.client = None


def ddb_client():
    if not ddb_client.client:
        ddb_client.client = boto3.client("dynamodb")
    return ddb_client.client


ddb_client.client = None


def _create_job_table(client=None):
    if client is None:
        client = boto3.client("dynamodb")

    log.info("creating job submission table")
    try:
        response = client.create_table(
            AttributeDefinitions=[
                {
                    'AttributeName': 'jobname',
                    'AttributeType': 'S'
                }
            ],
            TableName=TABLE_NAME,
            KeySchema=[
                {
                    'AttributeName': 'jobname',
                    'KeyType': 'HASH'
                }
            ],
            BillingMode='PAY_PER_REQUEST',
            StreamSpecification={
                'StreamEnabled': False,
            },
            SSESpecification={
                'Enabled': False
            },
            Tags=[
                {
                    'Key': 'platform',
                    'Value': PLATFORM
                },
                {
                    'Key': "BUNNIES_VERSION",
                    'Value': __version__,
                },
                {
                    'Key': "TABLE_VERSION",
                    'Value': TABLE_VERSION
                }
            ]
        )
        log.debug("created table: %s", response)
    except ClientError as clierr:
        if clierr.response['Error']['Code'] == 'ResourceInUseException':
            log.info("table already exists. skipping...")
            pass
        else:
            raise


def _setup_kv(**kwargs):
    """setup lock table and job completion tables in dynamodb"""
    ddb = boto3.client("dynamodb")
    # https://python-dynamodb-lock.readthedocs.io/en/latest/python_dynamodb_lock.html
    log.info("creating dynamodb_lock table")
    try:
        DynamoDBLockClient.create_dynamodb_table(ddb)
    except ClientError as clierr:
        if clierr.response['Error']['Code'] == 'ResourceInUseException':
            log.info("table already exists. skipping...")
            pass
        else:
            raise

    _create_job_table(ddb)


class SubmissionEntry(object):

    __slots__ = ("jobname", "submitter",
                 "jobtype", "jobdata", "jobattempt",
                 "updated_on")

    TBL_ATTRIBUTES = {
        "jobname": "S",
        "submitter": "S",
        "jobtype": "S",
        "jobdata": "S",
        "jobattempt": "N",
        "updated_on": "N"
    }

    def __init__(self, jobname, submitter="", jobtype="batch", jobdata="", jobattempt=1, updated_on=0):
        super(SubmissionEntry, self).__init__()
        self.jobname = jobname
        self.submitter = submitter
        self.jobtype = jobtype
        self.jobdata = jobdata
        self.jobattempt = jobattempt
        self.updated_on = updated_on or time.time()

    # invoke while lock is held
    def load(self):
        client = ddb_client()
        response = client.get_item(
            TableName=TABLE_NAME,
            Key={
                'jobname': {'S': self.jobname}
            },
            ConsistentRead=True,
            ProjectionExpression=",".join(self.TBL_ATTRIBUTES)
        )
        if 'Item' not in response:
            log.debug("key %s not found", self.jobname)
            return None

        log.debug("loading key %s: resp=%s", self.jobname, response)
        item = response['Item']

        def _convert(vval):
            if 'S' in vval:
                x = vval['S']
                if x == "-":
                    x = ""
                return x
            if 'N' in vval:
                return float(vval['N'])

        for attrname, vval in item.items():
            setattr(self, attrname, _convert(vval))

    # invoke while lock is held
    def save(self):
        client = ddb_client()
        self.updated_on = time.time()

        def _convert(x):
            if x == "":
                # values cannot be empty
                return "-"
            else:
                return str(x)

        item = {attrname: {attrtype: _convert(getattr(self, attrname))}
                for attrname, attrtype in self.TBL_ATTRIBUTES.items()}

        # validate fields -- no empty strings allowed
        for itemk, itemv in item.items():
            if "S" in itemv and not itemv['S']:
                raise ValueError("item attribute %s cannot be empty" % (itemk,))

        response = client.put_item(
            TableName=TABLE_NAME,
            Item=item)
        log.debug("updated item: %s", response)

    def submitted(self, jobtype, jobdata, attempt):
        self.jobtype = jobtype
        self.jobdata = jobdata
        self.attempt = attempt
        self.save()

    def __job_state(self):
        if self.__is_stale():
            return "NEW"

        # if batch status is unknown,
        return ("NEW", None, 0)
        # if batch status is CANCELLED,
        return ("NEW", None, 0)
        # if batch status is FAILED
        return ("RESTART", self.jobdata, self.jobattempt)
        # if batch status is DONE
        return ("DONE", self.jobdata, self.jobattempt)
        # else (running, pending, etc)
        return ("RUNNING", self.jobdata, self.jobattempt)


@contextmanager
def submit_lock_context(owner_name, jobname):
    lock = None
    try:
        # acquire the lock
        lock = lock_client().acquire_lock("lock." + jobname)
        entry = SubmissionEntry(jobname, owner_name)
        yield entry
    finally:
        if lock:
            try:
                lock.release()
            except Exception as exc:
                log.error("could not release lock: lock.%s", jobname, exc_info=exc)


def configure_parser(main_subparsers):

    parser = main_subparsers.add_parser("kv", help="commands concerning key-value store")
    subparsers = parser.add_subparsers(help="specify an operation on jobs", dest="jobs_command")

    subp = subparsers.add_parser("setup", help="setup roles and permissions to support compute environments",
                                 description="This creates the roles and permissions to create compute environments. "
                                 "You would call this once before using the platform, and forget about it.")
    subp.set_defaults(func=_setup_kv)
