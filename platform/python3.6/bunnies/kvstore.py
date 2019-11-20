#!/usr/bin/env python3

"""
   Helper functions to work with lambda submission and log collection
"""
import boto3
from .constants import PLATFORM
from .version import __version__

TABLE_NAME = PLATFORM + "_" + __version__.replace(".", "_")


def _delete_kv(**kwargs):
    pass


def _setup_kv(**kwargs):
    ddb = boto3.client("dynamodb")
    
    pass


def configure_parser(main_subparsers):

    parser = main_subparsers.add_parser("kv", help="commands concerning key-value store")
    subparsers = parser.add_subparsers(help="specify an operation on jobs", dest="jobs_command")

    subp = subparsers.add_parser("setup", help="setup roles and permissions to support compute environments",
                                 description="This creates the roles and permissions to create compute environments. "
                                 "You would call this once before using the platform, and forget about it.")
    subp.set_defaults(func=_setup_kv)
