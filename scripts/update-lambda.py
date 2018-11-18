#!/usr/bin/env python3

"""
   usage: update-lambda.py lambda_dir
"""
import os, os.path
import sys
import argparse
import zipfile
import tempfile
import logging
import json
import base64
import fnmatch
import errno
import subprocess
import shutil

import boto3
from botocore.exceptions import ClientError


EXCLUDES = [".metadata.json"]
EXCLUDE_PATTERNS = ["*~"]

REPO_ROOT = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))

PLATFORM_PKG = "bunnies"

PLATFORM_SRC = os.path.join(REPO_ROOT, "platform", "python3.6")

PLATFORM_EXTRA = [
    {"src": os.path.join(REPO_ROOT, "network-settings.json"),
     "dst": os.path.join(PLATFORM_PKG, "network-settings.json")},
    {"src": os.path.join(REPO_ROOT, "cluster-settings.json"),
     "dst": os.path.join(PLATFORM_PKG, "cluster-settings.json")}
]

def setup_logging():
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    ch = logging.StreamHandler(sys.stderr)

    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)

log = logging.getLogger("update-lambda")

def zip_lambda_dir(ziproot, zipfd):
    """
    zip files in folder, recursively, including empty folders, but
    excluding special files
    """
    def _add_entry(dirname, basename):
        if basename in EXCLUDES or any([fnmatch.fnmatchcase(basename, patt) for patt in EXCLUDE_PATTERNS]):
            return

        fullname = os.path.join(dirname, basename)
        relname = os.path.relpath(fullname, ziproot)
        zipfd.write(fullname, arcname=relname)
        log.info("added file %s", relname)

    for root, dirs, files in os.walk(ziproot):
        for basename in files: _add_entry(root, basename)
        for basename in dirs: _add_entry(root, basename)


def main():
    setup_logging()

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("lambdadir", metavar="LAMBDADIR", help="location where input lambda files are placed")
    parser.add_argument("--workdir", metavar="WORKDIR", type=str, default="/tmp/",
                        help="working directory (temp)")

    args = parser.parse_args()

    try:
        if not os.path.exists(args.workdir):
            os.makedirs(args.workdir)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    tmpfd = None
    platform_tmpdir = None
    try:

        # make temp dir to store platform files
        platform_tmpdir = tempfile.mkdtemp(suffix="_platform", prefix="reprod_", dir=args.workdir)

        # install platform tooling in lambda
        log.debug("installing platform module in lambda build directory %s", platform_tmpdir)
        try:
            cmd = ['pip', 'install', '-t', platform_tmpdir, PLATFORM_SRC + "[lambda]"]
            log.info("command: %s", cmd)
            subprocess.check_call(cmd)
        except subprocess.CalledProcessError as cpe:
            log.error("Command %s exited with error %d. output: %s", cpe.cmd, cpe.returncode, cpe.output)
            sys.exit(1)

        with tempfile.NamedTemporaryFile(suffix=".zip", prefix="update-lambda-", dir=args.workdir, delete=False) as tmpfd:
            with zipfile.ZipFile(tmpfd, mode='w', compression=zipfile.ZIP_DEFLATED) as zipfd:
                zip_lambda_dir(platform_tmpdir, zipfd)

                # add extra items
                for entry in PLATFORM_EXTRA:
                    zipfd.write(entry['src'], arcname=entry['dst'])
                    log.info("added file %s", entry['dst'])

                zip_lambda_dir(args.lambdadir, zipfd)

        # boto wants base64 encoded string data
        with open(tmpfd.name, 'rb') as zipfd:
            zipdata = zipfd.read()
            zip64 = base64.encodestring(zipdata)
    finally:
        # cleanup temp files
        if tmpfd: os.unlink(tmpfd.name)
        if platform_tmpdir: shutil.rmtree(platform_tmpdir)

    lambda_cli = boto3.client('lambda')
    iam_cli = boto3.resource('iam')

    with open(os.path.join(args.lambdadir, '.metadata.json'), "r") as metafd:
        metadata = json.load(metafd)

        lambdas = []
        for definition in metadata:
            updated = dict(definition)
            rolename = definition['Role'] = definition['Role']
            role = iam_cli.Role(rolename)
            role.load()

            updated['Role'] = role.arn
            updated['Code'] = {'ZipFile': zipdata}

            lambda_name = updated['FunctionName']
            try:
                log.info("Creating lambda %s...", lambda_name)
                lambdas.append(lambda_cli.create_function(**updated))
            except ClientError as err:
                if err.response['Error']['Code'] == 'ResourceConflictException':
                    log.info("Creation failed: %s", err)
                    now_func = lambda_cli.get_function_configuration(FunctionName=lambda_name)
                    current_rev = now_func['RevisionId']
                    log.info("Lambda %s already exists. updating code...", lambda_name)
                    code_update = lambda_cli.update_function_code(**{
                        "FunctionName": lambda_name,
                        "ZipFile": zipdata,
                        "RevisionId": current_rev
                    })
                    del updated['Code']
                    updated['RevisionId'] = code_update['RevisionId']
                    log.info("Lambda %s already exists. updating config...", lambda_name)
                    config_update = lambda_cli.update_function_configuration(**updated)
                    lambdas.append(config_update)
                else:
                    raise

if __name__ == "__main__":
    main()
    sys.exit(0)
