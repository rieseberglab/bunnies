#!/usr/bin/env python3

"""
   Helper functions to work with lambda submission and log collection
"""

import sys
import os, os.path

import boto3
import botocore.config

import json
import logging
import base64
import io
from . import setup_logging
log = logging.getLogger(__package__)

DATA_IMPORT = "data-import"
DATA_REHASH = "data-rehash"

def get_lambda_client():
    """return a client that can wait more than 60 seconds for the result of a lambda"""
    if get_lambda_client.client:
        return get_lambda_client.client
    session = boto3.session.Session()
    lambda_client = session.client('lambda', config=botocore.config.Config(read_timeout=910, retries={'max_attempts': 0}))
    get_lambda_client.client = lambda_client
    return lambda_client

get_lambda_client.client = None

def default_context():
    """See ClientContext syntax here: https://docs.aws.amazon.com/mobileanalytics/latest/ug/PutEvents.html"""
    uname = os.uname()

    return {
        "client": {
            "client_id": "bunnies-python3.6-lambda-0.1",
            "app_title": "bunnies platform client",
            "app_version_name": "0.1",
            "app_version_code": "1",
            "app_package_name": "bunnies-lambda-0.1"
        },
        "custom": {},
        "env": {
            "platform": uname.sysname,
            "model": "",
            "make": uname.machine,
            "platform_version": uname.release,
            "locale": os.environ.get("LANG", "")
        },
        "services": {}
    }

def invoke_sync(function_name, client_context=None, Payload=None, LogType='Tail', Qualifier="$LATEST"):

    obj_context = default_context()

    if not client_context:
        client_context = {}
    elif isinstance(client_context, str):
        client_context = json.loads(client_context)
    elif isinstance(client_context, bytes):
        client_context = json.loads(client_context.decode('ascii'))

    obj_context.update(**client_context)

    context_b64 = json.dumps(obj_context)
    context_b64 = base64.b64encode(context_b64.encode('ascii')).decode('ascii')

    l = get_lambda_client()

    if isinstance(Payload, dict):
        Payload=json.dumps(Payload, separators=(',',':')).encode('ascii')
        Payload=io.BytesIO(Payload)

    log.info("Invoking lambda name:%s qualifier:%s", function_name, Qualifier)
    response = l.invoke(FunctionName=function_name, ClientContext=context_b64, Payload=Payload, LogType=LogType, Qualifier=Qualifier)

    resp_payload = response['Payload']

    if 'LogResult' in response:
        response['LogResult'] = base64.b64decode(response['LogResult']).decode('ascii')
    else:
        response['LogResult'] = ''

    summary = {k:response[k] for k in ["StatusCode", "ExecutedVersion", "FunctionError"] if k in response}
    log.info("Result: %s", summary)

    if response.get('LogResult'):
        for line in response['LogResult'].splitlines():
            log.info("LOG: %s", line.rstrip())

    if 'FunctionError' in response:
        if response['FunctionError'] == "Handled":
            log.debug("Received Handled error.")
            return (1, response)
        if response['FunctionError'] == "Unhandled":
            log.debug("Received Unhandled error.")
            return (2, response)
        log.error("Unknown FunctionError value: %s", response['FunctionError'])
        return (3, response)

    return (0, response)

def main():
    """
    invoke a lambda synchronously, reading the payload from stdin.
    the last 4K of lambda logs are written to stderr.

      exit status of 0 indicates lambda success.
      exit status of 1 indicates a lambda user-defined error
      exit status of 2 indicates an unhandled lambda error (out of mem, out of time, etc.)
    """
    import argparse
    import yaml

    setup_logging(loglevel=logging.DEBUG)
    parser = argparse.ArgumentParser(description=main.__doc__)

    parser.add_argument("lambda_name", metavar="LAMBDANAME", type=str, help="the short name, partial ARN, or full ARN of the lambda to invoke")
    parser.add_argument("--context", metavar="CLIENTCTX", type=str, help="a json object with custom key value pairs to merge into the client context")
    parser.add_argument("--tail", action="store_true", dest="tail", default=True)
    parser.add_argument("--no-tail", action="store_false", dest="tail")

    args = parser.parse_args()

    exit_code, resp = invoke_sync(args.lambda_name, client_context=args.context, Payload=sys.stdin.read(), LogType=("Tail" if args.tail else 'None'))

    resp_payload = resp['Payload']
    while True:
        buf = resp_payload.read(8192).decode('ascii')
        if not buf: break
        sys.stdout.write(buf)

    return exit_code

if __name__ == "__main__":
    sys.exit(main() or 0)
