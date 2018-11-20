#!/usr/bin/env python3
"""
   reads urls on stdin, and calls the import of multiple samples in parallel.
   Each line will invoke data-import lambda to handle the url and digests


   Input syntax:

   INPUTURL OUTPUTURL [md5:MD5URL]

"""
import os, os.path, sys
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
import botocore

import bunnies.lambdas as lambdas
import bunnies

DATA_IMPORT_LAMBDA = "data-import"

def setup_logging(loglevel=logging.INFO):
    """configure custom logging for the platform"""

    root = logging.getLogger(__name__)
    root.setLevel(loglevel)
    ch = logging.StreamHandler(sys.stderr)
    ch.setLevel(loglevel)
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)
    #root.propagate = False

log = logging.getLogger(__name__)

def do_request(req):
    """execute one request. tail the logs. wait for completion"""
    log.debug("do_request: %s", req)
    code, response = lambdas.invoke_sync(DATA_IMPORT_LAMBDA, Payload=req)
    data = response['Payload'].read()
    if code != 0:
        raise Exception("The lambda failed: %s" + str(response))
    return json.loads(data.decode('ascii'))

def main_handler():
    """do the work. CLI"""

    import argparse
    import yaml

    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument("--threads", metavar="THREADS", type=int, default=1)
    parser.add_argument("--tmpbucket", metavar="TMPBUCKET", help="bucket name in which to store temporary files. optional", default='')
    parser.add_argument("--creds", metavar="CREDSFILE", type=str, default=None,
                        help="credentials file (yaml) with username and password key")

    args = parser.parse_args()

    creds = {}
    creds['username'] = os.environ.get('USERNAME', '')
    creds['password'] = os.environ.get('PASSWORD', '')
    tmp_bucket = os.environ.get('TMPBUCKET', '')

    if args.creds:
        with open(args.creds, "r") as stream:
            try:
                creds = yaml.load(stream)
                username = creds.get('username', '')
                password = creds.get('password', '')
            except yaml.YAMLError as exc:
                raise
    else:
        creds=None

    digests = []

    if args.tmpbucket:
        tmp_bucket = args.tmpbucket

    def _parse_line_request(line):
        toks = line.split()
        if len(toks) < 2:
            raise ValueError("not enough parameters")

        url, outputurl = toks[0:2]
        digests = []
        for digestarg in toks[2:]:
            dtype, durl = digestarg.split(':', maxsplit=1)
            digests.append([dtype, durl])

        return {
            'input': url,
            'output': outputurl,
            'digests': digests
        }

    requests = []
    for lineno, line in enumerate(sys.stdin):
        line = line.strip()
        if line.startswith("#") or not line: continue
        try:
            req = _parse_line_request(line)
            requests.append((lineno+1, req))
        except ValueError as verr:
            log.error("Problem on line %d: %s", lineno+1, str(verr))
            return 1

    futures = []
    log.info("Submitting %d requests with %d threads...", len(requests), args.threads)

    exception_count = 0
    upload_error_count = 0
    results = {}
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        futures = { executor.submit(do_request, {"requests": [req]}): lineno for (lineno, req) in requests }
        for future in as_completed(futures):
            lineno = futures[future]
            try:
                log.info("request on line %d completed.", lineno)
                data = future.result()
                results[lineno] = data
            except Exception as exc:
                log.error("request on line %d generated an exception: %s" % (lineno, exc), exc_info=exc)
                error_count += 1
                results[lineno] = {"error": str(exc)}

    upload_results = []
    for lineno in sorted(results.keys()):
        result = results[lineno]
        if result['error_count'] > 0:
            upload_error_count += result['error_count']
        upload_results += result['results']

    json.dump(upload_results, sys.stdout, sort_keys=True, indent=4, separators=(",", ": "))

    if exception_count > 0 :
        return 1


if __name__ == "__main__":
    setup_logging(logging.DEBUG)
    log.info("Running boto3:%s botocore:%s", boto3.__version__, botocore.__version__)
    bunnies.setup_logging(logging.DEBUG)
    ret = main_handler()
    sys.exit(0 if ret is None else ret)


