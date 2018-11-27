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
from urllib.parse import urlparse

import bunnies.lambdas as lambdas
import bunnies

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

def _s3_split_url(objecturl):
    """splits an s3://foo/bar/baz url into bucketname, keyname: ("foo", "bar/baz")

    the keyname may be empty
    """
    o = urlparse(objecturl)
    if o.scheme != "s3":
        raise ValueError("not an s3 url")

    keyname = o.path
    bucketname = o.netloc

    # all URL paths start with /. strip it.
    if keyname.startswith("/"):
        keyname = keyname[1:]

    return bucketname, keyname

def do_request(batch_no, req):
    """execute one request. tail the logs. wait for completion"""
    log.debug("do_request: %s", req)
    req = dict(req)
    log.info("REQ%s data-import request: %s", batch_no, json.dumps(req, sort_keys=True, indent=4, separators=(",", ": ")))
    code, response = lambdas.invoke_sync(lambdas.DATA_IMPORT, Payload=req)
    data = response['Payload'].read().decode("ascii")
    if code != 0:
        raise Exception("data-import failed to complete: %s" % (data,))
    data_obj = json.loads(data)
    log.info("REQ%s data-import result: %s", batch_no, data_obj)
    if data_obj['error_count'] > 0:
        raise Exception("data-import returned an error: %s" % (data_obj["results"][0],))

    details = data_obj['results'][0]
    if not details.get("move_to"):
        # final result - no delete needed.
        return details

    log.info("%s moving temp file to final location: %s", batch_no, details["move_to"])
    tmp_src = _s3_split_url(details['output'])
    cpy_dst = _s3_split_url(details['move_to'])

    new_req = {
        "src_bucket": tmp_src[0],
        "src_key": tmp_src[1],
        "dst_bucket": cpy_dst[0],
        "dst_key": cpy_dst[1],
        "src_etag": details["ETag"],
        "digests": details["digests"]
    }
    try:
        log.info("REQ%s data-rehash request: %s", batch_no, json.dumps(new_req, sort_keys=True, indent=4, separators=(",", ": ")))
        code, response = lambdas.invoke_sync(lambdas.DATA_REHASH, Payload=new_req)
        data = response['Payload'].read().decode("ascii")
        if code != 0:
            raise Exception("data-rehash failed to complete: %s" % (data,))
        data_obj = json.loads(data)
        if data_obj.get('error', None):
            raise Exception("data-rehash returned an error: %s" % (data["results"][0],))
        return data_obj
    finally:
        session = boto3.session.Session()
        s3 = session.client('s3', config=botocore.config.Config(read_timeout=300, retries={'max_attempts': 0}))
        log.info("REQ%s deleting temp file: Bucket=%s Key=%s", batch_no, tmp_src[0], tmp_src[1])
        try:
            s3.delete_object(Bucket=tmp_src[0], Key=tmp_src[1])
        except Exception as delete_exc:
            log.error("REQ%s delete failed", exc_info=delete_exc)

def main_handler():
    """do the work. CLI"""

    import argparse

    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument("--threads", metavar="THREADS", type=int, default=1)
    parser.add_argument("--tmpbucket", metavar="TMPBUCKET", help="bucket name in which to store temporary files. optional", default='')

    args = parser.parse_args()

    creds = {}
    creds['username'] = os.environ.get('USERNAME', '')
    creds['password'] = os.environ.get('PASSWORD', '')
    tmp_bucket = os.environ.get('TMPBUCKET', None)

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

        req_obj = {
            'download_only': True, # skip the tail end copy.
            'input': url,
            'output': outputurl,
            'digests': digests
        }
        if args.tmpbucket is not None:
            req_obj["tmp_bucket"] = args.tmpbucket
        return req_obj

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

    total_count = len(requests)
    upload_done_count = 0
    upload_error_count = 0
    results = {}
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        futures = { executor.submit(do_request, lineno, {"requests": [req]}): lineno for (lineno, req) in requests }
        for future in as_completed(futures):
            lineno = futures[future]
            upload_done_count += 1
            try:
                log.info("request on line %d completed.", lineno)
                data = future.result()
                results[lineno] = data
                log.info("request result: %s", json.dumps(data, sort_keys=True, indent=4, separators=(",", ": ")))
            except Exception as exc:
                log.error("request on line %d generated an exception: %s", lineno, exc, exc_info=exc)
                error_result = dict(requests[lineno])
                error_result["error"] = str(exc)
                results[lineno] = error_result
                upload_error_count += 1

            log.info("progress %4d/%4d done (%8.3f%%). %d error(s) encountered.",
                     upload_done_count, total_count, upload_done_count * 100.0 / total_count, upload_error_count)

    upload_results = [results[lineno] for lineno in sorted(results.keys())]

    json.dump(upload_results, sys.stdout, sort_keys=True, indent=4, separators=(",", ": "))
    if upload_error_count > 0:
        log.error("exiting. encountered %d error(s) out of %d requests.", upload_error_count, total_count)
        return 1
    log.info("completed %d requests.", total_count)
    return 0

if __name__ == "__main__":
    setup_logging(logging.DEBUG)
    log.info("Running boto3:%s botocore:%s", boto3.__version__, botocore.__version__)
    bunnies.setup_logging(logging.DEBUG)
    ret = main_handler()
    sys.exit(0 if ret is None else ret)


