#!/usr/bin/env python3

"""
   Data importer for Reproducible experiments.

   This program imports data from the web (as a URL) into S3.

"""


# For Lambdas:
#   Multiple files can be imported at once, as long
#   all transfers can be completed within the lambda
#   time limit. (900 seconds)
#
#
# Existing output files will not be overwriten.
# event = {
#     "requests": [
#         {
#             "input": "http://...",
#             "digests": [("md5": "http://...")],
#             "output": "s3://..."
#         }
#     ]
# }
#
# output = {
#     "error_count": int,
#     "results": [
#        {"input": inputurl, "output": finaloutputurl, "digests": {"md5": ..., "sha1": ...}}, # ok result
#        {"input": inputurl, "output": outputurl, "error": "error message"}, ...
#     ]
# }


import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

import json
import os, sys
import requests
import logging
import uuid
from urllib.parse import urlparse
from helpers import ProgressPercentage, HashingReader

import bunnies

# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3.html
META_PREFIX = "x-amz-meta-"
DIGEST_HEADER = META_PREFIX + "digest-" # + algo.lowercase()

def setup_logging(loglevel=logging.INFO):
    """configure custom logging for the platform"""
    root = logging.getLogger(__name__)
    root.setLevel(loglevel)
    ch = logging.StreamHandler(sys.stderr)
    ch.setLevel(loglevel)
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)

setup_logging(logging.DEBUG)

log = logging.getLogger(__name__)

class ImportError(Exception):
    pass

def _url_type(url):
    o = urlparse(url)
    if o.scheme == "s3":
        return "s3"
    elif o.netloc == "genomequebec.mcgill.ca" and o.path.startswith("/nanuqMPS/"):
        return "nanuq"
    elif o.scheme in ("http", "https"):
        return "http"
    else:
        # not handled
        return None

def _nanuq_get(url, username="", password="", logprefix=""):
    """returns an open request object to a NANUQ download"""
    # nanuq has a non-standard form auth
    log.info("%s GET %s (username=%s)", logprefix, url, username or "")

    if username:
        r = requests.post(url, data={'j_username': username,
                                     'j_password': password or ''},
        stream=True)
    else:
        r = requests.get(url, stream=True)

    if len(r.history) > 0:
        oldest = r.history[0]
        if oldest.status_code > 300 and oldest.status_code < 400:
            log.info("%s  redirected %s %s", logprefix, oldest.status_code, oldest.headers.get("location"))
            if "j_security_check" in oldest.headers.get("location", ""):
                log.info("%sdownload failed. bad credentials.", logprefix)
                raise ImportError("download failed")

    if r.status_code >= 400:
        log.info("%sdownload failed with code %s.", logprefix, r.status_code)
        raise ImportError("download failed with code %s" % (r.status_code,))

    content_length = r.headers.get('content-length', None)
    content_type = r.headers.get('content-type', None)

    if not content_length:
        # nanuq doesn't do chunked
        raise ImportError("Expected content-length")

    content_length = int(content_length, 10)
    log.info("%s  content-length %s  content-type %s", logprefix, content_length, content_type)
    return (r.headers, r.raw)

def _s3_get(url, logprefix="", **kwargs):
    o = urlparse(url)
    bucketname = o.netloc
    keyname = o.path
    s3 = boto3.resource('s3')
    obj = s3.Object(bucketname, keyname)
    try:
        data = obj.get()
        return (data, data['Body'])
    except ClientError as clierr:
        log.error("%sdownload error", log_prefix, exc_info=clierr)
        raise ImportError("Error for URL %s: %s" % (url, clierr['Error']['Code']))

def _http_get(url, logprefix="", **kwargs):
    """http or https GET"""
    log.info("%sdownloading %s", logprefix, url)

    r = requests.get(url, stream=True)

    if r.status_code >= 400:
        raise ImportError("download failed with code %s" % (r.status_code,))

    content_length = r.headers.get('content-length', None)
    if content_length:
        content_length = int(content_length, 10)

    log.info("%s  content-Length %s  content-type %s", logprefix, content_length, r.headers.get('content-type', "n/a"))
    return (r.headers, r.raw)

def _get(url, creds=None, logprefix=""):
    """returns an open request handle to a file"""
    url_type = _url_type(url)
    if url_type == "s3":
        return _s3_get(url, logprefix=logprefix)
    elif url_type == "nanuq":
        creds = creds or {}
        username, password = creds.get('username', ""), creds.get('password', "")
        return _nanuq_get(url, username=username, password=password, logprefix=logprefix)
    elif url_type == "http":
        creds = creds or {}
        username, password = creds.get('username', ""), creds.get('password', "")
        return _http_get(url, username=username, password=password, logprefix=logprefix)
    else:
        raise ImportError("Url has unsupported backend: %s" % (url,))

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

def _s3_delete(objecturl, logprefix=""):
    bucketname, keyname = _s3_split_url(objecturl)
    s3 = boto3.client('s3')
    log.info("%s S3-DELETE bucket:%s key:%s", logprefix, bucketname, keyname)
    return s3.delete_object(Bucket=bucketname, Key=keyname)

def _s3_copy(srcurl, dsturl, content_type=None, meta=None, logprefix=""):
    src_bucketname, src_keyname = _s3_split_url(srcurl)
    dst_bucketname, dst_keyname = _s3_split_url(dsturl)

    s3 = boto3.client('s3')
    log.info("%s S3-COPY bucket:%s key:%s => bucket:%s key:%s", logprefix,
             src_bucketname, src_keyname,
             dst_bucketname, dst_keyname)
    log.info("%s   content-type:%s meta:%s", logprefix, content_type, meta)

    xtra = {}
    if content_type:
        xtra['ContentType'] = content_type
    result = s3.copy_object(CopySource={'Bucket': src_bucketname, 'Key': src_keyname},
                            Bucket=dst_bucketname, Key=dst_keyname, Metadata=meta or {}, **xtra)
    log.debug("%s S3-COPY complete: bucket:%s key:%s etag:%s", logprefix, dst_bucketname, dst_keyname,
              result['CopyObjectResult']['ETag'])
    return result

def _s3_put(inputfp, outputurl, content_type=None, content_length=-1, meta=None, logprefix=""):
    """Store the input fileobject under key outputurl

    returns nothing.
    """
    bucketname, keyname = _s3_split_url(outputurl)
    if not keyname:
        log.error("%sempty key given", logprefix)
        raise ImportError("empty key given")

    meta = meta or {}

    s3 = boto3.client('s3')
    config = TransferConfig(use_threads=False, max_concurrency=1)
    progress = ProgressPercentage(size=content_length, logprefix=logprefix, logger=log)

    extra_args = {
        'ContentType': content_type or 'application/octet-stream',
        'Metadata': {}
    }

    # part uploads don't need length
    #if content_length >= 0:
    #    extra_args['ContentLength'] = content_length

    if meta:
        extra_args['Metadata'].update(meta)

    log.info("%s S3-PUT bucket:%s key:%s extra:%s",
              logprefix, bucketname, keyname, extra_args)

    try:
        return s3.upload_fileobj(inputfp, bucketname, keyname,
                                 Config=config,
                                 ExtraArgs=extra_args,
                                 Callback=progress)
    except ClientError as clierr:
        log.error("%sclient error: %s", logprefix, str(clierr))
        raise ImportError("error in upload to bucket:%s key:%s: %s" %
                          (bucketname, keyname, clierr.response['Error']['Code']))

def _digest_from_sum_url(digest_url, entry_name, creds=None, logprefix=""):
    """
    assumes the URL is a digest file (md5sum, sha1sum, etc).
    reads the first 5 MBs. finds the entry, returns the digest
    """
    headers, inputfd = _get(digest_url, creds=creds, logprefix=logprefix)
    text = inputfd.read(5*1024*1024).decode('utf-8')
    inputfd.close()

    for line in text.split('\n'):
        line = line.strip()
        if not line: continue
        try:
            hexdigest, name = line.split(maxsplit=1)
        except ValueError as ve:
            log.error("%s bad sumfile format: %s", logprefix, line)
            continue

        if name == entry_name:
            return hexdigest
    return None

def _handle_request_full(inurl, outurl, digests, creds=None, tmp_bucket=None, logprefix=""):
    """do the work for one url/digesturl combo"""

    logprefix=logprefix or ""
    input_digests = {}

    in_parsed = urlparse(inurl)
    basename = os.path.split(in_parsed.path)[1]
    for digest_type, digest_url in digests:
        digest_type = digest_type.strip().lower()
        log.info("%s fetching %s digest for name %s...", logprefix, digest_type, basename)
        hexdigest = _digest_from_sum_url(digest_url, basename, creds=creds, logprefix=logprefix.rstrip() + "." + digest_type + " ")
        if not hexdigest:
            raise ImportError("cannot find %s digest for file %s" % (digest_type, basename))
        log.info("%s found digest: %s %s", logprefix, digest_type, basename)
        input_digests[digest_type] = hexdigest

    out_parsed = urlparse(outurl)
    if out_parsed.scheme != "s3":
        log.error("%sbad output url %s. expected an s3:// url", logprefix, outurl)
        raise ImportError("bad output url")
    out_bucket = out_parsed.netloc

    # if outurl is a key prefix (directory), append input basename to it.
    if out_parsed.path.endswith("/"):
        outurl = os.path.join(outurl, basename)

    #
    # todo check if outurl exists and has the expected sums.
    # shortcircuit here
    #

    in_headers, in_fp = _get(inurl, creds=creds, logprefix=logprefix)
    in_ct = in_headers.get('Content-Type')
    in_cl = int(in_headers.get('Content-Length', -1), 10)


    # supported by default
    output_hashes = {'md5': True, 'sha1': True}
    for algoname in input_digests:
        # add anything that needs to be verified. carried from the input.
        output_hashes[algoname] = True

    pipe_fp = HashingReader(in_fp, algorithms=output_hashes.keys())

    tmp_bucket = tmp_bucket or out_bucket
    tmp_key = "reprod-data-import/%s" % (uuid.uuid4(),)
    tmp_url = "s3://%s/%s" % (tmp_bucket, tmp_key)
    try:
        _s3_put(pipe_fp, tmp_url, content_type=in_ct, content_length=in_cl, logprefix=logprefix)
    finally:
        pipe_fp.close()

    try:
        xfer_digests = pipe_fp.hexdigests()
        mismatches = []
        for algo, expected_digest in input_digests.items():
            if xfer_digests[algo] != expected_digest:
                log.error("%s %s digest mismatch: got %s but expected %s", logprefix, algo, xfer_digests[algo], expected_digest)
                raise ImportError("%s digest mismatch: got %s but expected %s" % (algo, xfer_digests[algo], expected_digest))
            else:
                log.debug("%s %s digest match: %s", logprefix, algo, expected_digest)

        # store digests in final location metadata
        meta = {}
        for algo, digest in xfer_digests.items():
            meta[DIGEST_HEADER + algo.lower()] = digest.strip()
        # mark which digests were verified on import
        import_checks = ",".join(input_digests.keys())
        meta[META_PREFIX + "import-digests"] = import_checks

        # copy to final location
        copy_result = _s3_copy(tmp_url, outurl, content_type=in_ct, meta=meta, logprefix=logprefix)
        return {"input": inurl,
                "output": outurl,
                "Content-Type": in_ct,
                "Content-Length": in_cl,
                "digests": pipe_fp.hexdigests()}

    finally:
        _s3_delete(tmp_url, logprefix=logprefix)

def handle_request(request, creds=None, tmp_bucket=None, logprefix=""):
    """
    returns the result of handling one request

    {"output": ..., "digests": [...]} on success
    {"error": "error message"} on error
    """
    input_url = request.get("input")
    output_url = request.get("output")
    input_digests = request.get("digests", [])

    for digest_typ, digest_url in input_digests:
        if digest_typ not in ("md5",):
            return {"error": "unrecognized digest type: %s" % (digest_typ,)}
    try:
        return _handle_request_full(input_url, output_url, input_digests, creds=creds, tmp_bucket=tmp_bucket, logprefix=logprefix)
    except ImportError as ie:
        return {"input": input_url, "output": output_url, "error": str(ie)}

def lambda_handler(event, context):
    """lambda entry point"""
    requests = event.get("requests", [])

    errors = []
    error_count = 0

    creds = {}
    creds['username'] = os.environ.get('USERNAME', '')
    creds['password'] = os.environ.get('PASSWORD', '')
    tmp_bucket = os.environ.get('TMPBUCKET', '')

    results = [ handle_request(req, creds=creds, tmp_bucket=tmp_bucket, logprefix="%03d" % i) for i, req in enumerate(requests) ]
    error_count = len([r for r in results if "error" in r])
    return {
        'error_count': error_count,
        'results': results
    }

def main_handler():
    """do the work. CLI"""

    import argparse
    import yaml

    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument("url", metavar="INURL", help="url to input file")
    parser.add_argument("outputurl", metavar="OUTPUTURL", help="target name in s3. e.g. s3://my-bucket/foo.txt")

    parser.add_argument("--md5url", metavar="MD5URL", help="url of md5sum file for the input. optional")
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

    if args.md5url:
        digests.append(("md5", args.md5url))

    request = {
        "input": args.url,
        "output": args.outputurl,
        "digests": digests
    }

    requests = [request]
    results = [ handle_request(req, creds=creds, tmp_bucket=tmp_bucket, logprefix="%03d" % i) for i, req in enumerate(requests) ]
    error_count = len([r for r in results if "error" in r])

    json.dump({"results": results, "error_count": error_count}, sys.stdout, sort_keys=True, indent=4, separators=(',', ': '))
    if error_count > 0:
        sys.exit(1)

if __name__ == "__main__":
    main_handler()
    sys.exit(0)


