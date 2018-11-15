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
#     "errors": [
#        {"input": ..., "output": ..., msg: "error message"}, ...
#     ]
# }


import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

import json
import os, sys
import requests
import logging
import threading
from urllib.parse import urlparse


import bunnies

# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3.html

META_PREFIX = "x-amz-meta-"
SHA1_HEADER = "x-amz-meta-sha1"
MD5_HEADER = "x-amz-meta-md5"

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

class ProgressPercentage(object):
    def __init__(self, size=-1, logprefix=""):
        self._size = size
        self._logprefix = logprefix
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            prog_siz = self._seen_so_far // (1024*1024)
            if self._size >= 0:
                prog_pct = self._seen_so_far / (self._size + 1) * 100
                log.info("%s  progress %6d / %d MiB (%5.2f%%)", self._logprefix,
                         prog_siz, (self._size + 512*1024) // (1024*1024), prog_pct)
            else:
                log.info("%s  progress %6d / -- MiB   --", self._logprefix, prog_siz)

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
    log.info("%sdownloading %s (username=%s)", logprefix, url, username or "")

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
                log.info("%s  download failed. bad credentials.", logprefix)
                raise ImportError("download failed")

    content_length = r.headers.get('content-length', None)
    if content_length:
        content_length = int(content_length, 10)

    log.info("%s  content-length %s", logprefix, content_length)
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

def _get(url, username="", password="", logprefix=""):
    """returns an open request handle to a file"""
    url_type = _url_type(url)
    if url_type == "s3":
        return _s3_get(url, logprefix=logprefix)
    elif url_type == "nanuq":
        return _nanuq_get(url, username=username, password=username, logprefix=logprefix)
    elif url_type == "http":
        return _http_get(url, username=username, password=password, logprefix=logprefix)
    else:
        raise ImportError("Url has unsupported backend: %s" % (url,))

def _s3_put(inputfp, outputurl, content_type=None, content_length=-1, meta=None, logprefix=""):
    """Store the input fileobject under key outputurl

    returns nothing.
    """
    o = urlparse(outputurl)
    bucketname = o.netloc
    keyname = o.path

    meta = meta or {}

    s3 = boto3.client('s3')
    config = TransferConfig(use_threads=False, max_concurrency=1)
    progress = ProgressPercentage(size=content_length, logprefix=logprefix)

    extra_args = {
        'ContentType': content_type or 'application/octet-stream',
        'Metadata': {}
    }

    # part uploads don't need length
    #if content_length >= 0:
    #    extra_args['ContentLength'] = content_length

    if meta:
        extra_args['Metadata'].update(meta)

    log.info("%sstarting upload: > bucket:%s key:%s extra:%s",
              logprefix, bucketname, keyname, extra_args)

    return s3.upload_fileobj(inputfp, bucketname, keyname,
                             Config=config,
                             ExtraArgs=extra_args,
                             Callback=progress)


def _get_digest():
    if digest_type:
        digest_type = digest_type.upper()
    if digest_type in ('MD5',):
        md5_file = _do_download(tempdir, digest_url, creds,
                                logprefix="%04d.md5 " % (serialno,))

        expected_digest = _extract_digest_entry(md5_file, basename)
        if not expected_digest:
            raise Exception("cannot obtain expected digest for entry: %s" % (basename,))

        digest_obj = hashlib.md5()

    elif digest_type:
        log.error("%04d digest type %s not supported", serialno, digest_type)
        raise Exception("cannot download content")

    log.info("%04d looking up content cache for %s:%s", serialno, digest_type, expected_digest)

    cached_path = cache.get(digest_type, expected_digest)
    if cached_path:
        log.info("%04d download skipped. already cached. (%s)", serialno, cached_path)
        return cached_path
    else:
        log.info("%04d no such content. downloading full file.", serialno)

def _do_download(dl_dir, url, creds, digest_obj=None, logprefix=""):

    raw_data = _nanuq_stream(url, creds.get('username', ''), creds.get('password', ''), logprefix=logprefix)

    download_output = os.path.join(dl_dir, urllib.parse.unquote(os.path.basename(url)))

    siz = 0
    chksiz = 256*1024
    last_update = 0
    with open(download_output, "wb") as fd:
        chunk = 'x'
        while chunk:
            chunk = raw_data.read(chksiz)
            siz += len(chunk)
            if digest_obj:
                digest_obj.update(chunk)
            fd.write(chunk)
            if siz % (5*1024*1024) < chksiz:
                now = time.time()
                if last_update + 5.0 < now:
                    last_update = now
                    prog_siz = siz // (1024*1024)
                    if content_length:
                        prog_pct = siz / (content_length + 1) * 100
                        log.info("%s  progress %6d / %d MiB (%5.2f%%)", logprefix,
                                 prog_siz, (content_length + 512*1024) // (1024*1024), prog_pct)
                    else:
                        log.info("%s  progress %6d / -- MiB   --", logprefix, prog_siz)
    log.info("%s  done. file %s written.", logprefix, download_output)

    return download_output

def _extract_digest_entry(digest_file, entry_name):
    with open(digest_file, "r") as fd:
        for line in fd:
            line = line.strip()
            if not line:
                continue
            dig, nam = line.split(maxsplit=1)
            if nam == entry_name:
                return dig
    return None

def download_content(cache, work_dir, url, digest_type, digest_url, creds, serialno):
    tempdir = tempfile.mkdtemp(prefix=".download-samples-", dir=work_dir)
    basename = os.path.basename(url)
    expected_digest = None
    digest_obj = None

    try:
        if digest_type:
            digest_type = digest_type.upper()
        if digest_type in ('MD5',):
            md5_file = _do_download(tempdir, digest_url, creds,
                                    logprefix="%04d.md5 " % (serialno,))

            expected_digest = _extract_digest_entry(md5_file, basename)
            if not expected_digest:
                raise Exception("cannot obtain expected digest for entry: %s" % (basename,))

            digest_obj = hashlib.md5()

        elif digest_type:
            log.error("%04d digest type %s not supported", serialno, digest_type)
            raise Exception("cannot download content")

        log.info("%04d looking up content cache for %s:%s", serialno, digest_type, expected_digest)

        cached_path = cache.get(digest_type, expected_digest)
        if cached_path:
            log.info("%04d download skipped. already cached. (%s)", serialno, cached_path)
            return cached_path
        else:
            log.info("%04d no such content. downloading full file.", serialno)


        # download full file
        if not digest_type:
            digest_obj = hashlib.md5()
            digest_type = "md5"

        full_file = _do_download(tempdir, url, creds, logprefix="%04d.data " % (serialno,), digest_obj=digest_obj)
        computed_digest = digest_obj.hexdigest()

        if expected_digest:
            if computed_digest != expected_digest:
                log.error("%04d download digest %s:%s does not match expected %s:%s",
                          serialno, digest_type, computed_digest, digest_type, expected_digest)
                raise Exception("download failed.")
            else:
                log.info("%04d digest %s:%s matches", serialno, digest_type, expected_digest)

        cached_path = cache.put(full_file, digest_type, computed_digest)
        return cached_path
    finally:
        shutil.rmtree(tempdir, ignore_errors=True)


def _handle_request_full(inurl, outurl, digests):
    pass

def handle_request(request):
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
        _handle_request_full(input_url, output_url, digests)
    except ImportError as ie:
        return {"error": ie.message}

def lambda_handler(event, context):
    requests = event.get("requests", [])

    errors = []
    error_count = 0

    results = [ handle_request(req) for req in requests ]
    error_count = len([r for r in results if "error" in r])
    return results


if __name__ == "__main__":
    import argparse
    import yaml

    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument("url", metavar="INURL", help="url to input file")
    parser.add_argument("--md5url", metavar="MD5URL", help="url of md5sum file for the input. optional") 
    parser.add_argument("outputurl", metavar="OUTPUTURL", help="target name in s3. e.g. s3://my-bucket/foo.txt")
    parser.add_argument("--workdir", metavar="WORKDIR", type=str, default="/tmp/",
                        help="working directory")
    parser.add_argument("--creds", metavar="CREDSFILE", type=str, default=None,
                        help="credentials file (yaml) with username and password key")

    args = parser.parse_args()

    if args.creds:
        with open(args.creds, "r") as stream:
            try:
                creds = yaml.load(stream)
                username = creds.get('username', '')
                password = creds.get('password', '')
            except yaml.YAMLError as exc:
                raise
    else:
        username = ''
        password = ''

    headers, inputfd = _get(args.url, username=username, password=password, logprefix="[main] ")
    result = _s3_put(inputfd, args.outputurl,
                     content_length=int(headers.get('content-length', -1), 10),
                     content_type=headers.get('content-type'),
                     logprefix="[main] ")

