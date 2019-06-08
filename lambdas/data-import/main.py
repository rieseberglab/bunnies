#!/usr/bin/env python3

"""
   Lambda data importer

   This program imports data from the web (as a URL) into S3.
   It was designed to work against Nanuq file servers, but
   it should also cover most other HTTP urls.

   Input:

   event = {
      "requests": [
          {
              "input": "http://...",
              "digests": [["md5", "http://..."]],
              "output": "s3://...",
              "tmp_bucket": None,
              "download_only": False
          }
      ]
   }

   Multiple files can be imported at once, as long
   all transfers can be completed within the lambda
   time limit. (900 seconds)

   input: HTTP url to import
   digests: list of [type, url] tuples to sumfiles
   output: the final location for the uploaded file
   download_only: when True, the final move is skipped. and details about the
                  temporary files are returned. This allows another step to
                  inspect the file before committing to the final location.
                  (see rehash.py)
   tmp_bucket: a bucket to place the temporary upload in. the default is to
               use a temporary key prefix in the output bucket.

   environment:

      USERNAME  := nanuq username
      PASSWORD  := nanuq password
      TMP_BUCKET := name of a default temporary bucket to store uploads


   output = {
     "error_count": int,
     "results": [
         {"input": inputurl, "output": finaloutputurl, "digests": {"md5": ..., "sha1": ...}}, # ok result
         {"input": inputurl, "output": outputurl, "error": "error message"}, ...
     ]
   }

   results are returned in the same order as the requests
   when download_only = True is given in a request, the temporary location is
                        also included in the response:
   {
     "input": inurl, # input url from request
     "output": tmp_url, # the temporary blob location
     "move_to": outurl,  # where it should be moved if it validates
     "ETag": the temporary file etag,
     "Content-Type": content type,
     "Content-Length": content length,
     "digests": pipe_fp.hexdigests()
   }
"""


import boto3
import botocore
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

import json
import os
import sys
import requests
import logging
import uuid
import time
from urllib.parse import urlparse

import bunnies
from bunnies import utils
from bunnies import transfers

DEFAULT_OUTPUT_HASHES = {} # {"md5": True, "sha1": True}

DIGEST_HEADER = bunnies.constants.DIGEST_HEADER_PREFIX  # + algoname
IMPORT_DIGESTS_HEADER = bunnies.constants.IMPORT_DIGESTS_HEADER


def setup_logging(loglevel=logging.INFO):
    """configure custom logging for the platform"""

    root = logging.getLogger(__name__)
    root.setLevel(loglevel)
    ch = logging.StreamHandler(sys.stderr)
    ch.setLevel(loglevel)
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)
    root.propagate = False


setup_logging(logging.DEBUG)
log = logging.getLogger(__name__)

log.info("Running boto3:%s botocore:%s", boto3.__version__, botocore.__version__)


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
        log.error("%sdownload error", logprefix, exc_info=clierr)
        raise ImportError("Error for URL %s: %s" % (url, clierr.response['Error']['Code']))


def _http_get(url, logprefix="", **kwargs):
    """http or https GET"""
    log.info("%s downloading %s", logprefix, url)

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


def _s3_delete(objecturl, logprefix=""):
    bucketname, keyname = utils.s3_split_url(objecturl)
    s3 = boto3.client('s3')
    log.info("%s S3-DELETE bucket:%s key:%s", logprefix, bucketname, keyname)
    return s3.delete_object(Bucket=bucketname, Key=keyname)


def _s3_head(objecturl, logprefix=""):
    bucketname, keyname = utils.s3_split_url(objecturl)
    s3 = boto3.client('s3')
    log.info("%s S3-HEAD bucket:%s key:%s", logprefix, bucketname, keyname)
    return s3.head_object(Bucket=bucketname, Key=keyname)


def _s3_copy(srcurl, dsturl, content_type=None, meta=None, etag_match=None, logprefix=""):
    src_bucketname, src_keyname = utils.s3_split_url(srcurl)
    dst_bucketname, dst_keyname = utils.s3_split_url(dsturl)

    s3 = boto3.client('s3')
    log.info("%s S3-COPY bucket:%s key:%s => bucket:%s key:%s", logprefix,
             src_bucketname, src_keyname,
             dst_bucketname, dst_keyname)
    log.info("%s   content-type:%s meta:%s", logprefix, content_type, meta)

    xtra = {}
    if content_type:
        xtra['ContentType'] = content_type
    if etag_match:
        xtra['CopySourceIfMatch'] = etag_match
    if meta:
        xtra['Metadata'] = meta
        xtra['MetadataDirective'] = 'REPLACE'

    result = s3.copy_object(CopySource={'Bucket': src_bucketname, 'Key': src_keyname},
                            Bucket=dst_bucketname, Key=dst_keyname, **xtra)
    log.debug("%s S3-COPY complete: bucket:%s key:%s etag:%s", logprefix, dst_bucketname, dst_keyname,
              result['CopyObjectResult']['ETag'])
    return result


def _s3_put(inputfp, outputurl, content_type=None, content_length=-1, meta=None, logprefix=""):
    """Store the input fileobject under key outputurl

    returns nothing.
    """
    bucketname, keyname = utils.s3_split_url(outputurl)
    if not keyname:
        log.error("%s empty key given", logprefix)
        raise ImportError("empty key given")

    meta = meta or {}

    s3 = boto3.client('s3')
    config = TransferConfig(use_threads=False, max_concurrency=1)
    progress = transfers.ProgressPercentage(size=content_length, logprefix=logprefix, logger=log)

    extra_args = {
        'ContentType': content_type or 'application/octet-stream',
        'Metadata': {}
    }

    # part uploads don't need length
    #if content_length >= 0:
    #    extra_args['ContentLength'] = content_length

    if meta:
        extra_args['Metadata'].update(meta)

    log.info("%s S3-Put (transfermanager) bucket:%s key:%s extra:%s",
              logprefix, bucketname, keyname, extra_args)

    try:
        s3.upload_fileobj(inputfp, bucketname, keyname,
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

    Other supported syntaxes:
    hash://sha256/9f86d081884c7d659a2feaa0?name=foo.zip#dummy   (Hash scheme: Host is the type. First path component is digest.)
    hash://md5/9f86d081884c7d659a/foo.zip
    md5:9f86d081884c7d659a
    """
    parsed = urlparse(digest_url)
    if parsed.scheme == "hash":
        # hash://md5/asdsadada
        return parsed.path[1:].lower()
    elif parsed.scheme in ('md5', 'sha1', 'sha256'):
        # md5:babababababababa
        return parsed.path

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


def _get_source_digest(digest_url, digest_type, entry_name, creds=None, logprefix=""):
    log.info("%s fetching %s digest for name %s...", logprefix, digest_type, entry_name)

    hexdigest = _digest_from_sum_url(digest_url, entry_name, creds=creds,
                                     logprefix=logprefix.rstrip() + "." + digest_type + " ")
    if not hexdigest:
        raise ImportError("cannot find %s digest for file %s" % (digest_type, entry_name))
    log.info("%s found digest: %s %s %s", logprefix, digest_type, entry_name, hexdigest)
    return hexdigest


def _match_existing(s3url, expected_digests, expected_len, expected_ct, logprefix=""):
    """
    Retrieve the information from the target object in S3 (if any). If present,
    ensure it matches what is expected.
    """
    head_res = _s3_head(s3url, logprefix=logprefix)

    if expected_len >= 0 and expected_len != head_res['ContentLength']:
        raise ImportError("destination exists. length mismatch: destination has %s, but expected %s" % (
            head_res['ContentLength'], expected_len))

    if expected_ct and expected_ct != head_res['ContentType']:
        raise ImportError("destination exists. content type mismatch: destination has %s, but expected %s" % (
            head_res['ContentType'], expected_ct))

    digest_verifications = 0
    head_digests = {key[len(DIGEST_HEADER):]: val for key, val in head_res['Metadata'].items()
                    if key.startswith(DIGEST_HEADER)}
    for dtype, dhex in expected_digests.items():
        if dhex:
            if dhex != head_digests[dtype]:
                raise ImportError("destination exists. %s digest mismatch: destination has %s, but expected %s" % (
                    dtype, head_digests[dtype], dhex))
            else:
                digest_verifications += 1

    if digest_verifications == 0:
        raise ImportError("destination exists. no methods of identification are provided.")

    return {"Content-Type": head_res['ContentType'],
            "Content-Length": head_res['ContentLength'],
            "digests": head_digests}


def _handle_request_full(req, creds=None, logprefix=""):
    """do the work for one url/digesturl combo"""

    logprefix=logprefix or ""
    input_digests = {}

    inurl = req["input"]
    outurl = req["output"]
    tmp_bucket = os.environ.get('TMPBUCKET', None)
    tmp_bucket = req.get("tmp_bucket", tmp_bucket)

    out_parsed = urlparse(outurl)
    if out_parsed.scheme != "s3":
        log.error("%sbad output url %s. expected an s3:// url", logprefix, outurl)
        raise ImportError("bad output url")

    in_parsed = urlparse(inurl)
    for digest_type, digest_url in req.setdefault('digests', []):
        digest_type = digest_type.strip().lower()
        input_digests[digest_type] = _get_source_digest(digest_url, digest_type, os.path.split(in_parsed.path)[1],
                                                        creds=creds, logprefix=logprefix.rstrip() + "." + digest_type)

    # if outurl is a key prefix (directory), append input basename to it.
    out_bucket = out_parsed.netloc
    if out_parsed.path.endswith("/") or out_parsed.path == "":
        outurl = os.path.join(outurl, os.path.split(in_parsed.path)[1])

    in_headers, in_fp = _get(inurl, creds=creds, logprefix=logprefix)
    in_ct = in_headers.get('Content-Type')
    in_cl = int(in_headers.get('Content-Length', -1), 10)

    try:
        match = _match_existing(outurl, input_digests, in_cl, in_ct, logprefix=logprefix)
        match['input'] = inurl
        match['output'] = outurl
        log.info("%s A file already exists at the destination and matches the input description. Done.",
                 logprefix)
        return match
    except ClientError as clierr:
        code = clierr.response['Error']['Code']
        if code == "404":
            pass
        else:
            log.error("%s error checking for existing file: %s", logprefix, clierr.response['Error'],
                      exc_info=clierr)
            raise ImportError("Error for URL %s: %s" % (inurl, clierr.response['Error']))

    # supported by default
    output_hashes = dict(DEFAULT_OUTPUT_HASHES)
    for algoname in input_digests:
        # add anything that needs to be verified. carried from the input.
        output_hashes[algoname] = True

    log.info("%s Request inurl:%s outurl:%s", logprefix, inurl, outurl)
    log.info("%s Computing %s hash(es) while streaming.", logprefix, ",".join([k for k in output_hashes]))

    tmp_bucket = tmp_bucket or out_bucket
    tmp_key = "reprod-data-import/%s" % (uuid.uuid4(),)
    tmp_url = "s3://%s/%s" % (tmp_bucket, tmp_key)
    try:
        # mark which digests will be verified on import
        import_checks = ",".join(input_digests.keys())

        start_time = time.time()
        pipe_fp = transfers.HashingReader(in_fp, algorithms=output_hashes.keys())

        transfers.s3_streaming_put(pipe_fp, tmp_url, content_type=in_ct, content_length=in_cl,
                                   meta={IMPORT_DIGESTS_HEADER: import_checks}, logprefix=logprefix)

        # # from file
        # pipe_fp = transfers.HashingReader(in_fp, algorithms=())#output_hashes.keys())
        # _s3_put(pipe_fp, tmp_url, content_type=in_ct, content_length=in_cl, logprefix=logprefix)

        delta_t = time.time() - start_time
        xfer_cl = pipe_fp.tell()
        log.info("%s PUT completed in %8.3f seconds. (%8.3f MB/s)", logprefix,
                 delta_t, xfer_cl / (1024*1024*(delta_t+0.00001)))
    finally:
        if pipe_fp: pipe_fp.close()

    try:
        # check length
        if in_cl >= 0:
            if in_cl != xfer_cl:
                log.error("%s length mismatch: downloaded %s bytes but expected %s", logprefix, xfer_cl, in_cl)
                raise ImportError("length mismatch: downloaded %s bytes but expected %s" % (xfer_cl, in_cl))
            log.info("%s download length match OK: %s", logprefix, xfer_cl)

        tmp_head = _s3_head(tmp_url, logprefix=logprefix)
        log.debug("%s head result %s %s", logprefix, tmp_url, tmp_head)
        if tmp_head["ContentLength"] != xfer_cl:
            log.error("%s length mismatch: uploaded %s bytes but expected %s", logprefix, tmp_head['ContentLength'], xfer_cl)
            raise ImportError("length mismatch: uploaded %s but expected %s" % (tmp_head['ContentLength'], xfer_cl))
        else:
            log.info("%s upload length match OK: %s", logprefix, xfer_cl)

        # check digests
        xfer_digests = pipe_fp.hexdigests()
        for algo, expected_digest in input_digests.items():
            if xfer_digests[algo] != expected_digest:
                log.error("%s %s digest mismatch: got %s but expected %s", logprefix, algo, xfer_digests[algo], expected_digest)
                raise ImportError("%s digest mismatch: got %s but expected %s" % (algo, xfer_digests[algo], expected_digest))
            else:
                log.info("%s %s digest match OK: %s", logprefix, algo, expected_digest)
    except Exception:
        _s3_delete(tmp_url, logprefix=logprefix)
        raise

    if req.get('download_only', True):
        log.info("%s download_only is set. skipping copy to final location. returning details.", logprefix)
        return {
            "input": inurl,
            "output": tmp_url,
            "move_to": outurl,
            "ETag": tmp_head['ETag'],
            "Content-Type": in_ct,
            "Content-Length": xfer_cl,
            "digests": pipe_fp.hexdigests()
        }
    try:
        # store digests in final location metadata
        meta = {}
        for algo, digest in xfer_digests.items():
            meta[DIGEST_HEADER + algo.lower()] = digest.strip()
        meta[IMPORT_DIGESTS_HEADER] = import_checks

        # copy to final location
        copy_res = _s3_copy(tmp_url, outurl, content_type=in_ct, meta=meta, etag_match=tmp_head['ETag'], logprefix=logprefix)
        return {
            "input": inurl,
            "output": outurl,
            "Content-Type": in_ct,
            "Content-Length": xfer_cl,
            "ETag":  copy_res['CopyObjectResult']['ETag'],
            "digests": pipe_fp.hexdigests()
        }

    finally:
        _s3_delete(tmp_url, logprefix=logprefix)


def handle_request(request, creds=None, logprefix=""):
    """
    returns the result of handling one request

    {"output": ..., "digests": [...]} on success
    {"error": "error message"} on error
    """
    input_url = request.get("input")
    output_url = request.get("output")
    input_digests = request.setdefault("digests", [])

    log.info("%s [REQ] input: %s", logprefix, input_url)
    for digest_type, digest_url in input_digests:
        log.info("%s [REQ] input digest %s: %s", logprefix, digest_type, digest_url)
    log.info("%s [REQ] output: %s", logprefix, output_url)
    log.info("%s [REQ] chunk_size: %s", logprefix, bunnies.constants.UPLOAD_CHUNK_SIZE)

    for digest_typ, _ in input_digests:
        if digest_typ not in ("md5",):
            return {"error": "unrecognized digest type: %s" % (digest_typ,)}
    try:
        return _handle_request_full(request, creds=creds, logprefix=logprefix)
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

    results = [ handle_request(req, creds=creds, logprefix="%03d" % i) for i, req in enumerate(requests) ]
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
        "tmp_bucket": tmp_bucket,
        "input": args.url,
        "output": args.outputurl,
        "digests": digests
    }

    requests = [request]
    results = [ handle_request(req, creds=creds, logprefix="%03d" % i) for i, req in enumerate(requests) ]
    error_count = len([r for r in results if "error" in r])

    json.dump({"results": results, "error_count": error_count}, sys.stdout, sort_keys=True, indent=4, separators=(',', ': '))
    if error_count > 0:
        sys.exit(1)

if __name__ == "__main__":
    main_handler()
    sys.exit(0)


