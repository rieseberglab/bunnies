"""
   Utilities for importing data that will be used by
   a bunnies pipeline.
"""
import logging
import boto3
import bunnies

from urllib.parse import urlparse
from .exc import LambdaException

logger = logging.getLogger(__package__)


def _import_remote_file(src_url, dst_url, digest_urls=None):

    digest_list = [[k, v] for k, v in digest_urls.items()]

    req = {
        'download_only': True,  # skip the tail end copy
        'input': src_url,
        'output': dst_url,
        'digests': digest_list
    }
    logger.info("data-import request: %s", json.dumps(req, sort_keys=True, indent=4, separators=(",", ": ")))
    code, response = lambdas.invoke_sync(lambdas.DATA_IMPORT, Payload=req)

    data = response['Payload'].read().decode("ascii")
    if code != 0:
        raise LambdaException("data-import failed to complete: %s" % (data,))
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



def s3_import(src_url, dst_url, digest_urls=None):
    """import a file, designated by `src_url` into a managed
       bunnies data blob designated by `dst_url`

       src_url:
          file://foo.txt              local file (relative)
          file:///etc/foo.txt         local file (abs)
          http://example.org/foo.txt  remote file
          s3://example-bucket/foo.txt remote s3 file

       digest_urls:
          { 'md5': 'http://example.org/path/to/md5sum',
            'sha1': 'hash://sha1/da39a3ee5e6b4b0d3255bfef95601890afd80709'
          }

       dst_url is an s3 url: s3://my-bucket-example/file.foo .
       if dst_url ends with /, or is just a bucket name, it dst_url
       will keep the same basename as src_url

       the destination will be created only if the digests match.
    """
    src_parsed = urlparse(src_url)
    dst_parsed = urlparse(dst_url)
    if dst_parsed.scheme != "s3":
        raise ValueError("destination must be on s3")
    if src.scheme in ("http", "https", "ftp"):
        
