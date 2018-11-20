#!/usr/bin/env python3

"""Data importer for Reproducible experiments.

   This program computes hashes on an s3 key, and moves/updates the
   metadata to include the discovered hashes.

"""


import boto3
import hashlib
import helpers

META_PREFIX = "" # in boto you don't give x-amz-meta-
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
    root.propagate = False

setup_logging(logging.DEBUG)
log = logging.getLogger(__name__)


def lambda_handler(event, context):
    """lambda entry point"""
    src_key = event.get("src_key", "")
    src_bucket = event.get("src_bucket", "")
    
    # copy to this name if the src hashes match
    # this defaults to the src key
    dst_key = event.get("dst_key", src_key)
    dst_bucket = event.get("dst_bucket", src_bucket)

    expected_digests = event.get("digests", {"md5":None, "sha1":None})

    if src_key == dst_key and src_bucket == dst_bucket:
        put_copy = True
    else:
        put_copy = False

    client = boto3.client("s3")
    
    head_res = client.head_object(Bucket=src_bucket, Key=src_key)
    orig_etag = head_res['ETag']
    
    completed_digests = {digest_type: head_res['Metadata'].get(digest_type) 
                         for digest_type in head_res['Metadata'] 
                         if digest_type.startswith(DIGEST_HEADER)}

    for digest_type in completed_digests:
        if expected_digests[digest_type] and completed_digests[digest_type] and \
           expected_digests[digest_type] != completed_digests[digest_type]:
            return {"error": "digest mismatch %s" % (digest_type,)}
    
    pending = []
    for digest_type in expected_digests:
        if not completed_digests[digest_type]:
            pending.append(digest_type)

    if not pending:
        return completed_digests

    resp = client.get_object(Bucket=src_bucket, Key=src_key, IfMatch=orig_etag)
    progress = helpers.ProgressPercentage(size=resp['ContentLength'],
    hashing_reader = helpers.HashingReader(resp['Body'], algorithms=pending)
    class ProgressPercentage(object):
    def __init__(self, size=-1, logprefix="", min_interval_s=5.0, logger=log):
        self._size = size
        self._logprefix = logprefix
        self._pos = 0
        self._lock = threading.Lock()
        self._last_update = 0
        self._last_update_pos = 0
        self._update_interval = min_interval_s
        self._logger = logger
        self._start_time = 0

    return {
        
    }
