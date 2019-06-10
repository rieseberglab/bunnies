#!/usr/bin/env python3

"""Data importer for Reproducible experiments.

   This program computes hashes on an s3 key, and moves/updates the
   metadata to include the discovered hashes.

   In the digests parameter, set the known digests for the object.
   They will be verified before committing the input key to the
   output key name. You may also set additional digest keys to null,
   if you also want them computed. The final digests will be written
   in the response, and in the final object's metadata.

{
  "src_bucket": "bar",
  "src_key":   "foo",

  "dst_bucket": "bar"      # defaults src_bucket
  "dst_key": "foo-hashed"  # defaults src_key

  ["src_etag": "adfafaaaaa...",]

  "digests": {"md5": null | "expectedhex", "digest": null | "expectedhex"}
}

The response will be:

{
  "Key": dst_key,
  "Bucket": dst_bucket,
  "ContentLength": int
  "Metadata": {"k":"v"},
  "ETag": "xxxxxxxxxxx",
  "LastModified": "date string",
  "digests": {...} # filled in version of request
}

or (in case of an error)

{
  "error": "error message"
}

"""


import boto3
import hashlib
import concurrent.futures
import time
import logging

from bunnies import transfers, constants

DIGEST_HEADER = constants.DIGEST_HEADER_PREFIX
MB = 1024*1024
DEFAULT_CHUNK_SIZE = 16*MB

def setup_logging(loglevel=logging.INFO):
    """configure custom logging for the platform"""
    root = logging.getLogger(__name__)
    root.setLevel(loglevel)

setup_logging(logging.DEBUG)
log = logging.getLogger(__name__)

def _form_response(dst_bucket, dst_key, clen, last_mod, new_etag, new_meta):
    return {
        "Key": dst_key,
        "Bucket": dst_bucket,
        "ContentLength": clen,
        "LastModified": last_mod,
        "ETag": new_etag,
        "Metadata": new_meta,
        "digests": {algo[len(DIGEST_HEADER):]: hexdigest for algo, hexdigest in new_meta.items()
                    if algo.startswith(DIGEST_HEADER)}
    }

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
    log.debug("HEAD: %s", head_res)
    orig_etag = head_res['ETag']
    orig_ct = head_res['ContentType']

    # check that object has expected etag (if possible)
    expected_etag = event.get("src_etag", None)
    if expected_etag and expected_etag != orig_etag:
        return {"error": "etag mismatch. expected %s but found %s" % (expected_etag, orig_etag)}

    # extract 'digest-ALGO' from Metadata in existing object
    # trust that they are accurate
    completed_digests = {digest_type[len(DIGEST_HEADER):]: head_res['Metadata'].get(digest_type)
                         for digest_type in head_res['Metadata']
                         if digest_type.startswith(DIGEST_HEADER)}

    for digest_type in completed_digests:
        if expected_digests[digest_type] and completed_digests[digest_type] and \
           expected_digests[digest_type] != completed_digests[digest_type]:
            return {"error": "digest mismatch %s" % (digest_type,)}

    pending = []
    for digest_type in expected_digests:
        if not completed_digests.get(digest_type, None):
            # need to recompute it
            pending.append(digest_type)

    if not pending:
        log.info("hashes %s are already available and match expected values. no-op.", ",".join(pending))
        return _form_response(dst_bucket, dst_key, head_res['ContentLength'],
                              head_res['ResponseMetadata']['last-modified'], head_res['ETag'], head_res['Metadata'])

    # GET OBJECT
    resp = client.get_object(Bucket=src_bucket, Key=src_key, IfMatch=orig_etag)

    progress = transfers.ProgressPercentage(size=resp['ContentLength'], logger=log)
    chunk_size = int(event.get("chunk_size", "0"), 10)
    if chunk_size <= 0:
        chunk_size = DEFAULT_CHUNK_SIZE

    chunk_iter = transfers.yield_in_chunks(resp['Body'], chunk_size)

    hashers = {algo: getattr(hashlib, algo)() for algo in pending}

    def _update_hash(algo, hasher, chunk):
        hasher.update(chunk)

    log.info("Hashing ~%dMB in ~%dMB chunks. Algorithms: %s",
             resp['ContentLength'] // MB,
             chunk_size // MB,
             ", ".join(pending)
    )

    futures = []
    start_time = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(pending)) as executor:
        for chunk in chunk_iter:

            # log it
            progress(len(chunk))

            for future in futures: future.result() # wait for previous batch to complete
            # submit last batch
            futures = [executor.submit(_update_hash, algo, hasher, chunk) for algo, hasher in hashers.items()]

        # wait for ultimate batch to complete
        for future in futures: future.result()

    for algo, hasher in hashers.items():
        completed_digests[algo] = hasher.hexdigest()

    log.info("computed %s hashes in %8.3f seconds.", ",".join(pending), time.time() - start_time)

    for digest_type in completed_digests:
        if expected_digests[digest_type] and completed_digests[digest_type] and \
           expected_digests[digest_type] != completed_digests[digest_type]:
            return {"error": "digest mismatch %s" % (digest_type,)}

    # prepare new metadata
    new_meta = dict(resp['Metadata'])
    new_meta.update({DIGEST_HEADER + algo: hexdigest for algo, hexdigest in completed_digests.items()})

    # copy (possibly in-place)
    start_time = time.time()
    if put_copy:
        log.info("overwriting object s3://%s/%s with metadata: %s", dst_bucket, dst_key, new_meta)
    else:
        log.info("saving object as s3://%s/%s with metadata: %s", dst_bucket, dst_key, new_meta)
    copy_result = client.copy_object(Key=dst_key, Bucket=dst_bucket,
                                     CopySource={"Key": src_key, "Bucket": src_bucket},
                                     CopySourceIfMatch=resp['ETag'],
                                     ContentType=orig_ct,
                                     Metadata=new_meta,
                                     MetadataDirective='REPLACE')
    log.info("copy completed in %8.3fseconds", time.time() - start_time)
    log.debug("copy result: %s", copy_result)
    return _form_response(dst_bucket, dst_key, resp['ContentLength'],
                          copy_result['CopyObjectResult']['LastModified'].strftime("%a, %d %b %Y %H:%M:%S %Z"),
                          copy_result['CopyObjectResult']['ETag'],
                          new_meta)

