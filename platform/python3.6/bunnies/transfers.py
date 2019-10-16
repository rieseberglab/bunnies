
import os
import time
import logging
import hashlib
import threading
import io
import concurrent.futures
import boto3
import base64
from botocore.exceptions import ClientError

from . import utils
from . import constants

log = logging.getLogger(__name__)


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

    def log_progress(self, as_of=None):
        prog_siz = self._pos // (1024*1024)
        now = as_of if as_of is not None else time.time()
        elapsed = now - self._start_time

        delta_t = now - self._last_update
        delta_x = self._pos - self._last_update_pos
        speed_mb = delta_x / (1024*1024*(delta_t+0.001))

        if self._size >= 0:
            prog_pct = self._pos / (self._size + 1) * 100
            self._logger.info("%s  progress %6d / %d MiB (%5.2f%%) %8.3fMB/s  elapsed %8.3f", self._logprefix,
                              prog_siz, (self._size + 512*1024) // (1024*1024), prog_pct, speed_mb, elapsed)
        else:
            self._logger.info("%s  progress %6d / -- MiB   --  %8.3fMB/s  elapsed %5.2f", self._logprefix, prog_siz,
                              speed_mb, elapsed)

    def __call__(self, bytes_amount):
        with self._lock:
            if self._start_time == 0: self._start_time = time.time()
            self._pos += bytes_amount
            now = time.time() if self._update_interval > 0 else 0
            if self._update_interval <= 0 or (self._last_update + self._update_interval < now):
                self.log_progress(as_of=now)
                self._last_update_pos = self._pos
                self._last_update = now


class HashingReader(object):
    """whenever data is read from this object, a corresponding amount of data
       is read from the source, and hashes are computed over this data
    """
    def __init__(self, sourcefp, algorithms=("md5", "sha1"), progress_callback=None):
        self._sourcefp = sourcefp
        self._hashers = []
        self._pos = 0
        self._progress_callback = progress_callback

        for algo in algorithms:
            self._hashers.append(getattr(hashlib, algo)())
        if len(self._hashers):
            self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=len(self._hashers))
        else:
            self._executor = None
        self._futures = []

    @property
    def progress_callback(self):
        return self._progress_callback

    @progress_callback.setter
    def progress_callback(self, progress):
        self._progress_callback = progress

    def hexdigests(self):
        """ returns the hexdigests of all data that has gone through so far.
            { algoname: hexdigest, ...}
        """
        return {o.name: o.hexdigest() for o in self._hashers}

    def __getattr__(self, attr):
        return getattr(self._sourcefp, attr)

    def read(self, size=-1):
        chunk = self._sourcefp.read(size)
        self._pos += len(chunk)

        # notify
        if self._progress_callback:
            self._progress_callback(len(chunk))

        # drain previous chunk
        if self._futures:
            for future in self._futures:
                future.result()
            self._futures = None

        # asynchronously hash
        if chunk and self._executor:
            self._futures = [self._executor.submit(obj.update, chunk) for obj in self._hashers]

        return chunk

    def close(self):
        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None
        return self._sourcefp.close()

    def seekable(self, *args):
        return False

    def seek(self, *args, **kwargs):
        raise OSError("Not seekable/truncatable")

    def tell(self, *args, **kwargs):
        return self._pos

    def tuncate(self, *args, **kwargs):
        raise OSError("Not seekable/truncatable")


def yield_in_chunks(fp, chunk_size_bytes):
    while True:
        chunk = fp.read(chunk_size_bytes)
        if not chunk:
            break
        yield chunk


def s3_streaming_put_simple(inputfp, outputurl, content_type=None, content_length=None, content_md5=None, content_encoding=None,
                            meta=None, logprefix=""):
    """
    Upload the inputfile (stream) using a single PUT operation

    you must specify content_length, and content_md5 (hexdigest)
    """
    bucketname, keyname = utils.s3_split_url(outputurl)
    if not keyname:
        log.error("%s empty key given", logprefix)
        raise ImportError("empty key given")

    if content_length < 0 or content_length is None or content_length > constants.MAX_SINGLE_UPLOAD_SIZE:
        raise ImportError("Content length must be known and between 5MiB and 5GiB")

    if content_md5 is None:
        raise ImportError("Content-MD5 must be known to do simple streaming uploads.")

    meta = meta or {}

    s3 = boto3.client('s3')

    base64_md5 = utils.hex2b64(content_md5)

    extra_args = {
        'ContentType': content_type or 'application/octet-stream',
        'ContentMD5': base64_md5,
        'ContentLength': content_length,
        'Metadata': {}
    }

    if content_encoding:
        extra_args['ContentEncoding'] = content_encoding

    if meta:
        extra_args['Metadata'].update(meta)

    log.debug("%s S3-PutObject bucket:%s key:%s extra:%s",
             logprefix, bucketname, keyname, extra_args)

    try:
        completed = s3.put_object(Bucket=bucketname, Key=keyname,
                                  Body=inputfp,
                                  **extra_args)
        log.debug("%s completed simple streaming upload bucket:%s key:%s etag:%s", logprefix, bucketname, keyname,
                  completed['ETag'])
        return completed

    except ClientError as clierr:
        log.error("%s client error: %s", logprefix, str(clierr))
        raise ImportError("error in upload to bucket:%s key:%s: %s" %
                          (bucketname, keyname, clierr.response['Error']['Code']))


def s3_upload_file(inputpath, outputurl, logprefix="", content_type=None, content_encoding=None, **extra):
    """upload the given local file to the s3 url given"""
    bucketname, keyname = utils.s3_split_url(outputurl)
    if not keyname:
        log.error("%s empty key given", logprefix)
        raise ValueError("empty key given")
    if logprefix:
        logprefix += " "

    log.info("%suploading %s => %s", logprefix, inputpath, outputurl)
    s3 = boto3.client('s3')
    # this downloads it to a temp file
    extra_args = {
        "ContentType": content_type or "application/octet-stream"
    }
    if content_encoding:
        extra_args['ContentEncoding'] = content_encoding
    extra_args.update(extra)

    s3.upload_file(inputpath, bucketname, keyname, ExtraArgs=extra_args)
    st_size = os.stat(inputpath).st_size
    log.info("%suploaded %s (%.3fMiB)", logprefix, inputpath, st_size / (1024 * 1024))
    return outputurl


def s3_download_file(inputurl, outputpath, logprefix=""):
    """download the given s3 url to the pathname given"""
    bucketname, keyname = utils.s3_split_url(inputurl)
    if not keyname:
        log.error("%s empty key given", logprefix)
        raise ValueError("empty key given")
    if logprefix:
        logprefix += " "

    log.info("%sdownloading %s => %s", logprefix, inputurl, outputpath)
    s3 = boto3.client('s3')
    # this downloads it to a temp file
    s3.download_file(bucketname, keyname, outputpath)
    st_size = os.stat(outputpath).st_size
    log.info("%sdownloaded %s (%.3fMiB)", logprefix, outputpath, st_size / (1024 * 1024))
    return outputpath


def s3_streaming_put(inputfp, outputurl, content_type=None, content_length=-1, content_encoding=None, meta=None, logprefix=""):
    """
    Upload the inputfile (fileobj) using a multipart approach.

    FIXME -- The XML Schema breaks if there are no parts (size 0)
    """
    bucketname, keyname = utils.s3_split_url(outputurl)
    if not keyname:
        log.error("%s empty key given", logprefix)
        raise ImportError("empty key given")

    meta = meta or {}

    s3 = boto3.client('s3')
    progress = ProgressPercentage(size=content_length, logprefix=logprefix, logger=log)

    extra_args = {
        'ContentType': content_type or 'application/octet-stream',
        'Metadata': {}
    }
    if content_encoding:
        extra_args['ContentEncoding'] = content_encoding

    if meta:
        extra_args['Metadata'].update(meta)

    log.debug("%s S3-PutObject multipart bucket:%s key:%s extra:%s",
             logprefix, bucketname, keyname, extra_args)

    mpart = None
    try:
        mpart = s3.create_multipart_upload(Bucket=bucketname, Key=keyname,
                                           **extra_args)
        parts = []
        partnumber = 0
        progress(0)

        chunk_iter = yield_in_chunks(inputfp, constants.UPLOAD_CHUNK_SIZE)
        chunkfp = None

        for chunk in chunk_iter:
            partnumber += 1

            chunklen = len(chunk)
            chunkdigest = hashlib.md5(chunk).digest()
            chunkfp = io.BytesIO(chunk)
            chunk = None

            base64_md5 = base64.b64encode(chunkdigest).decode('ascii')
            part_res = s3.upload_part(Body=chunkfp, Bucket=bucketname, Key=keyname,
                                      ContentLength=chunklen,
                                      ContentMD5=base64_md5,
                                      PartNumber=partnumber,
                                      UploadId=mpart['UploadId'])
            chunkfp.close()
            chunkfp = None
            parts.append((partnumber, part_res['ETag']))

            progress(chunklen)
        del chunk_iter

        # finish it
        parts_document = {
            'Parts': [{'ETag': etag, 'PartNumber': pnum} for pnum, etag in parts]
        }

        log.debug("%s completing multipart upload bucket:%s key:%s %s...", logprefix, bucketname, keyname, parts_document)
        completed = s3.complete_multipart_upload(Bucket=bucketname, Key=keyname, UploadId=mpart['UploadId'],
                                                 MultipartUpload=parts_document)
        mpart = None
        log.debug("%s completed multipart upload bucket:%s key:%s etag:%s", logprefix, bucketname, keyname, completed['ETag'])
        return completed

    except ClientError as clierr:
        log.error("%s client error: %s", logprefix, str(clierr))
        raise ImportError("error in upload to bucket:%s key:%s: %s" %
                          (bucketname, keyname, clierr.response['Error']['Code']))
    finally:
        if mpart:
            try:
                s3.abort_multipart_upload(Bucket=bucketname, Key=keyname, UploadId=mpart['UploadId'])
                log.debug("%s multipart upload aborted.", logprefix)
            except Exception as exc:
                log.error("%s could not abort multipart upload:", logprefix, exc_info=exc)
