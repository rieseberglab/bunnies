
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
from .exc import ImportError

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


def s3_copy_object(src_url, dst_url, client=None, logprefix="", threads=1, **kwargs):
    """copies the source blob to the destination. If the object is small this is
       a simple operation. If the object is > 5GB this performs a multipart upload
       copy.

       res = {
    'CopyObjectResult': {
        'ETag': 'string',
        'LastModified': datetime(2015, 1, 1)
    },
    'Expiration': 'string',
    'CopySourceVersionId': 'string',
    'VersionId': 'string',
    'ServerSideEncryption': 'AES256'|'aws:kms',
    'SSECustomerAlgorithm': 'string',
    'SSECustomerKeyMD5': 'string',
    'SSEKMSKeyId': 'string',
    'SSEKMSEncryptionContext': 'string',
    'RequestCharged': 'requester'
    }

    """
    if not client:
        s3 = boto3.client('s3')
    else:
        s3 = client

    # we need to know the size of the source url to determine the copy method
    src_bucket, src_key = utils.s3_split_url(src_url)
    dst_bucket, dst_key = utils.s3_split_url(dst_url)

    request_payer = kwargs.get("RequestPayer", None)
    meta_kwargs = {}
    if request_payer:
        meta_kwargs["RequestPayer"] = request_payer

    src_meta = utils.get_blob_meta(src_url, **meta_kwargs)
    src_etag = src_meta['ETag']
    src_size = src_meta['ContentLength']

    if kwargs.get('MetadataDirective', 'X') != 'COPY':
        new_meta = src_meta['Metadata']
        new_meta['SrcLastModified'] = str(src_meta['LastModified'].timestamp())
    else:
        new_meta = kwargs.pop('Metadata', {})

    if src_size <= 512 * 1024 * 1024:  # AWS limit for putcopy is 5GB. simple puts are slow.
        copy_args = {}
        copy_args.update(kwargs)
        copy_args.update({
            'Bucket': dst_bucket,
            'Key': dst_key,
            'CopySource': {'Bucket': src_bucket, 'Key': src_key},
            'Metadata': new_meta
        })
        if 'ContentType' in src_meta:
            copy_args['ContentType'] = src_meta['ContentType']
        else:
            copy_args['ContentType'] = 'application/octet-stream'
        if 'ContentEncoding' in src_meta:
            copy_args['ContentEncoding'] = src_meta['ContentEncoding']

        log.debug("%s copying %dB blob s3://%s/%s to s3://%s/%s", logprefix,
                  src_size, src_bucket, src_key, dst_bucket, dst_key)
        return s3.copy_object(**copy_args)

    #
    # multipart upload_part_copy. ugh
    #
    mpart = None
    try:
        create_args = {}
        create_args.update(kwargs)
        part_args = {}

        valid_create_kw = ["ACL", "Bucket", "CacheControl", "ContentDisposition", "ContentEncoding", "ContentLanguage",
                           "ContentType", "Expires", "GrantFullControl", "GrantRead", "GrantReadACP", "GrantWriteACP",
                           "Key", "Metadata", "ServerSideEncryption", "StorageClass", "WebsiteRedirectLocation",
                           "SSECustomerAlgorithm", "SSECustomerKey", "SSECustomerKeyMD5", "SSEKMSKeyId", "SSEKMSEncryptionContext",
                           "RequestPayer", "Tagging", "ObjectLockMode", "ObjectLockRetainUntilDate", "ObjectLockLegalHoldStatus"]
        for k in kwargs:
            if k.startswith("CopySource"):
                part_args[k] = kwargs[k]
                del create_args[k]
            elif k in ('RequestPayer'):
                part_args[k] = kwargs[k]
            elif k not in valid_create_kw:
                log.warn("%s ignoring copy request keyword %s key:%s bucket:%s", logprefix,
                         k, dst_bucket, dst_key)
                del create_args[k]

        create_args.update({
            'Bucket': dst_bucket,
            'Key': dst_key,
            'Metadata': new_meta
        })
        if 'ContentType' in src_meta:
            create_args['ContentType'] = src_meta['ContentType']
        else:
            create_args['ContentType'] = 'application/octet-stream'
        if 'ContentEncoding' in src_meta:
            create_args['ContentEncoding'] = src_meta['ContentEncoding']

        mpart = s3.create_multipart_upload(**create_args)

        def _gen_parts(size):
            part_size = 128 * 1024 * 1024  # 128MB
            num_parts = (size + (part_size - 1)) // part_size
            for i in range(0, num_parts):
                start = i * part_size
                end = start + part_size - 1
                if end >= size:
                    end = size - 1

                yield (i+1, num_parts, size, {
                    'PartNumber': i+1, # must be a natural integer
                    'CopySourceRange': "bytes=%d-%d" % (start, end)
                })

        parts = []
        part_args.update({
            "Key": dst_key,
            "Bucket": dst_bucket,
            "UploadId": mpart['UploadId'],
            "CopySource": {"Bucket": src_bucket, "Key": src_key},
        })
        if "CopySourceIfMatch" not in part_args and 'CopySourceIfNoneMatch' not in part_args:
            part_args["CopySourceIfMatch"] = src_etag

        def _upload_part_copy(call_args, totalparts, totalsize):
            part_res = s3.upload_part_copy(**call_args)
            return (call_args["PartNumber"], part_res['CopyPartResult']['ETag'])

        if threads < 2:
            # upload_part_copy foreach chunk
            for partnum, totalparts, totalsize, chunk_params in _gen_parts(src_size):
                call_args = dict(part_args)
                call_args.update(chunk_params)
                parts.append(_upload_part_copy(call_args, totalparts, totalsize))
        else:
            upload_errors = []
            future_to_partnum = {}
            completed = 0
            with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
                for i, (partnum, totalparts, totalsize, chunk_params) in enumerate(_gen_parts(src_size)):
                    call_args = dict(part_args)
                    call_args.update(chunk_params)
                    parts.append(None)
                    future = executor.submit(_upload_part_copy, call_args, totalparts, totalsize)
                    future_to_partnum[future] = (i, call_args)

                for future in concurrent.futures.as_completed(future_to_partnum):
                    (icall, call_args) = future_to_partnum[future]
                    try:
                        upload_res = future.result()
                    except ClientError as clierr:
                        upload_errors.append(clierr)
                        if clierr.response['Error']['Code'] == "PreconditionFailed":
                            for future in future_to_partnum:
                                future.cancel()
                    except concurrent.futures.CancelledError:
                        pass

                    except Exception as exc:
                        log.error("%s part number %d generated an error: %s", logprefix,
                                  call_args['PartNumber'], str(exc))
                        upload_errors.append(exc)
                        for future in future_to_partnum:
                            future.cancel()
                    else:
                        parts[icall] = upload_res
                        completed += 1
                        log.info("%s %.2f%% / %dB copied. Part #%d/%d -- %s completed.",
                                 logprefix, completed*100.0/totalparts, src_size,
                                 call_args['PartNumber'], totalparts, call_args['CopySourceRange']
                        )
            if upload_errors:
                raise upload_errors[0]

        parts_document = {
            'Parts': [{'ETag': etag, 'PartNumber': pnum} for pnum, etag in parts]
        }

        log.debug("%s completing multipart copy to bucket:%s key:%s", logprefix,
                  dst_bucket, dst_key)
        completed = s3.complete_multipart_upload(Bucket=dst_bucket, Key=dst_key,
                                                 UploadId=mpart['UploadId'],
                                                 MultipartUpload=parts_document)
        mpart = None
        log.debug("%s completed multipart copy to bucket:%s key:%s etag:%s size:%s", logprefix,
                  dst_bucket, dst_key, completed['ETag'], src_size)
        dst_meta = utils.get_blob_meta(dst_url, **meta_kwargs)

        # make the copy result uniform with the simple copy
        completed.update({
            'CopyObjectResult': {
                'ETag': dst_meta['ETag'],
                'LastModified': dst_meta['LastModified']
            },
        })
        assert src_size == dst_meta['ContentLength']

        # log.debug("%s multipart copy bucket:%s key:%s result:%s",
        #           logprefix, dst_bucket, dst_key, completed)
        return completed

    except ClientError as clierr:
        # no need to copy
        if clierr.response['Error']['Code'] == "PreconditionFailed":
            raise

        log.error("%s client error: %s", logprefix, str(clierr))
        raise ImportError("error in upload copy to bucket:%s key:%s: %s" %
                          (dst_bucket, dst_key, clierr.response['Error']['Code']))
    finally:
        if mpart:
            try:
                s3.abort_multipart_upload(Bucket=dst_bucket, Key=dst_key, UploadId=mpart['UploadId'])
                log.debug("%s multipart upload aborted bucket:%s key:%s uploadid:%s", logprefix,
                          dst_bucket, dst_key, mpart['UploadId'])
            except Exception as exc:
                log.error("%s could not abort multipart upload:", logprefix, exc_info=exc)


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
