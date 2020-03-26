"""
   Utilities for importing data that will be used by
   a bunnies pipeline.
"""
import os.path
import logging
import json
import time
import boto3
import botocore
from botocore.exceptions import ClientError
import uuid
import bunnies

from .exc import ImportError
from . import lambdas
from . import utils
from . import transfers
from . import constants

from .exc import BunniesException, NoSuchFile

from urllib.parse import urlparse
log = logging.getLogger(__package__)

def guess_type(fil):
    if not guess_type._cache:
        import mimetypes
        mimetypes.init()
        guess_type._cache = mimetypes.guess_type
    res = guess_type._cache(fil)
    ct, ce = res[0], res[1]
    if ct is None:
        ct = "application/octet-stream"
    return (ct, ce)

guess_type._cache = None

def guess_length(fp):
    try:
        pos = fp.tell()
    except IOError:
        return -1

    try:
        if not fp.seekable():
            return -1
    except AttributeError:
        pass

    try:
        fp.seek(0, 2)
    except IOError as ioe:
        log.error("could not seek to end of file. cannot infer length.", exc_info=ioe)
        return -1

    try:
        return fp.tell()
    finally:
        try:
            fp.seek(pos, 0)
        except Exception:
            log.error("could not reset file position in %s.", fp)

class DataImport(object):

    """Methods to import data from various sources into AWS (S3)"""

    def __init__(self):
        self.num_imports = 0

    # TODO The point of making this into a class is to attribute
    #      a context to the data import operations. This is important
    #      esp for billing -- or attributing a cost value to certain
    #      datasets.

    def __delete(self, objecturl, logprefix=""):
        logpad = logprefix + " " if logprefix else ""
        session = boto3.session.Session()
        s3 = session.client('s3', config=botocore.config.Config(read_timeout=300, retries={'max_attempts': 0}))
        bucketname, keyname = utils.s3_split_url(objecturl)
        s3 = boto3.client('s3')
        log.info("%sS3-DELETE bucket:%s key:%s", logpad, bucketname, keyname)
        return s3.delete_object(Bucket=bucketname, Key=keyname)

    def __match_existing(self, s3url, expected_digests, expected_len=-1, expected_ct=None, expected_ce=None, logprefix=""):
        """
        Retrieve the information from the target object in S3 (if any). If present,
        ensure it matches what is expected.
        """
        head_res = utils.get_blob_meta(s3url, logprefix=logprefix)
        log.info("RES %s", head_res)
        if expected_len >= 0 and expected_len != head_res['ContentLength']:
            raise ImportError("destination exists. length mismatch: destination has %s, but expected %s" % (
                head_res['ContentLength'], expected_len))

        if expected_ct and expected_ct != head_res.get('ContentType'):
            raise ImportError("destination %s exists. content type mismatch: destination has %s, but expected %s" % (
                s3url, head_res.get('ContentType'), expected_ct))

        if expected_ce and expected_ce != head_res.get('ContentEncoding'):
            raise ImportError("destination %s exists. content encoding mismatch: destination has %s, but expected %s" % (
                s3url, head_res.get('ContentEncoding'), expected_ce))

        digest_verifications = 0
        head_digests = {key[len(constants.DIGEST_HEADER_PREFIX):]: val for key, val in head_res['Metadata'].items()
                        if key.startswith(constants.DIGEST_HEADER_PREFIX)}
        for dtype, dhex in expected_digests.items():
            if dhex:
                if dhex != head_digests[dtype]:
                    raise ImportError("destination %s exists. %s digest mismatch: destination has %s, but expected %s" % (
                        s3url, dtype, head_digests[dtype], dhex))
                else:
                    digest_verifications += 1

        if digest_verifications == 0:
            raise ImportError("destination %s exists, but no methods of identification are provided.", s3url)

        return head_res

    def __rehash_copy(self, src_url, dst_url, src_etag, match_digests=None, logprefix=""):
        """use the rehash lambda to copy the source file to the destination, if and only if
           etag and content digests match the ones provided

           match_digests is a dict of expected digests:
                  <algo>: <hexdigest>

        """
        logpad = logprefix + " " if logprefix else ""
        tmp_src = utils.s3_split_url(src_url)
        cpy_dst = utils.s3_split_url(dst_url)

        new_req = {
            "src_bucket": tmp_src[0],
            "src_key": tmp_src[1],
            "dst_bucket": cpy_dst[0],
            "dst_key": cpy_dst[1],
            "src_etag": src_etag,
            "digests": dict(match_digests)
        }

        # compute at least sha1 and md5
        new_req["digests"].setdefault("sha1", None)
        new_req["digests"].setdefault("md5", None)

        log.debug("%sdata-rehash request: %s", logpad, json.dumps(new_req, sort_keys=True, indent=4, separators=(",", ": ")))
        code, response = lambdas.invoke_sync(lambdas.DATA_REHASH, Payload=new_req)
        data = response['Payload'].read().decode("ascii")
        if code != 0:
            raise ImportError("data-rehash failed to complete: %s" % (data,))
        data_obj = json.loads(data)
        if data_obj.get('error', None):
            raise ImportError("data-rehash returned an error: %s" % (data["results"][0],))

        log.debug("%sdata-rehash response: %s", logpad, json.dumps(data_obj, sort_keys=True, indent=4,
                                                                   separators=(",", ": ")))

        return data_obj

    def __upload_s3_file(self, src_url, dst_url, digest_urls=None, logprefix=""):
        logpad = logprefix + " " if logprefix else ""
        digest_urls = digest_urls or {}
        expected_digests = utils.parse_digests([v for v in digest_urls.values()])

        src_head = utils.get_blob_meta(src_url, logprefix=logprefix)
        src_digests = {key[len(constants.DIGEST_HEADER_PREFIX):]: val for key, val in src_head['Metadata'].items()
                       if key.startswith(constants.DIGEST_HEADER_PREFIX)}
        for algo, hexdigest in expected_digests:
            if src_digests[algo] != expected_digests[algo]:
                raise ImportError("source %s %s digest mismatch: expected %s but source has %s",
                                  src_url, algo, expected_digests[algo], src_digests[algo])

        # supported in rehash
        dst_digests = {k: v for k, v in src_digests.items()
                       if k in ('md5', 'sha1', 'sha256')}
        dst_digests.update(src_digests)

        try:
            match = self.__match_existing(dst_url, src_digests,
                                          expected_len=src_head['ContentLength'],
                                          expected_ct=src_head['ContentType'],
                                          expected_ce=src_head['ContentEncoding'],
                                          logprefix=logprefix)
            log.info("%sfile matches existing file %s. done.", logpad, dst_url)
            return match
        except ClientError as clierr:
            code = clierr.response['Error']['Code']
            if code == "404":
                pass
            else:
                log.error("%serror checking for existing file: %s", logpad, clierr.response['Error'],
                          exc_info=clierr)
                raise ImportError("Error for URL %s: %s" % (dst_url, clierr.response['Error']))
        except NoSuchFile as clierr:
            pass

        return self.__rehash_copy(src_url, dst_url, src_head['ETag'], match_digests=dst_digests, logprefix=logprefix)

    def __upload_local_file(self, src_url, dst_url, digest_urls=None,
                            content_type=None, content_encoding=None, content_length=-1, logprefix=""):

        logpad = logprefix + " " if logprefix else ""
        # if dst_url is a key prefix (directory), append input basename to it.
        out_parsed = urlparse(dst_url)
        out_bucket = out_parsed.netloc
        in_parsed = urlparse(src_url)
        in_path = os.path.join(in_parsed.netloc, in_parsed.path)

        if out_parsed.path.endswith("/") or out_parsed.path == "":
            dst_url = os.path.join(dst_url, os.path.split(in_path)[1])

        tmp_bucket = bunnies.config.get("storage", {}).get("tmp_bucket", out_bucket)
        tmp_key = "reprod-data-import/%s" % (uuid.uuid4(),)
        tmp_url = "s3://%s/%s" % (tmp_bucket, tmp_key)

        digest_urls = digest_urls or {}
        expected_digests = utils.parse_digests([v for v in digest_urls.values()])

        guessed = guess_type(in_path)
        log.info("guessed Content-Type: %s Content-Encoding: %s", guessed[0], guessed[1])
        if not content_type:
            content_type = guessed[0]
        if not content_encoding:
            content_encoding = guessed[1]

        pipe_fp = None
        with open(in_path, "rb") as in_fp:
            try:
                if content_length <= 0:
                    content_length = guess_length(in_fp)

                # check if destination exists
                if expected_digests:
                    try:
                        match = self.__match_existing(dst_url, expected_digests,
                                                      expected_len=content_length,
                                                      expected_ct=content_type,
                                                      expected_ce=content_encoding,
                                                      logprefix=logprefix)
                        log.info("%sfile matches existing file %s. done.", logpad, dst_url)
                        return match
                    except ClientError as clierr:
                        code = clierr.response['Error']['Code']
                        if code == "404":
                            pass
                        else:
                            log.error("%serror checking for existing file: %s", logpad, clierr.response['Error'],
                                      exc_info=clierr)
                            raise ImportError("Error for URL %s: %s" % (dst_url, clierr.response['Error']))
                    except NoSuchFile:
                        pass

                start_time = time.time()
                # only run the MD5 algorithm
                pipe_fp = transfers.HashingReader(in_fp, algorithms=("md5",))

                transfers.s3_streaming_put(pipe_fp, tmp_url, content_type=content_type, content_length=content_length,
                                           meta={constants.IMPORT_DIGESTS_HEADER: ",".join(expected_digests.keys())},
                                           logprefix=logprefix)
                delta_t = time.time() - start_time
                xfer_cl = pipe_fp.tell()
                log.info("%sPUT completed in %8.3f seconds. (%8.3f MB/s)", logpad,
                         delta_t, xfer_cl / (1024*1024*(delta_t+0.00001)))
            finally:
                if pipe_fp:
                    pipe_fp.close()

        try:
            # check length
            if content_length >= 0:
                if content_length != xfer_cl:
                    log.error("%slength mismatch: uploaded %s bytes but expected %s", logpad, xfer_cl, content_length)
                    raise ImportError("length mismatch: downloaded %s bytes but expected %s" % (xfer_cl, content_length))
                log.info("%sdownload length match OK: %s", logpad, content_length)

            tmp_head = utils.get_blob_meta(tmp_url)
            if tmp_head["ContentLength"] != xfer_cl:
                log.error("%slength mismatch: uploaded %s bytes but expected %s", logpad, tmp_head['ContentLength'], xfer_cl)
                raise ImportError("length mismatch: uploaded %s but expected %s" % (tmp_head['ContentLength'], xfer_cl))
            else:
                log.info("%supload length match OK: %s", logpad, xfer_cl)

            # check digests
            xfer_digests = pipe_fp.hexdigests()
            for algo, expected_digest in expected_digests.items():
                if algo in xfer_digests and xfer_digests[algo] != expected_digest:
                    log.error("%s%s digest mismatch: got %s but expected %s", logpad, algo, xfer_digests[algo], expected_digest)
                    raise ImportError("digest mismatch: got %s but expected %s" % (algo, xfer_digests[algo], expected_digest))
                else:
                    log.info("%s%s digest match OK: %s", logpad, algo, expected_digest)
        except Exception:
            try:
                self.__delete(tmp_url, logprefix=logprefix)
            except Exception as delete_exc:
                log.error("%sdelete failed", logpad, exc_info=delete_exc)
            raise

        try:
            # compute on the remote, making sure they match what we've computed locally
            expected_digests.update(xfer_digests)
            return self.__rehash_copy(tmp_url, dst_url, tmp_head['ETag'], expected_digests, logprefix=logprefix)
        finally:
            self.__delete(tmp_url)

    def __upload_remote_file(self, src_url, dst_url, digest_urls=None, logprefix=""):
        """import an http(s)/ftp file

        The data transfer is executed from the lambda context.
        Once the data is copied into a temp location, hashes
        are verified.
        """
        logpad = logprefix + " " if logprefix else ""
        digest_list = [[k, v] for k, v in digest_urls.items()]

        req = {
            'download_only': True,  # skip the tail end copy to final dst
            'input': src_url,
            'output': dst_url,
            'digests': digest_list
        }

        log.info("%sdata-import request: %s", logpad, json.dumps(req, sort_keys=True, indent=4, separators=(",", ": ")))
        code, response = lambdas.invoke_sync(lambdas.DATA_IMPORT, Payload=req)
        data = response['Payload'].read().decode("ascii")
        if code != 0:
            raise BunniesException("data-import failed to complete: %s" % (data,))

        data_obj = json.loads(data)
        log.info("%sdata-import result: %s", logpad, data_obj)
        if data_obj['error_count'] > 0:
            raise Exception("data-import returned an error: %s" % (data_obj["results"][0],))

        details = data_obj['results'][0]
        if not details.get("move_to"):
            # final result - no delete needed.
            return details

        try:
            return self.__rehash_copy(details['output'], details['move_to'], details['ETag'], details["digests"],
                                      logprefix=logprefix)
        finally:
            self.__delete(details['output'], logprefix=logprefix)

    def ensure_bucket(self, bucket_name, **kwargs):
        default_region = utils.user_region()
        try:
            s3 = boto3.client("s3")
            bucket_cfg = kwargs.setdefault("CreateBucketConfiguration", {})
            bucket_cfg.setdefault("LocationConstraint", default_region)
            s3.create_bucket(Bucket=bucket_name, **kwargs)
        except ClientError as clierr:
            code = clierr.response['Error']['Code']
            if code == "BucketAlreadyOwnedByYou":
                return

            log.error("bucket creation failed: %s %s", clierr, code)
            raise

    def import_file(self, src_url, dst_url, digest_urls=None):
        """import a file, designated by `src_url` into a managed
           bunnies data blob designated by `dst_url`

           src_url:
              file://foo.txt              local file (relative)
              file:///etc/foo.txt         local file (abs)
              http://example.org/foo.txt  remote file
              s3://example-bucket/foo.txt remote s3 file

           digest_urls:
              { 'md5':  'http://example.org/path/to/md5sum',
                'sha1': 'hash://sha1/da39a3ee5e6b4b0d3255bfef95601890afd80709'
              }

           dst_url is an s3 url: s3://my-bucket-example/file.foo .
           if dst_url ends with /, or is just a bucket name, it dst_url
           will keep the same basename as src_url

           the destination will be created only if the digests match.

           it is safe to re-import the same source file.

           Returns the equivalent of a head object on the final destination.
        """
        src_parsed = urlparse(src_url)
        dst_parsed = urlparse(dst_url)
        if dst_parsed.scheme != "s3":
            raise ValueError("destination must be on s3")

        logprefix = "[#%04d %s]" % (self.num_imports, os.path.split(src_parsed.path)[1])
        self.num_imports += 1

        if src_parsed.scheme in ("http", "https", "ftp"):
            return self.__upload_remote_file(src_url, dst_url, digest_urls=digest_urls, logprefix=logprefix)

        if src_parsed.scheme in ("s3",):
            return self.__upload_s3_file(src_url, dst_url, digest_urls=digest_urls, logprefix=logprefix)

        if src_parsed.scheme in ("file",):
            return self.__upload_local_file(src_url, dst_url, digest_urls=digest_urls, logprefix=logprefix)

        if src_parsed.scheme == "":
            # assume file
            return self.__upload_local_file("file://" + src_url, dst_url, digest_urls=digest_urls, logprefix=logprefix)

        raise ValueError("unrecognized scheme: %s" % (src_url,))
