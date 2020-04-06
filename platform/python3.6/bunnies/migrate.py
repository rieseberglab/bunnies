from contextlib import contextmanager
from collections.abc import MutableMapping

import json
import logging
import os.path

from collections import OrderedDict
import boto3
from botocore.exceptions import ClientError
from . import constants
from . import utils
from . import transfers
import datetime
import io

logger = logging.getLogger(__name__)


class Journal(MutableMapping):
    def __init__(self, fname, read_only=False):
        self.fname = fname
        self.fd = None
        self.read_only = read_only
        self.db = {}

    def __load(self):
        for line in self.fd:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            doc = json.loads(line)
            self.db[doc['src']] = doc['dst']
        logger.debug("%s: %d journal entries loaded.", self.fname, len(self.db))

    def __log(self, src, dst):
        logger.debug("journal %s => %s", src, dst)
        if self.read_only:
            return
        self.fd.write(json.dumps({'src': src, 'dst': dst}) + "\n")
        self.fd.flush()

    def __len__(self):
        return len(self.db)

    def __setitem__(self, key, val):
        if self.db.get(key, None) == val:
            return
        self.db[key] = val
        self.__log(key, val)

    def __getitem__(self, key):
        return self.db[key]

    def __delitem__(self, key):
        del self.db[key]

    def __iter__(self):
        return self.db.__iter__()

    def __contains__(self, item):
        return self.db.__contains__(item)

    def items(self):
        return self.db.items()

    @classmethod
    @contextmanager
    def open(cls, fname, read_only=False):
        journal_obj = cls(fname, read_only=read_only)
        open_mode = "r" if read_only else "a+"
        try:
            journal_obj.fd = open(journal_obj.fname, open_mode)
            journal_obj.fd.seek(0)
            journal_obj.__load()
        except FileNotFoundError:
            if journal_obj.read_only:
                pass
            else:
                journal_obj.fd.close()
                journal_obj.fd = None
                raise
        try:
            yield journal_obj
        finally:
            if journal_obj.fd:
                journal_obj.fd.close()
            journal_obj.fd = None


def _bucket_keys(bucket, prefix, client=None):
    if not client:
        client = boto3.client('s3')

    base_args = {
        "Bucket": bucket,
        "FetchOwner": False,
        "RequestPayer": "requester"
    }
    if prefix:
        base_args['Prefix'] = prefix

    logger.debug("listing keys. bucket=%s keyprefix=%s", repr(bucket), repr(prefix))
    total_count = 0
    cont_token = None
    resp = {'IsTruncated': True}
    while resp['IsTruncated'] == True:
        args = dict(base_args)
        if cont_token:
            args['ContinuationToken'] = cont_token
        resp = client.list_objects_v2(**args)

        if 'Contents' in resp:
            for info in resp['Contents']:
                total_count += 1
                yield info

        cont_token = resp.get('NextContinuationToken', None)
        logger.debug("listed %d %s entries (NextContinuationToken=%s...)",
                     total_count, bucket, cont_token[:20] if cont_token else None)


def _rewrite_results_file(src_url, dst_url, translations, client=None):
    """parse it and translate all recognized URLs"""

    if not client:
        client = boto3.client('s3')

    def _walk_obj(obj):
        if isinstance(obj, str):
            if obj in translations:
                _walk_obj.count += 1
                # logger.info("translating %s to %s", obj, translations.get(obj))
                return translations.get(obj)
            else:
                return obj
        if isinstance(obj, (list, tuple)):
            return [_walk_obj(x) for x in obj]
        if isinstance(obj, dict):
            return {_walk_obj(k): _walk_obj(v) for (k, v) in obj.items()}
        return obj
    _walk_obj.count = 0

    with utils.get_blob_ctx(src_url, RequestPayer='requester') as (body, info):
        json_obj = json.loads(body.read())
        json_meta = info['Metadata']
        json_ct = info.get('ContentType', "application/json")
        json_obj = _walk_obj(json_obj)

    logger.debug("performed %d URL translations in %s", _walk_obj.count, src_url)

    json_str = json.dumps(json_obj, sort_keys=True, indent=4, separators=(',', ': '))
    fp = io.BytesIO(json_str.encode('utf-8'))
    return transfers.s3_streaming_put(fp, dst_url, content_type=json_ct,
                                      meta=json_meta)


def _is_results_file(keyname):
    dirname, basename = os.path.split(keyname)
    if basename == constants.TRANSFORM_RESULT_FILE:
        return True
    return False


def _is_nested(fpath, indirs):
    if not fpath:
        return False

    while fpath.endswith("/"):
        fpath = fpath[:-1]

    if fpath in indirs:
        return True

    parent, base = os.path.split(fpath)
    return _is_nested(parent, indirs)


def _cmd_migrate_restore(srcpath, tier, dry_run=False, days=3, **kwargs):
    s3 = boto3.client("s3")
    src_bucket, src_keypart = utils.s3_split_url(srcpath)

    src_keys = [sk for sk in _bucket_keys(src_bucket, src_keypart, client=s3)]

    def _is_frozen(sk):
        return sk['StorageClass'] in ('GLACIER', 'DEEP_ARCHIVE')

    thaw_count = 0
    to_restore = [x for x in src_keys if _is_frozen(x)]

    # S3 doens't preserve lastmodified across copies, so we instead
    # copy them in order from oldest to newest -- this guarantees at least
    # that index files will remain newer than their associated data files.
    to_restore.sort(key=lambda sk: sk['LastModified'])

    logger.info("%d keys need to be restored.", len(to_restore))

    num_initiated = 0
    num_ongoing = 0
    num_done = 0

    for i, sk in enumerate(to_restore):
        logger.info("[%d/%d] restoring key s3://%s/%s", i+1, len(to_restore),
                    src_bucket, sk['Key'])

        if not dry_run:
            head = s3.head_object(Bucket=src_bucket,
                                  Key=sk['Key'],
                                  RequestPayer='requester')
            if head.get('Restore', None) is None:
                logger.info("[%d/%d] initiating restore (tier=%s days=%d): s3://%s/%s",
                            i+1, len(to_restore), tier, days, src_bucket, sk['Key'])
                num_initiated += 1
                s3.restore_object(
                    Bucket=src_bucket,
                    Key=sk['Key'],
                    RestoreRequest={
                        'Days': days,
                        'GlacierJobParameters': {
                            'Tier': tier,
                        },
                    },
                    RequestPayer='requester'
                )
            elif head.get('Restore', "").startswith('ongoing-request="false"'):
                logger.info("[%d/%d] already restored: s3://%s/%s",
                            i+1, len(to_restore), src_bucket, sk['Key'])
                num_done += 1
            else:
                logger.info("[%d/%d] restore ongoing:: s3://%s/%s",
                            i+1, len(to_restore), src_bucket, sk['Key'])
                num_ongoing += 1

    logger.info("total restores: %d. (initated: %d  ongoing: %d  completed: %d)",
                len(to_restore), num_initiated, num_ongoing, num_done)
    return None


def _cmd_migrate_bucket(srcpath, dstprefix, src_keyprefix="",
                        journal_path="migrate.journal.txt", dry_run=False,
                        keep_source=False, migrate_all=False, threads=1, **kwargs):

    s3 = boto3.client('s3')
    src_bucket, src_keypart = utils.s3_split_url(srcpath)
    if not src_keypart.startswith(src_keyprefix):
        raise ValueError("key portion of SRCPATH (%s) should start with KEYPREFIX (%s)" % (repr(src_keypart), repr(src_keyprefix)))

    dst_bucket, dst_keyprefix = utils.s3_split_url(dstprefix)

    logger.info("moving files under s3://%s/%s", src_bucket, src_keypart)
    if src_keyprefix:
        logger.info("stripping source prefix: %s", src_keyprefix)
    logger.info("moving files to: s3://%s/%s", dst_bucket, dst_keyprefix)

    def _dst_key(srckey):
        """srckey does not have the bucket information"""
        if srckey.startswith(src_keyprefix):
            srckey = srckey[len(src_keyprefix):]
        out = os.path.join(dst_keyprefix, srckey)
        # logger.debug("skey:%s src_keyprefix:%s dst_keyprefix:%s dkey:%s",
        #              srckey, src_keyprefix, dst_keyprefix, out)
        return out

    with Journal.open(journal_path, read_only=dry_run) as journal:

        src_keys = [sk for sk in _bucket_keys(src_bucket, src_keypart, client=s3)]
        # S3 doens't preserve lastmodified across copies, so we instead
        # copy them in order from oldest to newest -- this guarantees at least
        # that index files will remain newer than their associated data files.
        src_keys.sort(key=lambda sk: sk['LastModified'])

        transform_dirs = {}
        for sk in src_keys:
            if _is_results_file(sk['Key']):
                dirname, basename = os.path.split(sk['Key'])
                transform_dirs[dirname] = basename

        logger.info("found %d result source folders (prefix=%s)...", len(transform_dirs), srcpath)

        # list keys on target bucket
        dst_keys = {dk['Key']: dk['LastModified'] for dk in _bucket_keys(dst_bucket, dst_keyprefix, client=s3)}
        src_blobs = OrderedDict() # blobs to move to destination

        final_locations = {}
        # files we have moved on previous runs
        for old_location, new_location in journal.items():
            final_locations[old_location] = new_location

        # populate mapping of objects that need to still be moved.
        # we only consider files inside a bunnies result folder (unless migrate_all is True)

        copy_count = 0
        update_count = 0
        for sk in src_keys:
            if migrate_all or _is_nested(sk['Key'], transform_dirs):
                dk = _dst_key(sk['Key'])
                fullkey = "s3://%s/%s" % (src_bucket, sk['Key'])
                final_locations[fullkey] = "s3://%s/%s" % (dst_bucket, dk)

                # XXX
                # skip the copy if it exists
                if dk not in dst_keys:
                    copy_count += 1
                    src_blobs[sk['Key']] = {'dst_url': dk, 'src_info': sk}
                elif dst_keys[dk] < sk['LastModified']:
                    update_count += 1
                    src_blobs[sk['Key']] = {'dst_url': dk, 'src_info': sk}

        epoch = datetime.datetime(1970, 1, 1, 0, 0)

        # check if some targets overwrite some sources
        if src_bucket == dst_bucket:
            for src_blob, dst_info in src_blobs.items():
                if dst_info['dst_url'] in src_blobs:
                    raise ValueError("target file s3://%s/%s overwrites source file s3://%s/%s" %
                                     (dst_bucket, dst_info['dst_url'], src_bucket, src_blob))

        # check that all targets are currently available
        # FIXME -- check ongoing state
        storage_class_errors = []
        for i, (src_blob, dst) in enumerate(src_blobs.items()):
            sk = dst['src_info']
            if sk['StorageClass'] in ('GLACIER', 'DEEP_ARCHIVE'):
                logger.info("[%d/%d] checking glacier status for %s...",
                            i+1, len(src_blobs), src_blob)
                head = s3.head_object(Bucket=src_bucket,
                                      Key=src_blob,
                                      RequestPayer='requester')
                if not head.get('Restore', "").startswith('ongoing-request="false"'):
                    logger.error("blob %s is in not available. its storage class is: %s restore-status: %s",
                                 src_blob, sk['StorageClass'], head.get('Restore', "''"))
                    storage_class_errors.append(src_blob)

        if storage_class_errors:
            raise Exception("%d blobs are not available.", len(storage_class_errors))

        logger.info("migrating %d blobs (%d copies, %d updates)...",
                    len(src_blobs), copy_count, update_count)

        #
        # copy non-result files
        #

        migrate_files = [(x, y) for (x, y) in src_blobs.items() if not _is_results_file(x)]
        logger.info("migrating %d non-result files", len(migrate_files))
        for i, (src_blob, dst_info) in enumerate(migrate_files):

            dst_blob = dst_info['dst_url']
            src_size = dst_info['src_info']['Size']

            modified_since = dst_keys.get(dst_blob, epoch)
            logger.info("[%d/%d] s3://%s/%s  (%s) => s3://%s/%s",
                        i+1, len(src_blobs), src_bucket, src_blob, utils.human_size(src_size),
                        dst_bucket, dst_blob)

            if not dry_run:
                try:
                    _ = transfers.s3_copy_object("s3://%s/%s" % (src_bucket, src_blob),
                                                 "s3://%s/%s" % (dst_bucket, dst_blob),
                                                 logprefix="[%d/%d]" % (i+1, len(src_blobs)),
                                                 CopySourceIfModifiedSince=modified_since,
                                                 TaggingDirective='COPY',
                                                 RequestPayer='requester',
                                                 threads=threads
                    )
                except ClientError as clierr:
                    if clierr.response['Error']['Code'] == "PreconditionFailed":
                        logger.info("[%d/%d] Destination file already up to date.",
                                    i+1, len(src_blobs))

            # we log this before we delete the source
            journal["s3://%s/%s" % (src_bucket, src_blob)] = "s3://%s/%s" % (dst_bucket, dst_blob)

            if not dry_run and not keep_source:
                logger.info("[%d/%d] Deleting source s3://%s/%s",
                            i+1, len(src_blobs), src_bucket, src_blob)
                s3.delete_object(Bucket=src_bucket,
                                 Key=src_blob,
                                 RequestPayer='requester')

        #
        # migrate/rewrite result files
        #
        i_offset = len(migrate_files) + 1
        migrate_files = [(x, y) for (x, y) in src_blobs.items() if _is_results_file(x)]
        logger.info("migrating %d result manifests", len(migrate_files))
        for i, (src_blob, dst_info) in enumerate(migrate_files):
            dst_blob = dst_info['dst_url']

            logger.info("[%4d/%4d] s3://%s/%s  => s3://%s/%s",
                        i + i_offset, len(src_blobs), src_bucket, src_blob, dst_bucket, dst_blob)
            if not dry_run:
                _rewrite_results_file("s3://%s/%s" % (src_bucket, src_blob), "s3://%s/%s" % (dst_bucket, dst_blob),
                                      final_locations, client=s3)
            # log this before deleting the source
            journal["s3://%s/%s" % (src_bucket, src_blob)] = "s3://%s/%s" % (dst_bucket, dst_blob)
            if not dry_run and not keep_source:
                logger.info("[%4d/%4d] Deleting source s3://%s/%s",
                            i + i_offset, len(src_blobs), src_bucket, src_blob)
                s3.delete_object(Bucket=src_bucket,
                                 Key=src_blob,
                                 RequestPayer='requester')


def configure_parser(main_subparsers):
    parser = main_subparsers.add_parser("migrate", help="tools for data migration")
    subparsers = parser.add_subparsers(help="Perform data migration. Commands:", dest="migrate")

    subp = subparsers.add_parser("bucket", help="Migrate bunnies data from one bucket to another."
                                 "The calling user is assumed to own the destination bucket and will pay "
                                 "to pull resources from the source bucket (i.e. (RequestPayer=request).""")
    subp.set_defaults(func=_cmd_migrate_bucket)
    subp.add_argument("srcpath", metavar="SRCPATH", type=str,
                      help="migrate keys matching this urlprefix (e.g. s3://my-bucket.example.org/)")
    subp.add_argument("dstprefix", metavar="DSTPREFIX", type=str,
                      help="migrate to this key prefix. (e.g. s3://dst-bucket.example.org/data/")
    subp.add_argument("--srcprefix", dest="src_keyprefix", metavar="SRCPREFIX", default="", type=str,
                      help="identify the portion of the SRCPATH that should not be copied to the destination.")
    subp.add_argument("-n", dest="dry_run", action="store_true", default=False,
                      help="dry run. just print what will be done")
    subp.add_argument("--keep", dest="keep_source", action="store_true", default=False,
                      help="keep files in source location")
    subp.add_argument("--journal", metavar="JPATH", dest="journal_path", type=str, default="migrate.journal.txt",
                      help="append migrated records to this file")
    subp.add_argument("--threads", metavar="THREADS", type=int, default=1,
                      help="parallelize multipart uploads")
    subp.add_argument("--all", action="store_true", dest="migrate_all", default=False,
                      help="migrate all files you find, not just those produced by bunnies.")

    subp = subparsers.add_parser("restore", help="Change the storage class of one or more files in the bucket. "
                                 "The calling user is charged the request cost, but the owner will"
                                 " incur storage costs for the restored objects.")
    subp.set_defaults(func=_cmd_migrate_restore)
    subp.add_argument("srcpath", metavar="SRCPATH", type=str,
                      help="restore files matching this prefix. (e.g. s3://my-bucket/foo/)")
    subp.add_argument("--tier", metavar="TIER", type=str, default="Standard",
                      choices=["Expedited", "Standard", "Bulk"], help="""
How quickly the restore from glacier should happen.
   Expedited: 1-5min for GLACIER, n/a for DEEP_ARCHIVE
   Standard: 3-5h for GLACIER, 12h for DEEP_ARCHIVE.
             Bulk: 5-12h GLACIER, 48h DEEP_ARCHIVE.
""")
    subp.add_argument("--days", metavar="NDAYS", type=int, default=3,
                      help="number of days to unfreeze the data for.")
    subp.add_argument("-n", dest="dry_run", action="store_true", default=False,
                      help="dry run. just print what will be done.")
