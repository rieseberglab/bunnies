"""
misc utilities
"""

import os
import os.path
import errno
from urllib.parse import urlparse
import hashlib
import json
import logging
import boto3
import base64
import glob
import fnmatch
import subprocess
import sys

from botocore.exceptions import ClientError
from .exc import NoSuchFile

logger = logging.getLogger(__package__)


def human_size(numbytes):
    """converts a number of bytes into a readable string by humans"""
    KB = 1024
    MB = 1024*KB
    GB = 1024*MB
    TB = 1024*GB

    if numbytes >= TB:
        amount = numbytes / TB
        unit = "TiB"
    elif numbytes >= GB:
        amount = numbytes / GB
        unit = "GiB"
    elif numbytes >= MB:
        amount = numbytes / MB
        unit = "MiB"
    elif numbytes >= KB:
        amount = numbytes / KB
        unit = "KiB"
    else:
        amount = numbytes
        unit = "B"
    return "%.3f%s" % (amount, unit)


def data_files(globname):
    """retrieves filenames under the data directory, matching the given file glob pattern (relative to the data dir)"""
    here = os.path.dirname(__file__)
    data_dir = os.path.join(here, "data")
    matches = [permfile for permfile in glob.glob(os.path.join(data_dir, globname))]
    return matches


def find_config_file(startdir, filname):
    """recurse in folder and parents to find filname and open it
       returns (fd, path)
    """
    parent = os.path.dirname(startdir)
    try:
        attempt = os.path.join(startdir, filname)
        return open(attempt, "r"), attempt
    except IOError as ioe:
        if ioe.errno != errno.ENOENT:
            raise
    # reached /
    if parent == startdir:
        return None, None

    return find_config_file(parent, filname)


def user_region():
    """get the effective profile's region"""
    # follow up: https://stackoverflow.com/questions/56502789/obtaining-the-boto3-profile-settings-from-python/56502829#56502829
    session = boto3.session.Session()
    return session.region_name


def s3_split_url(objecturl):
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


def get_blob_meta(objecturl, logprefix="", **kwargs):
    """fetches metadata about the given object. if the object doesn't exist. raise NoSuchFile if file doesn't exist

      Ex response:

       {'ContentType': 'binary/octet-stream',
        'Metadata': {},
        'ContentLength': 27523682,
        'LastModified': datetime.datetime(2019, 9, 4, 19, 42, tzinfo=tzutc()),
        'AcceptRanges': 'bytes',
        'ETag': '"9dc5c71abae60eec7ac0a27406789b24-6"',
        'ResponseMetadata': {'RequestId': '481AA20234257B4A',
                             'HostId': '+37u4lmlkTIFq/2tMcAYbfwtfX+zV54xj+9lRZk2tBCFFVtBDKY4uxcxlCEogGxXWBLokv+fOXM=',
                             'RetryAttempts': 0,
                             'HTTPHeaders': {'accept-ranges': 'bytes', 'content-length': '27523682',
                                             'date': 'Wed, 04 Sep 2019 19:50:14 GMT', 'content-type': 'binary/octet-stream',
                                             'x-amz-id-2': '+37u4lmlkTIFq/2tMcAYbfwtfX+zV54xj+9lRZk2tBCFFVtBDKY4uxcxlCEogGxXWBLokv+fOXM=',
                                             'etag': '"9dc5c71abae60eec7ac0a27406789b24-6"',
                                             'x-amz-request-id': '481AA20234257B4A', 'server':
                                             'AmazonS3', 'last-modified': 'Wed, 04 Sep 2019 19:42:00 GMT'},
                             'HTTPStatusCode': 200}
       }
    """
    bucketname, keyname = s3_split_url(objecturl)
    logprefix = logprefix + " " if logprefix else logprefix
    logger.debug("%sfetching meta for URL: %s", logprefix, objecturl)
    s3 = boto3.client('s3')
    try:
        head_res = s3.head_object(Bucket=bucketname, Key=keyname, **kwargs)
    except ClientError as clierr:
        if clierr.response['Error']['Code'] == '404':
            raise NoSuchFile(objecturl)
        logger.error("%scould not fetch URL (%s): %s", logprefix, repr(clierr.response['Error']['Code']), objecturl,
                     exc_info=clierr)
        raise
    return head_res


class StreamingBodyCtx(object):
    __slots__ = ("res", "body")

    def __init__(self, res):
        self.res = res
        self.body = res['Body']

    def __enter__(self):
        return self.body, self.res

    def __exit__(self, typ, value, traceback):
        self.body.close()


def get_blob_ctx(objecturl, logprefix="", **kwargs):
    """returns (body, info) for a given blob url.
       It takes care of closing the connection automatically.

        kwargs is passed to GetObject call

    >>> with get_blob_ctx("s3://foo/bar") as (body, info):
    ...    data = body.read()

    """
    bucketname, keyname = s3_split_url(objecturl)
    logprefix = logprefix + " " if logprefix else logprefix
    logger.info("%sfetching URL: %s", logprefix, objecturl)
    s3 = boto3.client('s3')
    try:
        res = s3.get_object(Bucket=bucketname, Key=keyname, **kwargs)
    except ClientError as clierr:
        error_code = clierr.response['Error']['Code']
        if error_code in ('404', 'NoSuchKey'):
            raise NoSuchFile(objecturl)
        logger.error("%scould not fetch URL (%s): %s", logprefix, repr(error_code), objecturl, exc_info=clierr)
        raise
    return StreamingBodyCtx(res)


def hash_data(data, algo='md5', encoding="hex"):
    digest_obj = getattr(hashlib, algo)()
    digest_obj.update(data)
    if encoding in ("hex", "hexdigest", 16):
        return digest_obj.hexdigest()
    elif encoding in ("base64", "b64", 64):
        return base64.b64encode(digest_obj.digest()).decode('ascii')
    return digest_obj


def canonical_hash(canon_obj, algo='sha1'):
    """hash a canonical dictionary representation into a hexdigest.

    contained objects must be JSONSerializable, and strings must be unicode, otherwise a TypeError is raised.
    """
    serialized = json.dumps(canon_obj, sort_keys=True, separators=(",",  ":")).encode('utf-8')
    digest_obj = getattr(hashlib, algo)()
    digest_obj.update(serialized)
    return "%s_%s" % (algo, digest_obj.hexdigest())


def load_json(obj):
    if isinstance(obj, str):
        return json.loads(obj)
    elif isinstance(obj, bytes):
        return json.loads(obj.decode('utf-8'))
    elif hasattr(obj, "read"):
        return json.load(obj)
    else:
        raise TypeError("cannot load json from this object")


def parse_digests(digests):
    """
    digests is either a single string digest,
    a list of string digests, or a dictionary of algo:hexdigest keypairs.

    string digest forms supported:

      1) "d41d8cd98f00b204e9800998ecf8427e"
      2) "md5:d41d8cd98f00b204e9800998ecf8427e"
      3) "md5_d41d8cd98f00b204e9800998ecf8427e"
      4) "hash://md5/d41d8cd98f00b204e9800998ecf8427e"
      algorithmic prefixes are ignored. algorithm
      deduced from hexdigest length

    returns a dictionary {'algo': 'hexdigest'}
    """
    def _atom(orig):
        s = orig
        if s.startswith("hash://"):
            s = os.path.split(s[len("hash://"):])[1]
        if ':' in s:
            # e.g. "md5:asdaddas"
            s = s.split(':')[-1]
        if '_' in s:
            # e.g. "sha1_asdsads"
            s = s.split('_')[-1]
        s = s.lower()
        res = {32: ('md5', s),
               40: ('sha1', s),
               64: ('sha256', s),
               128: ('sha512', s)}.get(len(s), None)
        if not res:
            raise ValueError("invalid digest string: %s" % (orig,))
        return res

    if isinstance(digests, (dict,)):
        return dict([_atom(v) for v in digests.values()])
    if not isinstance(digests, (tuple, list)):
        digests = (digests,)
    return dict([_atom(digest) for digest in digests])


def hex2b64(hexstr):
    if len(hexstr) % 2 != 0:
        raise ValueError("Invalid hexstring")
    hexbits = bytes([(int(hexstr[i], 16) << 4) + int(hexstr[i+1], 16) for i in range(0, len(hexstr), 2)])
    return base64.b64encode(hexbits).decode('ascii')


def walk_tree(rootdir, excludes=(), exclude_patterns=()):
    """
    yield files under rootdir, recursively, including empty folders, but
    excluding special files in excludes, or those matching globs in exclude_patterns
    """
    def _is_excluded(comp):
        return (comp in excludes) or any([fnmatch.fnmatchcase(comp, patt) for patt in exclude_patterns])

    def _get_entry(dirname, basename):
        if _is_excluded(basename):
            return

        return os.path.join(dirname, basename)

    for parent, dirs, files in os.walk(rootdir):
        if any([_is_excluded(comp) for comp in os.path.split(parent)]):
            continue

        for basename in files:
            entry = _get_entry(parent, basename)
            if entry:
                yield entry
        for basename in dirs:
            entry = _get_entry(parent, basename)
            if entry:
                yield entry


def run_cmd(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, log_on_err=True,
            show_out=False, cwd=None, log_cmd=True, **kwargs):
    """run a command -- wrapper around subprocess. returns the process object.

       check: True        throw an error if the subprocess fails. use the returncode attribute
                          to inspect the code when set to False.
       log_on_err: True   if the subprocess fails and the stdout/stderr are set to PIPE, then
                          the stdout and stderr contents are printed to the logs
       show_out: False    if the process exits successfully, and stdout is set to PIPE, then
                          log contents of stdout.

       the rest of the arguments are passed directly to subprocess.run
    """

    if cwd and cwd != ".":
        if log_cmd:
            logger.info("+CMD (cwd %s) %s", cwd, " ".join([repr(x) for x in args]))
    else:
        if log_cmd:
            logger.info("+CMD %s", " ".join([repr(x) for x in args]))

    proc = subprocess.run(args, stdout=stdout, stderr=stderr, check=False, cwd=cwd, **kwargs)
    if proc.returncode != 0 and log_on_err:
        logger.error("command returned code %d", proc.returncode)
        if stdout == subprocess.PIPE:
            for line in proc.stdout.decode('utf-8').splitlines():
                logger.error("out: %s", line)
        if stderr == subprocess.PIPE:
            for line in proc.stderr.decode('utf-8').splitlines():
                logger.error("err: %s", line)
    if proc.returncode == 0 and show_out and stdout == subprocess.PIPE:
        for line in proc.stdout.decode('utf-8').splitlines():
            logger.info("out: %s", line)

    if check:
        proc.check_returncode()
    return proc


def read_log_stream(log_group_name, stream_name, startTime=None, endTime=None, startFromHead=False):
    """yields each log event of the job,
       startTime and endTime are in ms.
    """
    # containerInstanceArn would likely allow obtaining logs for the instance.

    client = boto3.client('logs')

    extra = {}
    if startTime is not None:
        extra['startTime'] = startTime
    if endTime is not None:
        extra['endTime'] = endTime

    extra['startFromHead'] = startFromHead

    tokenKey = 'nextForwardToken' if startFromHead else 'nextBackwardToken'

    while True:
        try:
            resp = client.get_log_events(logGroupName=log_group_name, logStreamName=stream_name,
                                         **extra)
        except ClientError as clierr:
            if clierr.response['Error']['Code'] == 'ResourceNotFoundException':
                return
            else:
                raise

        if not resp['events']:
            return

        for event in resp['events']:
            yield event

        extra["nextToken"] = resp[tokenKey]


class UIOutput(object):
    """a writable file descriptor which automatically redirects to a scrollable pager (less) if standard out is a terminal, or
       stdout itself."""
    __slots__ = ("sub", "fd", "pager")

    def __init__(self, fileobj=None, pager="/usr/bin/less"):
        """fileobj is where the output should normally be sent.
           pager is the absolute path to the program used as the pager.
        """
        self.fd = fileobj or sys.stdout
        self.pager = pager
        self.sub = None

    def __enter__(self):
        if self.fd.isatty() and os.path.exists(self.pager) and os.access(self.pager, os.X_OK) and os.path.isfile(self.pager):
            self.sub = subprocess.Popen([self.pager], bufsize=0, executable=None, stdin=subprocess.PIPE,
                                        stdout=self.fd, stderr=sys.stderr, shell=False, env=None, universal_newlines=True)
            return self.sub.stdin
        return self.fd

    def __exit__(self, typ, value, traceback):
        if self.sub:
            self.sub.stdin.close()
            self.sub.communicate()
        self.sub = None
        self.fd = None

