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

from botocore.exceptions import ClientError
from .exc import NoSuchFile

logger = logging.getLogger(__package__)


def find_config_file(startdir, filname):
    """recurse in folder and parents to find filname and open it"""
    parent = os.path.dirname(startdir)
    try:
        return open(os.path.join(startdir, filname), "r")
    except IOError as ioe:
        if ioe.errno != errno.ENOENT:
            raise
    # reached /
    if parent == startdir:
        return None

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


def get_blob_meta(objecturl, logprefix=""):
    """fetches metadata about the given object"""
    bucketname, keyname = s3_split_url(objecturl)
    logprefix = logprefix + " " if logprefix else logprefix
    logger.info("%sfetching meta for URL: %s", logprefix, objecturl)
    s3 = boto3.client('s3')
    try:
        head_res = s3.head_object(Bucket=bucketname, Key=keyname)
    except ClientError as clierr:
        logger.error("%scould not fetch URL (%s): %s", logprefix, repr(clierr.response['Error']['Code']), objecturl,
                     exc_info=clierr)
        if clierr.response['Error']['Code'] == '404':
            raise NoSuchFile(objecturl)
        raise
    return head_res


def canonical_hash(canon_obj, algo='sha1'):
    """
    hash a canonical dictionary representation into a hexdigest
    """
    serialized = json.dumps(canon_obj, sort_keys=True, separators=(",",  ":")).encode('utf-8')
    digest_obj = getattr(hashlib, algo)()
    digest_obj.update(serialized)
    return "%s_%s" % (algo, digest_obj.hexdigest())


def parse_digests(digests):
    """
    digests is either a single string digest, or
    a list of string digests. digest string forms
    supported:

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
            s = os.path.split(s[7:])[1]
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

    if not isinstance(digests, (tuple, list)):
        digests = (digests,)
    return dict([_atom(digest) for digest in digests])


def hex2b64(hexstr):
    if len(hexstr) % 2 != 0:
        raise ValueError("Invalid hexstring")
    hexbits = bytes([(int(hexstr[i], 16) << 4) + int(hexstr[i+1], 16) for i in range(0, len(hexstr), 2)])
    return base64.b64encode(hexbits).decode('ascii')
