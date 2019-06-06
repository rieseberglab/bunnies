"""
misc utilities
"""

import os, os.path
import errno
from urllib.parse import urlparse
import hashlib

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

def canonical_hash(canon, algo='sha1'):
    """
    hash a canonical dictionary representation into a hexdigest
    """
    serialized = json.dumps(canon, sort_keys=True, separators=(",",  ":")).encode('utf-8')
    digest_obj = getattr(hashlib, algo)()
    digest_obj.update(serialized)
    return "%s_%s" % (algo, digest_obj.hexdigest())
