
#
# This module code is available in lambdas and container tasks
#

import os, os.path
import json
import logging
import errno

from . import exc

logger = logging.getLogger(__name__)

def setup_logging(loglevel=logging.INFO):
    """configure custom logging for the platform"""
    root = logging.getLogger(__name__)
    root.setLevel(loglevel)
    ch = logging.StreamHandler(sys.stderr)
    ch.setLevel(loglevel)
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)

def _load_config():
    if _load_config.cache:
        return _load_config.cache

    def _find_file(startdir, filname):
        """recurse in folder and parents to find filname and open it"""
        parent = os.path.dirname(startdir)
        try:
            tryname = os.path.join(startdir, filname)
            fd = open(os.path.join(startdir, filname), "r")
            return fd
        except IOError as ioe:
            if ioe.errno != errno.ENOENT:
                raise
        # reached /
        if parent == startdir:
            return None

        return _find_file(parent, filname)


    startdir = os.path.dirname(os.path.realpath(__file__))
    settings = {}

    search_for = (
        ("cluster", "cluster-settings.json"),
        ("network", "network-settings.json")
    )

    for name, target in search_for:
        doc = None
        infd = None
        try:
            infd = _find_file(startdir, target)
            if infd is None:
                raise exc.BunniesException("couldn't load %s file %s" % (name, target))
            doc = json.load(infd)
            settings.update(doc)
        finally:
            if infd: infd.close()

    _load_config.cache = settings
    return settings
_load_config.cache = None


config = _load_config()


# main command line executable
def main():
    pass
