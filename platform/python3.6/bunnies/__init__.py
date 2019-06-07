
#
# This module code is available in lambdas and container tasks
#
import sys
import os, os.path
import json
import logging
import errno

from . import exc
from . import utils
from . import constants
from .graph import S3Blob, Transform, ExternalFile
from .pipeline import build_target

logger = logging.getLogger(__package__)

def setup_logging(loglevel=logging.INFO):
    """configure custom logging for the platform"""
    root = logging.getLogger(__name__)
    root.setLevel(loglevel)
    ch = logging.StreamHandler(sys.stderr)
    ch.setLevel(loglevel)
    formatter = logging.Formatter('[%(asctime)s] %(name)s %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)

def _load_config():
    if _load_config.cache:
        return _load_config.cache


    startdir = os.path.dirname(os.path.realpath(__file__))
    settings = {}

    search_for = (
        ("cluster", "cluster-settings.json"),
        ("network", "network-settings.json"),
        ("storage", "storage-settings.json")
    )

    for name, target in search_for:
        doc = None
        infd = None
        try:
            infd = utils.find_config_file(startdir, target)
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
