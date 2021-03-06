import os
import sys
import os.path
import json

from . import utils
from . import exc


def _load_config():
    if _load_config.cache:
        return _load_config.cache

    startdir = os.path.dirname(os.path.realpath(__file__))
    settings = {}

    search_for = (
        ("cluster", "cluster-settings.json"),
        ("network", "network-settings.json"),
        ("storage", "storage-settings.json"),
        ("key",     "key-pair-settings.json"),
        ("environment", "environment-settings.json")
    )

    for name, target in search_for:
        doc = None
        infd = None
        try:
            infd, inpath = utils.find_config_file(startdir, target)
            if infd is None:
                sys.stderr.write("Warning: could not load %s file %s\n" % (name, target))
                continue
            try:
                doc = json.load(infd)
                _load_config.files[target] = os.path.realpath(inpath)

            except json.decoder.JSONDecodeError as decodeErr:
                raise exc.BunniesException("invalid json in %s file %s: %s" % (name, target, decodeErr))
            settings.update(doc)
        finally:
            if infd:
                infd.close()

    _load_config.cache = settings
    return settings
_load_config.cache = None
_load_config.files = {}

def active_config_files():
    return dict(_load_config.files)

config = _load_config()
