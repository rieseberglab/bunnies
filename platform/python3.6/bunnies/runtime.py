#
# Utilities for running jobs. Intended to be used on the remote side of a compute job.
#
import os
import os.path
import json
import io
import logging
import fnmatch

from .unmarshall import unmarshall
from .utils import load_json, get_blob_ctx
from .exc import NoSuchFile

from . import transfers

log = logging.getLogger(__package__)


def add_user_deps(root, includepath, excludes=(), exclude_patterns=()):
    """
    add file dependencies to install on the remote when running a task

    Include everything under <root>/<includepath>  (recursively, including empty folders),
    except files matching exactly one of the `excludes`, or one of the patterns in `exclude_patterns` (fnmatch)
    """
    def _is_included(dirname, basename):
        if basename in excludes or any([fnmatch.fnmatchcase(basename, patt) for patt in exclude_patterns]):
            return

        fullname = os.path.join(dirname, basename)
        relname = os.path.relpath(fullname, root)
        return (fullname, relname)

    included = []
    for parent, dirs, files in os.walk(os.path.join(root, includepath)):
        for basename in files:
            tup = _is_included(parent, basename)
            if tup:
                included.append(tup)
        for basename in dirs:
            tup = _is_included(parent, basename)
            if tup:
                included.append(tup)

    log.info("%d new file(s) marked as user dependencies", len(included))
    for f in included:
        log.debug("marked file %s as a dependency (remote-name: %s)",
                  f[0], f[1])

    add_user_deps._files += included
add_user_deps._files = []

def add_user_hook(python_code):
    """
    adds a string of python code to execute on the remote side.
    this will execute within the execution transfer script
    """
    add_user_hook._hooks += [python_code]

add_user_hook._hooks = []


def update_result(result_url, logprefix="", **kwargs):
    """update job results file"""

    if not result_url.startswith("s3://"):
        raise ValueError("result path must be an s3 url")

    try:
        with get_blob_ctx(result_path, logprefix=logprefix) as (body, info):
            result = json.load(body)
    except NoSuchFile:
        result = {}

    for field in ('output', 'log', 'usage', 'manifest', 'output'):
        if field in kwargs.get(field, None) is not None:
            result[field] = kwargs.get(field)

    # push the result back
    fp = io.StringIO(json.dumps(result, sort_keys=True, indent=4, separators=(',', ': ')))
    transfers.s3_streaming_put(fp, result_url, content_type="application/json",
                               meta=None, logprefix=logprefix)

