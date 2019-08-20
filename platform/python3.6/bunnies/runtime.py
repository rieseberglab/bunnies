#
# Utilities for running jobs. Intended to be used on the remote side of a compute job.
#
import os
import os.path
import json
import io
import logging
import subprocess
import shutil
import tempfile
import zipfile

from .unmarshall import unmarshall
from .utils import load_json, get_blob_ctx, parse_digests, walk_tree, run_cmd
from .exc import NoSuchFile

from . import transfers
from . import constants
from .config import config, active_config_files
from . import data_import

log = logging.getLogger(__name__)


#
# This is a hack. We point to the directory containing the setup.py for the platform code.
# Ideally, we'd be able to copy over the exact same version that is locally installed.
#
PLATFORM_SRC = os.path.join(os.path.dirname(__file__), "..")
REPO_ROOT = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
PLATFORM_EXTRA = [
    {"src": fpath,
     "dst": os.path.join(constants.PLATFORM, name)}
    for name, fpath in active_config_files().items()
]

def add_user_deps(root, includepath, excludes=(), exclude_patterns=()):
    """
    add file dependencies to install on the remote when running a task

    Include everything under <root>/<includepath>  (recursively, including empty folders),
    except files matching exactly one of the `excludes`, or one of the patterns in `exclude_patterns` (fnmatch)

    files on the remote will be accessible relative to <root>. i.e. add_user_deps("a", "b") will make "a/b" available
    as "b" on the remote.

    """
    included = []

    for fullname in walk_tree(os.path.join(root, includepath), excludes=excludes, exclude_patterns=exclude_patterns):
        relname = os.path.relpath(fullname, root)
        included.append({"src": fullname, "dst": relname})

    log.info("%d new file(s) marked as user dependencies", len(included))
    for f in included:
        log.debug("marked file %s as a dependency (remote-name: %s)",
                  f["src"], f["dst"])

    add_user_deps._files += included

# list of (path_to_source, path_in_dest)
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


def upload_user_context(output_prefix):
    """packages user and platform dependencies as a zip.
       uploads the zip under its canonical name.

       returns the URL to the final remote file.
    """
    if upload_user_context.cache.get(output_prefix, None) is not None:
        return upload_user_context.cache[output_prefix]

    # make temp dir to store platform files
    package_tmpdir = tempfile.mkdtemp(prefix="temp-packaging-", dir=".")
    try:
        # hack -- this needs to be done when wrapping the container image
        log.debug("installing platform module in %s", package_tmpdir)
        run_cmd(['pip', 'install', '-t', package_tmpdir, PLATFORM_SRC + "[lambda]"])

        log.info("preparing user context for upload...")
        with tempfile.NamedTemporaryFile(suffix=".zip", prefix="user-files-", dir=package_tmpdir, delete=False) as tmpfd:
            with zipfile.ZipFile(tmpfd, mode='w', compression=zipfile.ZIP_DEFLATED) as zipfd:

                # include platform python code and dependencies
                for tozip in walk_tree(package_tmpdir,
                                       excludes=(".metadata.json", os.path.basename(tmpfd.name), "__pycache__"),
                                       exclude_patterns=("*~",)):
                    relname = os.path.relpath(tozip, package_tmpdir)
                    zipfd.write(tozip, arcname=relname)
                    log.debug("added file %s", relname)

                # add platform extra
                for entry in PLATFORM_EXTRA:
                    zipfd.write(entry['src'], arcname=entry['dst'])
                    log.debug("added file %s", entry['dst'])

                # add user files
                for entry in add_user_deps._files:
                    zipfd.write(entry['src'], arcname=entry['dst'])
                    log.debug("added file %s", entry['dst'])

        log.info("uploading user_context %s ==> %s", tmpfd.name, output_prefix)

        zip_digests = None
        with open(tmpfd.name, "rb") as user_zip:
            hashr = transfers.HashingReader(user_zip)
            data = '_'
            while data:
                data = hashr.read(1024*1024)
            zip_digests = hashr.hexdigests()

        new_name = "user-package-%s.zip" % (zip_digests['sha1'],)
        dst_name = os.path.join(output_prefix, new_name)
        importer = data_import.DataImport()
        importer.import_file("file://%s" % (tmpfd.name,), dst_name, digest_urls=zip_digests)
        log.info("user context uploaded to: %s", dst_name)

        upload_user_context.cache[output_prefix] = dst_name
        return dst_name

    finally:
        # cleanup temp files
        if package_tmpdir: shutil.rmtree(package_tmpdir)

upload_user_context.cache = {}
