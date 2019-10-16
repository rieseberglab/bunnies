#
# Utilities for running jobs. Intended to be used on the remote side of a compute job.
#
import os
import os.path
import json
import io
import logging
import shutil
import tempfile
import zipfile
import boto3
import requests

from .utils import get_blob_ctx, walk_tree, run_cmd
from .exc import NoSuchFile

from . import transfers
from . import constants
from .config import active_config_files
from . import data_import
from .version import __version__

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


def setenv_batch_metadata():
    """run this from inside an AWS batch container job to define
       additional environment variables about the job's placement
    """
    if not os.environ.get("AWS_BATCH_JOB_ID", None):
        return
    batch_job_id = os.environ.get("AWS_BATCH_JOB_ID")
    batch = boto3.client('batch')
    job_descs = batch.describe_jobs(
            jobs=[batch_job_id]
    )['jobs']
    if not job_descs:
        return

    # look at container envelope in batch job
    job_desc = job_descs[0]
    this_container = job_desc.get('container', {})
    this_container_instance = this_container.get("containerInstanceArn", "")
    this_task_arn = this_container.get('taskArn', "")
    os.environ["BUNNIES_TASK_ARN"] = this_task_arn
    os.environ["BUNNIES_CONTAINER_INSTANCE_ARN"] = this_container_instance

    instance_id = requests.get("http://169.254.169.254/latest/meta-data/instance-id").text
    instance_type = requests.get("http://169.254.169.254/latest/meta-data/instance-type").text
    os.environ["BUNNIES_EC2_INSTANCE_ID"] = instance_id
    os.environ["BUNNIES_EC2_INSTANCE_TYPE"] = instance_type
    log.info("running on instance %s (type %s)", instance_id, instance_type)

    # get ecs cluster
    ecs_container_metadata_uri = os.environ.get("ECS_CONTAINER_METADATA_URI", "")
    ecs_cluster = None
    if ecs_container_metadata_uri:
        container_metadata = requests.get(ecs_container_metadata_uri).json()
        labels = container_metadata.get("Labels", {})
        ecs_cluster = labels.get("com.amazonaws.ecs.cluster", None)
        os.environ["BUNNIES_ECS_CLUSTER"] = ecs_cluster

        # Limits.CPU is in units of 1024/vcpu
        os.environ["BUNNIES_RES_VCPU"] = str((container_metadata['Limits']['CPU'] + 1023) // 1024)

        # MB
        os.environ["BUNNIES_RES_MEMORY"] = str(container_metadata['Limits']['Memory'])
        # log.info("container metadata: %s", container_metadata)

    # bunnies-ecs-instance-role
    # security_creds = requests.get("http://169.254.169.254/latest/meta-data/iam/security-credentials/").text
    # log.info("security creds: %s", security_creds)

    # ec2 = boto3.client('ec2')
    # instances = ec2.describe_instances(
    #     InstanceIds=[instance_id])
    # if not instances or not instances['Reservations']:
    #     return
    # res = instances['Reservations'][0]
    # if not res['Instances']:
    #     return
    # instance_info = res['Instances'][0]
    # log.info("instance info: %s", instance_info)


def update_result(result_url, logprefix="", **kwargs):
    """update job results file"""

    if not result_url.startswith("s3://"):
        raise ValueError("result path must be an s3 url")

    try:
        with get_blob_ctx(result_url, logprefix=logprefix) as (body, info):
            result = json.load(body)
    except NoSuchFile:
        result = {}

    ## overwrite version field
    result["platform_version"] = __version__
    for field in ('output', 'log', 'usage', 'manifest', 'output', 'canonical', 'environment'):
        if field in kwargs:
            result[field] = kwargs.get(field)

    # push the result back
    json_str = json.dumps(result, sort_keys=True, indent=4, separators=(',', ': '))
    fp = io.BytesIO(json_str.encode('utf-8'))
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
