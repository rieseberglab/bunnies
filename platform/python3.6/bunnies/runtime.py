#
# Utilities for running jobs. Intended to be used on the remote side of a compute job.
#
import os

from .unmarshall import unmarshall
from .utils import load_json

def job_manifest():
    with open(os.environ.get("JOB_MANIFEST"), "r") as fp:
        return load_json(fp)

def job_transform():
    manifest = job_manifest()
    return unmarshall(manifest)

