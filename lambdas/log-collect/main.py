#!/usr/bin/env python3

"""Lambda which collects logs when a job completes.

   This should be configured so that it runs _once_ whenever a bunnies job completes.

   jobs should have their environment set so that it is possible to identify that they
   are a bunnies job, submitted with a framework matching the version of this lambda.
"""

import boto3
import logging
import bunnies
import bunnies.jobs

batch = boto3.client('batch')

bunnies.setup_logging()


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _bunnies_info(container):
    """extract bunnies information from container information in job"""
    if not container:
        return None
    env = container.get('environment', None)
    if not env:
        return None

    info = {
        'BUNNIES_JOB_ID': None,
        'BUNNIES_VERSION': None,
        'BUNNIES_ATTEMPT': None
    }

    for env_entry in env:
        name, val = env_entry['name'], env_entry['val']
        if name in info:
            info[name] = val
    return info


def _batch_handler(event, context):
    """ collect the job logs and usage logs from a completed bunnies batch job """

    job_id, status = event.get('jobId', None), event.get('status', None)

    if status not in ("SUCCEEDED", "FAILED"):
        return None

    if 'container' not in event:
        return None

    bunnies_info = _bunnies_info(event['container'])
    if not bunnies_info:
        return None
    if None in [val for val in bunnies_info.values()]:
        return None

    # version match
    job_version = bunnies_info["BUNNIES_VERSION"]
    if job_version != bunnies.__version__:
        logger.debug("job has version %s but this lambda is version %s",
                     job_version, bunnies.__version__)
        return None

    job = bunnies.jobs.AWSBatchSimpleJob.from_job_id(job_id)
    if not job:
        logger.error("job not found: %s", job_id)
        return None

    usage_url, usage_info, usage_written = job.save_usage()
    logs = job.save_logs()

    return {
        'logs': logs,
        'usage_url': usage_url,
        'usage_info': usage_info,
        'usage_written': usage_written
    }


def lambda_handler(event, context):
    # Log the received event
    if event.get('source', "") != "aws.batch":
        return None

    return _batch_handler(event, context)
