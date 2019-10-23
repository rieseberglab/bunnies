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
logger.setLevel(logging.DEBUG)


def _bunnies_info(container):
    """extract bunnies information from container information in job"""
    if not container:
        return None
    env = container.get('environment', None)
    if not env:
        return None

    info = {
        'BUNNIES_JOBID': None,
        'BUNNIES_VERSION': None,
        'BUNNIES_ATTEMPT': None
    }

    for env_entry in env:
        name, val = env_entry['name'], env_entry['value']
        if name in info:
            info[name] = val
    return info


def _batch_handler(event, context):
    """ collect the job logs and usage logs from a completed bunnies batch job """

    detail = event['detail']

    job_id, status = detail.get('jobId', None), detail.get('status', None)
    if not job_id:
        logger.error("no jobId: %s", detail)
        return None

    if status not in ("SUCCEEDED", "FAILED"):
        logger.debug("skipping event id %s. state: %s", event['id'], status)
        return None

    if 'container' not in detail:
        logger.debug("skipping event id %s. missing container info", event['id'])
        return None

    bunnies_info = _bunnies_info(detail['container'])
    if not bunnies_info:
        logger.debug("skipping event id %s. could not extract platform information", event['id'])
        return None

    if None in [val for val in bunnies_info.values()]:
        logger.error("skipping event id %s. platform parameters missing: %s", event['id'], bunnies_info)
        return None

    # version match
    job_version = bunnies_info["BUNNIES_VERSION"]
    if job_version != bunnies.__version__:
        logger.debug("skipping event id %s. job has version %s but this lambda is version %s",
                     event['id'], job_version, bunnies.__version__)
        return None

    logger.debug("running on event id %s: %s", event['id'], event)

    job = bunnies.jobs.AWSBatchSimpleJob.from_job_id(job_id)
    if not job:
        logger.error("job not found: %s", job_id)
        return None

    logs = job.save_logs()
    usage_url, usage_info, usage_written = job.save_usage()

    return {
        'logs': logs,
        'usage_url': usage_url,
        'usage_info': usage_info,
        'usage_written': usage_written
    }


def lambda_handler(event, context):
    # Log the received event
    if event.get('source', "") != "aws.batch":
        logger.error("unexpected event source: %s", event)
        return None

    return _batch_handler(event, context)
