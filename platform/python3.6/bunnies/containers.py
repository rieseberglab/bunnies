#
# Tools to manipulate docker container images
#
from .constants import PLATFORM
from .utils import data_files, run_cmd

import boto3
from botocore.exceptions import ClientError

import os.path
import subprocess
import logging
import base64

log = logging.getLogger(__name__)

def _docker_login_to_registry(registry_id, registry_uri):
    log.debug("authenticating docker client to registry id %s url %s...", registry_id, registry_uri)
    client = boto3.client('ecr')
    token = client.get_authorization_token(registryIds=[registry_id])
    decoded_user_pass = base64.b64decode(token['authorizationData'][0]['authorizationToken']).decode('utf-8')
    user, passwd = decoded_user_pass.split(":", 1)

    server = registry_uri.split("/", 1)[0]

    # FIXME some docker clients require "-e none" as part of the arguments
    run_cmd(["docker", "login", "-u", user, "--password-stdin", "https://" + server], input=passwd.encode('utf-8'))

    log.debug("authenticated for the next 12 hours.")


def wrap_user_image(src_image):
    """
    makes the user's image `src_image` (image:tag) compatible with the platform's runtime.

    if the source images's tag ends with the name of the platform, the image is assumed
    to already be compatible.

    images that have been seen before will be cached.
    """
    if src_image.endswith("-" + PLATFORM):
        return user_image

    src_repo, src_tag = (src_image.rsplit(":", 1) + ["latest"])[0:2]
    src_image = src_repo + ":" + src_tag

    if src_image in wrap_user_image.cache:
        return wrap_user_image.cache[src_image]

    dst_repo, dst_tag = (src_repo, src_tag + "-" + PLATFORM)

    log.info("wrapping user image %s:%s ==> %s:%s", src_repo, src_tag, dst_repo, dst_tag)
    wrap_helper_script = data_files(os.path.join("scripts", "wrap-user-image.sh"))[0]
    run_cmd([wrap_helper_script, "--nopush", src_repo + ":" + src_tag, dst_repo + ":" + dst_tag],
            stderr=subprocess.STDOUT)

    # ensure repo creation
    log.info("image will be hosted in ECR repository %s ...", dst_repo)
    client = boto3.client("ecr")
    try:
        resp = client.create_repository(repositoryName=dst_repo,
                                        tags=[{'Key': "platform", 'Value': PLATFORM}])['repository']
    except ClientError as clierr:
        if clierr.response['Error']['Code'] == 'RepositoryAlreadyExistsException':
            resp = client.describe_repositories(repositoryNames=[dst_repo])['repositories'][0]
        else:
            raise

    repo_uri = resp['repositoryUri']
    registry_id = resp['registryId']

    run_cmd(["docker", "tag", dst_repo + ":" + dst_tag, repo_uri + ":" + dst_tag])
    _docker_login_to_registry(registry_id, repo_uri)
    run_cmd(["docker", "push", repo_uri + ":" + dst_tag])
    log.info("image pushed: %s:%s ==> %s:%s", dst_repo, dst_tag, repo_uri, dst_tag)

    wrap_user_image.cache[src_image] = repo_uri + ":" + dst_tag
    return wrap_user_image.cache[src_image]

wrap_user_image.cache = {}
