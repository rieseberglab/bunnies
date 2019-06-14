#!/usr/bin/env bash

#
# install bunnies container base tools around an
# existing container with the user's tools.
#

imgname="$1"
BUILDDIR=$(mktemp -d -p . "build.docker.XXXX")
SCRIPTSDIR=$(dirname "$(readlink -f "$0")")

set -Exo pipefail
[[ -n "$imgname" ]]

function do_cleanup ()
{
    if [[ -d "$BUILDDIR" ]]; then rm -rf --one-file-system -- "$BUILDDIR"; fi
}

function ensure_repo ()
{
    local reponame="$1"
    aws ecr create-repository --repository-name "$reponame" > /dev/null || :

    # read the url back
    local repourl=$(aws ecr describe-repositories --repository-names "$reponame" | "$SCRIPTSDIR"/read_json_key repositories.0.repositoryUri)

    [[ -n "$repourl" ]] || return 1

    echo "$repourl"
}

trap do_cleanup EXIT

# Lustre should only be installed in containers if they can be made privileged.
# https://docs.aws.amazon.com/fsx/latest/LustreGuide/install-lustre-client.html
#
# ADD https://downloads.whamcloud.com/public/lustre/lustre-2.10.6/ubuntu1604/client/lustre-client-modules-4.4.0-131-generic_2.10.6-1_amd64.deb \
#     https://downloads.whamcloud.com/public/lustre/lustre-2.10.6/ubuntu1604/client/lustre-utils_2.10.6-1_amd64.deb /tmp/

dockerfile="
FROM $imgname

RUN apt-get update && \
    apt-get -y install unzip python3-pip && \
    pip3 install requests boto3==1.9.35 awscli && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ADD bunnies_entrypoint.sh /usr/local/bin/bunnies_entrypoint.sh

ENTRYPOINT [\"/usr/local/bin/bunnies_entrypoint.sh\"]
"

tee "$BUILDDIR"/Dockerfile <<<"$dockerfile"
cp -a "$SCRIPTSDIR"/entrypoint/bunnies_entrypoint.sh "$BUILDDIR"/

srcrepo="${imgname%%:*}"
srctag="${imgname##*:}"
if [[ "$srctag" == "" ]]; then srctag=latest; fi

dstrepo="${srcrepo}"
dsttag="${srctag}-bunnies"

docker build -t "${dstrepo}:${dsttag}" "$BUILDDIR"

# aws ecr get-login

ecsrepo=$(ensure_repo "${srcrepo}")

docker tag "${dstrepo}:${dsttag}" "$ecsrepo:$dsttag"
docker push "$ecsrepo:$dsttag"

