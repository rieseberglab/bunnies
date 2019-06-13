#!/usr/bin/env bash

#
# install the linux lustre client on a docker image
#

imgname="$1"

BUILDDIR=$(mktemp -d -p . "build.docker.XXXX")

set -exo pipefail

function do_cleanup ()
{
    if [[ -d "$BUILDDIR" ]]; then rm -rf --one-file-system -- "$BUILDDIR"; fi
}

trap do_cleanup EXIT

# https://docs.aws.amazon.com/fsx/latest/LustreGuide/install-lustre-client.html
dockerfile="
FROM $imgname

ADD [\"https://downloads.whamcloud.com/public/lustre/lustre-2.10.6/ubuntu1604/client/lustre-client-modules-4.4.0-131-generic_2.10.6-1_amd64.deb\",\
     \"https://downloads.whamcloud.com/public/lustre/lustre-2.10.6/ubuntu1604/client/lustre-utils_2.10.6-1_amd64.deb\" /tmp]
"

tee "$BUILDDIR"/Dockerfile <<<"$dockerfile"

docker build -t foo "$BUILDDIR"
