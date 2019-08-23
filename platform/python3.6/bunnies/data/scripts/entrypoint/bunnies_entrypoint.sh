#!/bin/bash

# Copyright 2013-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the
# License. A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.

# This script can help you download and run a script from S3 using aws-cli.
# It can also download a zip file from S3 and run a script from inside.
# See below for usage instructions.

PATH="/bin:/usr/bin:/sbin:/usr/sbin:/usr/local/bin:/usr/local/sbin"
BASENAME="${0##*/}"

set -exo pipefail

usage () {
  if [ "${#@}" -ne 0 ]; then
    echo "* ${*}"
    echo
  fi
  cat <<ENDUSAGE
Usage:


export BUNNIES_USER_DEPS="s3://path/to/zipfile"
export BUNNIES_TRANSFER_SCRIPT="s3://path/to/transfer-script"
export BUNNIES_JOBID=some-job-id
${BASENAME} [command]

ENDUSAGE

  exit 2
}

# Standard function to print an error and exit with a failing return code
error_exit () {
  echo "${BASENAME} - ${@}" >&2
  exit 1
}

# Check what environment variables are set
if [[ -z "${BUNNIES_TRANSFER_SCRIPT}" ]]; then
    usage "BUNNIES_TRANSFER_SCRIPT not set"
fi

if [[ -z "${BUNNIES_JOBID}" ]]; then
    usage "BUNNIES_JOBID not set"
fi

scheme="$(echo "${BUNNIES_TRANSFER_SCRIPT}" | cut -d: -f1)"
if [[ "${scheme}" != "s3" ]]; then
    usage "BUNNIES_TRANSFER_SCRIPT must be for an S3 object; expecting URL starting with s3://"
fi

# Check that necessary programs are available
which aws >/dev/null 2>&1 || error_exit "Unable to find AWS CLI executable."
which unzip >/dev/null 2>&1 || error_exit "Unable to find unzip executable."

# Create a temporary directory to hold the downloaded contents, and make sure
CLEANUP_EXTRA=()
cleanup () {
   if [[ -z "${KEEP_TMP}" ]] && [[ -n "${TMPDIR}" ]] && [[ "${TMPDIR}" != "/" ]]; then
       rm -rf --one-file-system -- "${TMPDIR}"
   fi

   if [[ -z "${KEEP_TMP}" ]] && [[ "${#CLEANUP_EXTRA[@]}" -gt 0 ]]; then
       rm -rf --one-file-system -- "${CLEANUP_EXTRA[@]}"
   fi
}
trap 'cleanup' EXIT

# mktemp arguments are not very portable.  We make a temporary directory with
# portable arguments, then use a consistent filename within.
TMPTEMPLATE="${BUNNIES_JOBID}-XXXXXXX"
TMPDIR="$(mktemp -d -t "$TMPTEMPLATE")" || error_exit "Failed to create temp directory."
TMPFILE="${TMPDIR}/jobscript"
install -m 0600 /dev/null "${TMPFILE}" || error_exit "Failed to create temp file."


# Fetch and run a script
fetch_and_run_script () {
  # Create a temporary file and download the script
  aws s3 cp "${BUNNIES_TRANSFER_SCRIPT}" - > "${TMPFILE}" || error_exit "Failed to download S3 script."

  # Make the temporary file executable and run it with any given arguments
  chmod u+x "${TMPFILE}" || error_exit "Failed to chmod script."

  : creating workdir
  BUNNIES_WORKDIR=$(mktemp -d -p "/scratch/" "${BUNNIES_JOBID}-XXXXX")
  CLEANUP_EXTRA+=("${BUNNIES_WORKDIR}")
  export BUNNIES_WORKDIR

  : logging space available
  df -h

  : logging environment
  env

  : logging container metadata
  curl -v -o - "${ECS_CONTAINER_METADATA_URI}" || :
  echo # flush line

  (
      cd "${BUNNIES_WORKDIR}" && "${TMPFILE}" "${@}"
  )
}


# Download a zip and run a specified script from inside
unpack_user_deps () { # s3_url targetdir

    # Create a temporary file and download the zip file
    local tmpzip="$(mktemp -t user_deps.XXXXXXX.zip)" || error_exit "cannot create temp file for user deps archive"
    EXTRA_CLEANUP+=( "$tmpzip" )
    aws s3 cp "${1}" - > "$tmpzip" || error_exit "Failed to download user deps zip file from ${1}"

    # Create a temporary directory and unpack the zip file
    unzip -q -d "${2}" "$tmpzip" || error_exit "Failed to unpack zip file."
}

if [[ -n "${BUNNIES_USER_DEPS}" ]]; then
    unpack_user_deps "${BUNNIES_USER_DEPS}" "${TMPDIR}"
    if [[ -z "$PYTHONPATH" ]]; then
	export PYTHONPATH="$TMPDIR"
    else
	export PYTHONPATH="$PYTHONPATH:$TMPDIR"
    fi
fi

fetch_and_run_script
