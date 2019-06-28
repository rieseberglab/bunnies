#!/usr/bin/env bash
set -x
date
echo "Args: $@"
env
echo "This is my simple test job!."
echo "jobId: $AWS_BATCH_JOB_ID"
echo "jobQueue: $AWS_BATCH_JQ_NAME"
echo "computeEnvironment: $AWS_BATCH_CE_NAME"
sleep $1
date
df -h || :
mount || :
find /data || :
echo "$$" >> "/data/foo.$$" || :
mkdir -p /data/x/ || :
find /data || :
echo "bye bye!!"
