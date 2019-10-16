import os

PLATFORM = "bunnies"

# all files imported into s3 have the following key set
# in their metadata for each content digest processed on them
# e.g. "digest-md5", "digest-sha1", etc.
DIGEST_HEADER_PREFIX = "digest-"  # + algo.lowercase()

# this metadata key lists the algorithms that were listed at the time of import
# for content-integrity verification. comma separated.
# e.g.: syntax: import-digests: "md5,sha1"
IMPORT_DIGESTS_HEADER = "import-digests"

MB = 1024 * 1024
MAX_SINGLE_UPLOAD_SIZE = 5 * (1024 ** 3)
UPLOAD_CHUNK_SIZE = int(os.environ.get("UPLOAD_CHUNK_SIZE", "0"), 10) or 6*MB


CE_ECS_INSTANCE_ROLE = "bunnies-ecs-instance-role"
CE_SPOT_ROLE = "bunnies-ec2-spot-fleet-role"
CE_BATCH_SERVICE_ROLE = "bunnies-batch-service-role"
CE_INSTANCE_PROFILE = "bunnies-batch-instance-profile"


# reserved attribute name in json manifest dictionary
# to represent the "kind" of graph object serialized
MANIFEST_KIND_ATTR = "_kind"

#
# result file -- if this file exists, the transform has completed successfully
# and _all_ of its outputs have been successfully saved)
#
# {
#   'manifest': {...}
#   'output': {
#        'my_output1': "s3://path/to/file" || "./relative_path_to_file"
#   },
#   'log': ["url to raw log file"]
#   'usage': "url to resource usage statistics file"
# }
#
TRANSFORM_RESULT_FILE = PLATFORM + ".transform-result.json"
JOB_USAGE_FILE = PLATFORM + ".usage.json"
JOB_LOGS_PREFIX = PLATFORM + ".job."
