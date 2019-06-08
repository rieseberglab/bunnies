import os

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
