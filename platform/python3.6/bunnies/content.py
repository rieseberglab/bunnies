
class ContentResolver(object):
    """the ContentResolver is responsible for retrieving content based on
       digests or other symbolic representations of equivalent computation.
    """
    def fromCanonical(cls, canonical):
        # transform a hash into a s3 url, using one of the
        # resolvers available.
        # TODO options:
        #   - lookup contents of "digest-bucket/content/MD5_FOO"  (S3 symbolic link)
        #   - lookup "digests-table" in dynamodb
        #   - lookup equivalent of md5sum file in bucket.
        #
        raise NotImpl("ContentResolver.fromCanonical")
