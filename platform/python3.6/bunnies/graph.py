#!/usr/bin/env python3

"""
    Models for constructing a Bunnies pipeline
"""
from . import constants
from . import utils
from . import config
from .exc import NotImpl, NoSuchFile, IntegrityException
from . import unmarshall


class Cacheable(object):
    """a cacheable resource, canonically named according to its contents or provenance"""
    __slots__ = ()

    def canonical(self):
        """
        returns the strict minimum amount of information for naming the resource
        completely and unambiguously. two objects with the same canonical
        representation will be considered equivalent.
        """
        raise NotImpl("Cacheable.canonical")

    @property
    def canonical_id(self):
        """
        retrieve an id string that can be used as a key to unambiguously identify this
        resource in a cache

        implementations might want to cache the result of the computation
        """
        canon_doc = self.canonical()
        return utils.canonical_hash(canon_doc)


class Target(object):
    """target is a collection of one or more resources that can be generated and retrieved"""
    __slots__ = ()

    def exists(self):
        """
        returns a URL if the target contents are readily available. None if it
        doesn't exist (yet).
        """
        raise NotImpl("Target.exists")

    def ls(self):
        """returns a "directory" (in the general sense) describing the target's contents, in the form of a {k:v} dictionary.

        targets are allowed to contain multiple sub-items. the directory is what a user of the Target would consult to
        pick a particular item or datum of interest.
        """
        raise NotImpl("Target.ls")


class ExternalFile(Cacheable, Target):
    """An opaque handle to data with known digest(s)"""

    kind = "bunnies.ExternalFile"

    def __init__(self, url, desc=None, digests=None, size=-1):
        self.url = url
        self.desc = desc
        self.digests = utils.parse_digests(digests) if digests else {}
        if 'md5' not in self.digests:
            raise ValueError("md5 missing from digests provided: %s" % (repr(digests),))
        self.size = size
        if self.size < 0:
            self.size = -1
        if not self.digests:
            raise Exception("at least one expected digest must be specified for external files")

    @classmethod
    def from_manifest(cls, doc):
        ef = cls(doc['url'], doc['desc'], digests=doc['digests'], size=doc['size'])
        return ef

    def manifest(self):
        return {
            constants.MANIFEST_KIND_ATTR: self.kind, # fixme meta class?
            'url': self.url,
            'desc': self.desc,
            'digests': self.digests,
            'size': self.size
        }

    def canonical(self):
        hexdigest = self.digests['md5']
        assert hexdigest
        return {
            'type': "blob",
            'md5': hexdigest
        }

    def __str__(self):
        return "ExternalFile(%(url)s, info=%(info)s)" % {
            "url": self.url,
            "info": self.canonical()
        }

    def exists(self):
        """
        Whether the external file is readily available.
        External files typically will exist already before the pipeline starts.
        """
        return self.url

    def ls(self):
        return {
            'url': self.url,
            'digests': self.digests
        }

unmarshall.register_kind(ExternalFile)


class S3Blob(Cacheable, Target):
    """a blob on S3

    blobs have a length and known digests.
    specifying the digests in the contruction is optional: they will be
    retrieved from the metadata on the remote object.

    """
    kind = "bunnies.S3Blob"

    __slots__ = ("url", "desc", "digests", "_manifest")

    def __init__(self, url, desc=None, digests=None):
        self.url = url
        self.desc = desc

        # we use those in case they cannot be retrieved from s3 metadata
        self.digests = utils.parse_digests(digests) if digests else {}

        self._manifest = None

    def __str__(self):
        return "S3(%(url)s, info=%(info)s)" % {
            "url": self.url,
            "info": "?" if not self._manifest else self._manifest
        }

    def manifest(self):
        if not self._manifest:
            meta = utils.get_blob_meta(self.url)
            pfx = constants.DIGEST_HEADER_PREFIX
            head_digests = {key[len(pfx):]: val for key, val in meta['Metadata'].items()
                            if key.startswith(pfx)}
            try:
                md5_digest = head_digests['md5']
                if "md5" in self.digests and md5_digest != self.digests['md5']:
                    raise IntegrityException("provided digest %s doesn't match digest in %s (%s)" % (
                        self.digests['md5'], self.url, md5_digest))
            except KeyError:
                if 'md5' not in self.digests:
                    raise IntegrityException("no md5 digest in %s on blob: %s" % (repr(head_digests), self.url))
                else:
                    head_digests['md5'] = self.digests['md5']

            self._manifest = {constants.MANIFEST_KIND_ATTR: self.kind,  # FIXME meta class
                              "desc": self.desc,
                              "url": self.url,
                              "digests": head_digests,
                              "size": meta['ContentLength']}
        return self._manifest

    @classmethod
    def from_manifest(cls, doc):
        obj = cls(doc["url"], desc=doc.get('desc'))
        # no need to do HEAD again.
        obj._manifest = doc
        return obj

    def canonical(self):
        manifest = self.manifest()
        # exclude length
        return {
            'type': 'blob',
            'md5': manifest['digests']['md5']
        }

    def exists(self):
        """External files are assumed to exist before the pipeline starts"""
        self.manifest() # for the side effects
        return self.url

    def ls(self):
        return self.manifest()

unmarshall.register_kind(S3Blob)


class Input(Cacheable):
    """Input attaches a name and description to an edge in the dependency graph.

       The description should explain why the dependency exists.
    """
    __slots__ = ("name", "node", "desc")

    kind = "bunnies.Input"

    def __init__(self, name, node, desc=""):
        self.name = name
        self.node = node
        self.desc = desc

    def canonical(self):
        # no extra information is added. give this the same id as the
        # node it is referencing.
        return self.node.canonical()

    def ls(self):
        return self.node.ls()

    def manifest(self):
        return {
            constants.MANIFEST_KIND_ATTR: self.kind, # fixme meta class?
            "name": self.name,
            "node": self.node.manifest(),
            "desc": self.desc
        }

    @classmethod
    def from_manifest(cls, doc):
        return cls(doc['name'], doc['node'], desc=doc.get('desc', ""))

unmarshall.register_kind(Input)


class Transform(Target, Cacheable):
    """
    A transformation of inputs performed by a program, with the given parameters
    """
    __slots__ = ("name", "desc", "version", "image", "inputs", "params", "_canonical_id")

    kind = "bunnies.Transform"

    def __init__(self, name=None, version=None, image=None, desc="", **kwargs):

        if not name:
            raise ValueError("all Transforms must have a name")

        if not version:
            raise ValueError("all Transforms must identify a version for their implementation")

        # image is optional

        self.name = name
        self.desc = desc
        self.version = version
        self.image = image

        self.inputs = kwargs.get('inputs', {})
        self.params = kwargs.get('params', {})

        self._canonical_id = None

    def __str__(self):
        return "Transform(%(name)s, %(version)s, %(params)s)" % {
            "name": self.name,
            "version": self.version,
            "params": self.params
        }

    def add_input(self, key, node, desc=""):
        # FIXME lock down the inputs if the canonical representation has been retrieved
        self.inputs[key] = Input(key, node, desc=desc)

    def manifest(self):
        obj = {}
        obj[constants.MANIFEST_KIND_ATTR] = self.kind # fixme meta class?
        obj['type']    = "transform"
        obj['name']    = self.name
        obj['desc']    = self.desc
        obj['version'] = self.version
        obj['image']   = self.image
        obj['inputs']  = {k: self.inputs[k].manifest() for k in self.inputs}
        obj['params']  = self.params
        return obj

    @classmethod
    def from_manifest(cls, doc):
        if doc['type'] != "transform":
            raise ValueError("expected type transform, but got %s" % (doc['type'],))

        inst = cls(manifest=doc)

        if inst.image != doc['image'] or inst.version != doc['version']:
            raise ValueError("expected manifest image anv version: %s %s, but got %s %s" %
                             (inst.image, inst.version, doc['image'], doc['version']))

        if inst.name != doc['name']:
            raise ValueError("expected transform name %s, but got %s" % (inst.name, doc['name']))

        return inst

    def canonical(self):
        """Returns the minimal amount of information for naming the object completely and unambiguously. If two different
        Transforms produce the same canonical representation, they are considered equivalent, and it will be assumed
        that they will produce results that are equivalent.

        All parameters and inputs that have a deterministic influence over the transformation's output (files) should
        therefore be covered in one form or another in the canonical representation. But, ideally, the canonical set of
        parameters should be as small as possible.

        """
        obj = {
            'type': "transform",
            'name': self.name,
            'version': self.version,
            'image': self.image,
            'params': self.params,
            'inputs': {k: self.inputs[k].canonical() for k in self.inputs}
        }
        return obj

    @property
    def canonical_id(self):
        if not self._canonical_id:
            self._canonical_id = super(Transform, self).canonical_id
        return self._canonical_id

    def output_prefix(self, bucket=None):
        bucket = bucket or config['storage']['build_bucket']
        return "s3://%(bucket)s/%(name)s-%(version)s-%(cid)s/" % {
            'name': self.name,
            'bucket': bucket,
            'version': self.version,
            'cid': self.canonical_id
        }

    def exists(self):
        """
        check if the results of the transformation exist
        """
        buckets = [config['storage']['build_bucket']]
        for bucket in buckets:
            try:
                candidate = "%(prefix)s%(result)s" % {
                    'prefix': self.output_prefix(bucket),
                    'result': constants.TRANSFORM_RESULT_FILE
                }
                _ = utils.get_blob_meta(candidate, logprefix=self.kind)
                return candidate
            except NoSuchFile:
                pass
        return None

    def task_resources(self, failures=None, **kwargs):
        """Return a dictionary of required resources to complete the work.

           The resources can be adjusted based on inputs and job parameters, which
           are available at the time of the call.

           Additional parameters are provided:
                 failures := [str, str, str] or None
                                          list of reasons (str) why this task has failed in the past.

           e.g.:

           {
               'vcpus': 1,
               'memory': 4000,
               'timeout': 4*3600
           }
        """
        raise NotImplementedError("subclasses must implement task_resources")

    def ls(self):
        transform_result = self.exists()
        if not transform_result:
            raise NoSuchFile("target is not available")

        with utils.get_blob_ctx(transform_result, logprefix=self.kind) as (body, info):
            doc = utils.load_json(body)

        return doc['output']

unmarshall.register_kind(Transform)

