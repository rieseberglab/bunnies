#!/usr/bin/env python3

"""
    Models for constructing a Bunnies pipeline
"""
from . import constants
from . import utils


class NotImpl(Exception):
    pass


class Cacheable(object):
    def canonical(self):
        """
        returns the strict minimum amount of information for naming the object
        completely and unambiguously. two objects with the same canonical
        representation will be considered equivalent.
        """
        raise NotImpl("Cacheable.canonical")


class ExternalFile(Cacheable):
    """An opaque handle to data with known digest(s)"""
    def __init__(self, url, desc=None, digests=None):
        self.url = url
        self.desc = desc
        self.digests = utils.parse_digests(digests) if digests else {}
        if not self.digests:
            raise Exception("at least one expected digest must be specified for external files")

    def canonical(self):
        # FIXME let strategy pick appropriate name
        hexdigest = self.digests['md5']
        return {"type": "blob",
                "md5": hexdigest}

    def __str__(self):
        return "ExternalFile(%(url)s, info=%(info)s)" % {
            "url": self.url,
            "info": self.canonical()
        }

class S3Blob(Cacheable):
    __slots__ = ("url", "desc", "_manifest")

    def __init__(self, url, desc=None):
        self.url = url
        self.desc = desc
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

            # FIXME let strategy pick appropriate name
            hexdigest = head_digests['md5']
            self._manifest = {"type": "blob",
                              "md5": hexdigest,
                              "len": meta['ContentLength']}
        return self._manifest

    def canonical(self):
        # exclude length
        manifest = self.manifest()
        return {k: manifest[k] for k in ("type", "md5")}

class Input(Cacheable):
    """Inputs draw a named edge in the dependency graph.

       The description is a merely a hint to describe why the
       dependency exists.
    """
    __slots__ = ("name", "node", "desc")
    def __init__(self, name, node, desc=""):
        self.name = name
        self.node = node
        self.desc = desc

    def canonical(self):
        return self.node.canonical()

    def manifest(self):
        return {
            "name": self.name,
            "node": self.node,
            "desc": self.desc
        }

class NamedOutput(object):
    """Specifies an output for a given transformation.  The output path provides a specification of how to retrieve the
       produced output resource from the results of a transformation.
    """
    __slots__ = ('name', 'path', 'desc')

    def __init__(self, name, path, desc=""):
        self.name = name
        self.path = path
        self.desc = desc

class Transform(Cacheable):
    """A transformation of inputs performed by a program,
       with the given parameters
    """

    __slots__ = ("name", "desc", "version", "image", "inputs", "params")

    def __init__(self, name, version=None, image=None, desc=""):
        self.name = name
        self.desc = desc
        self.version = version
        self.image = image
        self.inputs = {}
        self.outputs = {}
        self.params = {}

    def __str__(self):
        return "Transform(%(name)s, %(version)s, %(params)s)" % {
            "name": self.name,
            "version": self.version,
            "params": self.params
        }

    def add_input(self, key, node, desc=""):
        self.inputs[key] = Input(key, node, desc=desc)

    def add_named_output(self, name, path, desc=""):
        self.outputs[name] = NamedOutput(name, path, desc)

    def manifest(self):
        """
        the manifest is a dictionary document that contains a full
        description of the data transformation. it describes the
        operation, the inputs for the operation, and its parameters.

        it can contain redundant information, and information that is
        supplementary to the transformation.
        """
        obj = {}
        obj['type']    = "transform"
        obj['name']    = self.name
        obj['desc']    = self.desc
        obj['version'] = self.version
        obj['image']   = self.image
        obj['inputs']  = {k: self.inputs[k].manifest() for k in self.inputs}
        obj['params']  = self.params
        obj['outputs'] = self.outputs
        return obj

    def canonical(self):
        """Returns the minimal amount of information for naming the object completely and unambiguously. If two different
        Transforms produce the same canonical representation, they are considered equivalent, and it will be assumed that
        they will produce results that are equivalent.

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
