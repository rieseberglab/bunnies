#!/usr/bin/env python3

"""
    Models for constructing a Bunnies pipeline
"""
import boto3
from . import utils
from . import constants

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
    def __init__(self, url, desc=None, digests=None):
        self.url = url
        self.desc = desc
        self.digests = {k:v for k,v in digests.items()} if digests else {}
        if not self.digests:
            raise Exception("at least one expected digest must be specified for external files")

    def canonical(self):
        #FIXME let strategy pick appropriate name
        hexdigest = self.digests['md5']
        return {"type": "blob",
                "md5": hexdigest}

class S3Blob(Cacheable):
    def __init__(self, url, desc=None):
        self.url = url
        self.desc = desc

    def canonical(self):
        bucketname, keyname = _s3_split_url(objecturl)
        s3 = boto3.client('s3')
        head_res = s3.head_object(Bucket=bucketname, Key=keyname)
        pfx = constants.DIGEST_HEADER_PREFIX
        head_digests = {key[len(pfx):]: val for key, val in head_res['Metadata'].items()
                        if key.startswith(pfx)}

        #FIXME let strategy pick appropriate name
        hexdigest = head_digests['md5']
        return {"type": "blob",
                "md5": hexdigest}

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

class Transform(Cacheable):
    """A transformation of inputs performed by a program,
       with the given parameters
    """
    def __init__(self, name, version=None, image=None, desc=""):
        self.name = name
        self.desc = desc
        self.version = version
        self.image = image
        self.inputs = {}
        self.params = {}

    def add_input(self, key, node, desc=""):
        self.inputs[key] = Input(key, node, desc=desc)

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
        return obj

    def canonical(self):
        """
        returns the minimal amount of information for naming the object
        completely and unambiguously. two transformations producing the same
        canonical representation are considered equivalent.

        all parameters and inputs that have a deterministic influence
        over the transformation's output should be covered in one form
        or another in the canonical representation.
        """

        obj = {
            'type': "transform",
            'name': self.name,
            'version': self.version,
            'image': self.image,
            'params': self.params,
            'inputs': {k: self.inputs[k].canonical(self) for k in self.inputs}
        }
        return obj
