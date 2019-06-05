#!/usr/bin/env python3

"""
    Models for constructing a Bunnies pipeline
"""
import boto3
from . import utils
from . import constants

class NotImpl(Exception):
    pass

class Result(object):
    def canonical(self, strategy=None):
        """return the canonical name representing the result of
           a computation or the result of a data import.
        """
        raise NotImpl("canonical")

class ExternalFile(Result):
    def __init__(self, url, desc=None, digests=None):
        self.url = url
        self.desc = desc
        self.digests = {k:v for k,v in digests.items()} if digests else {}
        if not self.digests:
            raise Exception("at least one expected digest must be specified for external files")

    def canonical(self, strategy=None):
        #FIXME let strategy pick appropriate name
        hexdigest = self.digests['md5']
        return {"type": "blob",
                "md5": hexdigest}

class S3Blob(Result):
    def __init__(self, url, desc=None):
        self.url = url
        self.desc = desc

    def canonical(self, strategy=None):
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

class Transform(Result):
    """A transformation of inputs performed by a program,
       with the given parameters
    """
    def __init__(self, name, program=None, image=None):
        self.name = name
        self.program = program
        self.image = image
        self.inputs = {}
        self.params = {}

    def add_input(self, key, node, desc=""):
        self.inputs[key] = node

    def manifest(self):
        """
        the manifest is a dictionary document that contains the
        information necessary to reproduce the data transformation.

        it may not be the strict minimum amount of information

        """
        obj = {}
        obj['name']    = self.name
        obj['program'] = self.program
        obj['image']   = self.image
        obj['inputs']  = {k: self.inputs[k].canonical() for k, node in self.inputs.items()}
        obj['params']  = self.params
        return obj

    def canonical(self, strategy=None):
        """
        returns the minimal amount of information to represent the computation
        completely and unambiguously. two transformations producing the same
        canonical representation are considered equivalent.
        """
        manifest = self.manifest()
        raise NotImpl("Transform.canonical")

    @property
    def output(self):
        # obtain a proxy object that allows looking up
        # outputs in the transformation result.
        raise NotImpl("Transform.getOutput")
