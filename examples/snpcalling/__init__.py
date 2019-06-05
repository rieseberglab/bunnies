# -*- charset: utf-8; -*-

import bunnies


def InputFile(url, description=""):
    if url.startswith("s3://"):
        return bunnies.S3Blob(url, desc=desc)
    else:
        raise Exception("unknown input type: %s" % (url,))

class Align(bunnies.Transform):

    ALIGN_IMAGE = "rieseberglab:5-2.3.0"

    def __init__(self, sample_name=None, r1=None, r2=None, ref=None, ref_index=None):
        super().__init__("align", image=ALIGN_IMAGE)

        if None in (sample_name, r1, ref, ref_index):
            raise Exception("invalid parameters for alignment")

        self.add_input("r1", r1)
        self.add_input("r2", r2)
        self.add_input("ref", ref)
        self.add_input("ref_index", ref_index)
        self.add_param

class Merge(object):
    pass

