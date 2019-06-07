# -*- charset: utf-8; -*-

import bunnies


def InputFile(url, desc="", digests=None):
    """
    Factory method to wrap various file URL forms into a Bunnies file
    """
    if url.startswith("s3://"):
        return bunnies.S3Blob(url, desc=desc)
    else:
        return bunnies.ExternalFile(url, desc=desc, digests=digests)


class Align(bunnies.Transform):
    """
    Align a paired-end fastq or sra file against a reference genome
    """
    ALIGN_IMAGE = "rieseberglab:5-2.3.0"
    VERSION = "1"

    TASK_NAME = "align-task"

    __slots__ = ("sample_name", "r1", "r2", "ref", "ref_index")

    def __init__(self, sample_name=None, r1=None, r2=None, ref=None, ref_idx=None):
        super().__init__("align", version=self.VERSION, image=self.ALIGN_IMAGE)

        if None in (sample_name, r1, ref, ref_idx):
            raise Exception("invalid parameters for alignment")

        self.sample_name = sample_name
        self.r1 = r1
        self.r2 = r2
        self.ref = ref
        self.ref_idx = ref_idx

        self.add_input("r1", r1,    desc="fastq forward reads")
        self.add_input("r2", r2,    desc="fastq reverse reads")
        self.add_input("ref", ref,  desc="reference fasta")
        self.add_input("ref_idx", ref_idx, desc="reference index")

        self.params["lossy"] = False
        self.params["sample_name"] = sample_name


class Merge(bunnies.Transform):
    """
    merge one or more bam files and modify the readgroup with the
    provided information. bams are merged in the order provided.
    """
    MERGE_IMAGE = "rieseberglab/analytics:5-2.3.2"
    VERSION = "1"

    TASK_NAME = "merge-task"

    __slots__ = ("sample_name",)

    def __init__(self, sample_name, bams):
        super().__init__("merge", version=self.VERSION, image=self.MERGE_IMAGE)
        self.sample_name = sample_name
        self.params["sample_name"] = sample_name
        if not bams:
            raise Exception("merging requires 1 or more inputs")
        for i, bam in enumerate(bams):
            self.add_input(i, bam, desc="aligned input #%d" % (i,))
