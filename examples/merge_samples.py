#!/usr/bin/env python3
# -*- charset: utf-8; -*-

"""
An example reprod pipeline which aligns and merges samples
"""

# framework
import bunnies

# experiment specific
from snpcalling import InputFile, Align, Merge

bunnies.setup_logging()

ha412     = InputFile("s3://rieseberg-references/HA412/genome/Ha412HOv2.0-20181130.fasta")
ha412_idx = InputFile("s3://rieseberg-references/HA412/genome/Ha412HOv2.0-20181130.fasta.fai")

a1 = Align(
    sample_name="ANN0830",
    r1=InputFile("https://github.com/rieseberglab/fastq-examples/raw/master/data/HI.4038.002.index_10.ANN0830_R1.fastq.gz",
                 digests=("cfdbedf549fd23685321d7b27fccfb10",)),
    r2=InputFile("https://github.com/rieseberglab/fastq-examples/raw/master/data/HI.4038.002.index_10.ANN0830_R2.fastq.gz",
                 digests=("397c364cbad6cb16377f5572b89ec5c5",)),
    ref=ha412,
    ref_idx=ha412_idx)

a2 = Align(
    sample_name="ANN0830",
    r1=InputFile("https://github.com/rieseberglab/fastq-examples/raw/master/data/HI.4549.004.index_10.ANN0830_R1.fastq.gz",
                 digests=("f646412d9568e0c7f1822b951ccc2e6e",)),
    r2=InputFile("https://github.com/rieseberglab/fastq-examples/raw/master/data/HI.4549.004.index_10.ANN0830_R2.fastq.gz",
                 digests=("73ea5780ff055c35d1ac939e73c47580",)),
    ref=ha412,
    ref_idx=ha412_idx)

a3 = Align(
    sample_name="ANN0832",
    r1=InputFile("https://github.com/rieseberglab/fastq-examples/raw/master/data/HI.4019.002.index_8.ANN0832_R1.fastq.gz",
                 digests=("d841ccf568e94aec99418f232db4535a",)),
    r2=InputFile("https://github.com/rieseberglab/fastq-examples/raw/master/data/HI.4019.002.index_8.ANN0832_R2.fastq.gz",
                 digests=("41720b0a79e20dd81c8865d9404cd550",)),
    ref=ha412,
    ref_idx=ha412_idx)

all_bams = [a1, a2, a3]

# merge them by key
merged_bam1 = Merge("ANN0830", [bam for bam in all_bams if bam.sample_name == "ANN0830"])
merged_bam2 = Merge("ANN0832", [bam for bam in all_bams if bam.sample_name == "ANN0832"])

all_merged = [merged_bam1, merged_bam2]

# - fixates software versions and parameters
# - creates graph of dependencies
pipeline = bunnies.build_target(all_merged)

# pipeline.export_schedule("my_merge.json")

# Assembled pipelines can be archived and distributed.
# pipeline = reprod.import_pipeline("my_merge.json")

# a URL where we can see details and progress in the browser
# print(pipeline.dashboard_url())

for job in pipeline.build_order():
    print("build %s" % job.data)
    print("transfer_script %s" % job.execution_transfer_script())


#compute_env = bunnies.ComputeEnv("merge-example")
#compute_env.create()

