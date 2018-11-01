#!/usr/bin/env python3
"""
An example reprod pipeline which aligns and merges samples
"""

# framework
import reprod

# experiment specific
from snpcalling import InputFile, Align, Merge


reference = InputFile("s3://reprod-example-bucket/HanXRQ.fasta")

a1 = Align(
    sample_name="ANN0830",
    r1=InputFile("https://github.com/rieseberglab/fastq-examples/blob/master/data/HI.4038.002.index_10.ANN0830_R1.fastq.gz"),
    r2=InputFile("https://github.com/rieseberglab/fastq-examples/blob/master/data/HI.4038.002.index_10.ANN0830_R2.fastq.gz"),
    ref=reference)

a2 = Align(
    sample_name="ANN0830",
    r1=InputFile("https://github.com/rieseberglab/fastq-examples/blob/master/data/HI.4549.004.index_10.ANN0830_R1.fastq.gz?raw=true"),
    r2=InputFile("https://github.com/rieseberglab/fastq-examples/blob/master/data/HI.4549.004.index_10.ANN0830_R2.fastq.gz?raw=true"),
    ref=reference)

a3 = Align(
    sample_name="ANN0832",
    r1=InputFile("https://github.com/rieseberglab/fastq-examples/blob/master/data/HI.4019.002.index_8.ANN0832_R1.fastq.gz?raw=true"),
    r2=InputFile("https://github.com/rieseberglab/fastq-examples/blob/master/data/HI.4019.002.index_8.ANN0832_R2.fastq.gz?raw=true"))

all_bams = [a1, a2, a3]

# merge them by key. naive.
merged_bam1 = Merge([bam for bam in all_bams if bam.sample_name == "ANN0830"])
merged_bam2 = Merge([bam for bam in all_bams if bam.sample_name == "ANN0832"])

all_merged = [merged_bam1, merged_bam2]

# - fixates software versions and parameters
# - creates graph of dependencies
pipeline = reprod.build_target(all_merged)

reprod.export_pipeline("my_merge.json")

# Assembled pipelines can be archived and distributed.
# pipeline = reprod.import_pipeline("my_merge.json")

# a URL where we can see details in the browser
print(pipeline.url())
