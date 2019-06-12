#!/usr/bin/env python3

"""
An example reprod pipeline which aligns and merges samples
"""

# framework
import bunnies

# experiment specific
from snpcalling import InputFile, Align, Merge


ha412     = InputFile("s3://rieseberg-references/HA412/genome/Ha412HOv2.0-20181130.fasta")
ha412_idx = InputFile("s3://rieseberg-references/HA412/genome/Ha412HOv2.0-20181130.fasta.fai")

a1 = Align(
    sample_name="ANN0830",
    r1=InputFile("https://github.com/rieseberglab/fastq-examples/blob/master/data/HI.4038.002.index_10.ANN0830_R1.fastq.gz",
                 digests=("d41d8cd98f00b204e9800998ecf8427e",)),
    r2=InputFile("https://github.com/rieseberglab/fastq-examples/blob/master/data/HI.4038.002.index_10.ANN0830_R2.fastq.gz",
                 digests=("d41d8cd98f00b204e9800998ecf8427e",)),
    ref=ha412,
    ref_idx=ha412_idx)

a2 = Align(
    sample_name="ANN0830",
    r1=InputFile("https://github.com/rieseberglab/fastq-examples/blob/master/data/HI.4549.004.index_10.ANN0830_R1.fastq.gz?raw=true",
                 digests=("d41d8cd98f00b204e9800998ecf8427e",)),
    r2=InputFile("https://github.com/rieseberglab/fastq-examples/blob/master/data/HI.4549.004.index_10.ANN0830_R2.fastq.gz?raw=true",
                 digests=("d41d8cd98f00b204e9800998ecf8427e",)),
    ref=ha412,
    ref_idx=ha412_idx)

a3 = Align(
    sample_name="ANN0832",
    r1=InputFile("https://github.com/rieseberglab/fastq-examples/blob/master/data/HI.4019.002.index_8.ANN0832_R1.fastq.gz?raw=true",
                 digests=("d41d8cd98f00b204e9800998ecf8427e",)),
    r2=InputFile("https://github.com/rieseberglab/fastq-examples/blob/master/data/HI.4019.002.index_8.ANN0832_R2.fastq.gz?raw=true",
                 digests=("d41d8cd98f00b204e9800998ecf8427e",)),
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

task_resp = bunnies.execute.ecs_run_task("align-task", {
    "containerOverrides": [
        {
            'name': 'main',  # name of container config to override. from task definition.
            'environment': [
                {
                    'name': "JOBSPEC",
                    'value': "TEST LAUNCH"
                }
            ],
        }
    ]
})

print(task_resp)
