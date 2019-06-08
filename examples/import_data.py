#!/usr/bin/env python3

""" this example shows how to add data to your bunnies data lake """

#
# Make sure you have run the configuration scripts and that
# core lambdas have been deployed.
#
import bunnies
import boto3

bunnies.setup_logging()

importer = bunnies.DataImport()

reference_bucket = "rieseberg-references"

importer.ensure_bucket(reference_bucket)

dst_path = "s3://" + reference_bucket + "/HA412/genome/"

importer.import_file("/data/ref_genome/HA412/genome/Ha412HOv2.0-20181130.fasta.fai", dst_path)
importer.import_file("/data/ref_genome/HA412/genome/Ha412HOv2.0-20181130.fasta", dst_path)

dst_path = "s3://" + reference_bucket + "/HanXRQr1.0-20151230/genome/"
importer.import_file("/data/ref_genome/HanXRQr1.0-20151230/genome/HanXRQr1.0-20151230.fa.fai", dst_path)
importer.import_file("/data/ref_genome/HanXRQr1.0-20151230/genome/HanXRQr1.0-20151230.fa", dst_path)
