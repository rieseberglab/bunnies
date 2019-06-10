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

# if the destination path ends with '/' then the last component of the source filename is used for the destination file.
# analogous to `cp path/to/src.example path/to/dst/`.
importer.import_file("/data/ref_genome/HA412/genome/Ha412HOv2.0-20181130.fasta.fai", dst_path)

#
# Passing the same file again.  If a digest is known before upload, the data upload is skipped if the file is
# already on the destination.
#
importer.import_file("/data/ref_genome/HA412/genome/Ha412HOv2.0-20181130.fasta.fai", dst_path,
                     digest_urls={'md5': "71c539432800f3cb84c0ece01c494bbe"})


# This is optional, but large files should have digest information, if possible, to avoid unnecessary uploads.
importer.import_file("/data/ref_genome/HA412/genome/Ha412HOv2.0-20181130.fasta", dst_path,
                     digest_urls={'md5': "233696e492a8354a8cf1b4cd009e843a"})

dst_path = "s3://" + reference_bucket + "/HanXRQr1.0-20151230/genome/"
importer.import_file("/data/ref_genome/HanXRQr1.0-20151230/genome/HanXRQr1.0-20151230.fa.fai", dst_path)

importer.import_file("/data/ref_genome/HanXRQr1.0-20151230/genome/HanXRQr1.0-20151230.fa", dst_path,
                     digest_urls={'md5': "c2ec0e664ad2d26a5b0860e029fda6c2"})
