#!/usr/bin/env python3

#
# Throwaway script which uploads a subset of samples, verifying that they are ready
# on Nanuq.
#

import sys
import os, os.path
import logging
import boto3

def setup_logging(loglevel=logging.INFO):
    """configure custom logging for the platform"""

    root = logging.getLogger(__name__)
    root.setLevel(loglevel)
    ch = logging.StreamHandler(sys.stderr)
    ch.setLevel(loglevel)
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)
    root.propagate = False

setup_logging(logging.DEBUG)
log = logging.getLogger(__name__)

all_urls = {}

ready_projects = {
    "10618": True,
    "16040": True
}


def read_all_urls():
    all_urls = {}
    with open("TS-61400_2018_11_09_project_downloads.txt", "r") as infd:
        for line in infd:
            line = line.strip()
            if not line or line.startswith("#"): continue
            project_id, url = line.split(maxsplit=1)
            all_urls.setdefault(url, {})['ProjectId'] = project_id.strip()
    return all_urls


def read_download_set(fp, url_db, md5_db):
    for line in fp:
        line = line.strip()
        if not line or line.startswith("#"): continue
        #SAMPLENAME	SOURCE_TYPE	SOURCE_NAME	SOURCE_RUN	SOURCE_URL	EXTRA
        #ANN1225	NANUQ	ANN1225	HI.4549.005.index_25.ANN1225	https://genomequebec.mcgill.ca/nanuqMPS/readSetMd5Download/id/421984/type/READ_SET_FASTQ/filename/HI.4549.005.index_25.ANN1225_R1.fastq.gz.md5	md5
        #ANN1225	NANUQ	ANN1225	HI.4549.005.index_25.ANN1225	https://genomequebec.mcgill.ca/nanuqMPS/readSetMd5Download/id/421984/type/READ_SET_FASTQ_PE/filename/HI.4549.005.index_25.ANN1225_R2.fastq.gz.md5	md5
        samplename, src_type, src_name, src_run, src_url, extra = line.split('\t')
        if src_url not in url_db:
            log.error("URL %s not present in master set.", src_url)
            raise Exception("missing data")

        url_db[src_url]['SampleName'] = samplename
        url_db[src_url]['Type'] = extra

        prj = url_db[src_url]['ProjectId']
        if not ready_projects.get(prj, False):
            raise Exception("URL %s is for project %s, but that project is not yet available" % (src_url, prj))

        url_db[src_url]['Download'] = True
        url_db[src_url]['URL'] = src_url

        if extra == "md5":
            # allow finding the md5 entry from non md5 entry
            non_md5_base = os.path.basename(src_url)[:-4]
            if non_md5_base in md5_db:
                raise Exception("duplicate md5 entry:" + src_url)
            md5_db[non_md5_base] = url_db[src_url]

def main():
    all_urls = read_all_urls()
    md5s = {}
    read_download_set(sys.stdin, all_urls, md5s)
    BUCKET_NAME = os.environ.get("BUCKET_NAME", "rieseberglab-fastq-juliely-1")
    for url, data in all_urls.items():
        if not data.get('Download'): continue
        if data['Type'] == "fastq":
            md5_entry = md5s[os.path.basename(url)]
            if not md5_entry.get('Download'):
                raise Exception("URL %s is set for download, but its associated md5 isn't", url)
                return 1
            print(url, "s3://%s/fastq/%s/%s" % (BUCKET_NAME, data['SampleName'], os.path.basename(url)), "md5:" + md5_entry['URL'])


if __name__ == "__main__":
    sys.exit(main() or 0)
