# -*- charset: utf-8; -*-

import bunnies
import bunnies.unmarshall
import os
import sys
import logging

log = logging.getLogger(__package__)

ANALYTICS_IMAGE = "rieseberglab/analytics:7-2.5.5"

def setup_logging(loglevel=logging.INFO):
    """configure custom logging for the platform"""
    root = logging.getLogger(__package__)
    root.setLevel(loglevel)
    ch = logging.StreamHandler(sys.stderr)
    ch.setLevel(loglevel)
    formatter = logging.Formatter('[%(asctime)s] %(name)s %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)


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
    ALIGN_IMAGE = ANALYTICS_IMAGE
    VERSION = "1"

    __slots__ = ("sample_name", "r1", "r2", "ref", "ref_idx")

    kind = "snpcalling.Align"

    def __init__(self, sample_name=None, r1=None, r2=None, ref=None, ref_idx=None, lossy=False, manifest=None):
        super().__init__("align", version=self.VERSION, image=self.ALIGN_IMAGE, manifest=manifest)

        if manifest is not None:
            inputs, params = manifest['inputs'], manifest['params']
            r1 = inputs['r1'].node
            r2 = inputs['r2'].node
            ref = inputs['ref'].node
            ref_idx = inputs['ref_idx'].node

            lossy = params['lossy']
            sample_name = params['sample_name']

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
        self.params["lossy"] = bool(lossy)
        self.params["sample_name"] = sample_name

    @classmethod
    def task_template(cls, compute_env):
        scratchdisk = compute_env.get_disk('scratch')
        if not scratchdisk:
            raise Exception("Align tasks require a scratch disk")

        return {
            'jobtype': 'batch',
            'image': cls.ALIGN_IMAGE
        }

    def task_resources(self, **kwargs):
        # adjust resources based on inputs and job parameters
        return {
            'vcpus': 4,
            'memory': 12000,
            'timeout': 4*3600
        }

    def run(self, **params):
        """ this runs in the image """

        import tempfile
        import json

        def cache_remote_file(url, md5_digest, casdir):
            return bunnies.run_cmd([
                "cas", "-put", url, "-get", "md5:" + md5_digest, casdir
            ]).stdout.decode('utf-8').strip()

        workdir = params['workdir']
        s3_output_prefix = self.output_prefix()
        local_output_dir = os.path.join(workdir, "output")

        cas_dir = "/scratch/cas"
        os.makedirs(cas_dir, exist_ok=True)
        os.makedirs(local_output_dir, exist_ok=True)

        # log credentials
        bunnies.run_cmd([
            "curl", "-sv", "-o", "-", "http://169.254.169.254/latest/meta-data/iam/security-credentials/"
        ], show_out=True)

        #
        # download reference in /scratch
        # /scratch is shared with other jobs in the same compute environment
        #
        ref_target = self.ref.ls()
        ref_idx_target = self.ref_idx.ls()
        ref_path = cache_remote_file(ref_target['url'], ref_target['digests']['md5'], cas_dir)
        ref_idx_path = cache_remote_file(ref_idx_target['url'], ref_idx_target['digests']['md5'], cas_dir)

        align_args = [
            "align",
            "-cas", cas_dir
        ]
        if self.params['lossy']:
            align_args.append("-lossy")

        r1_target = self.r1.ls()
        r2_target = self.r2.ls()

        # write jobfile
        jobfile_doc = {
            self.params['sample_name']: {
                "name": self.params['sample_name'],
                "locations": [
                    [r1_target['url'], "md5:" + r1_target['digests']['md5']],
                    [r2_target['url'], "md5:" + r2_target['digests']['md5']]
                ]
            }
        }
        log.info("align job: %s", repr(jobfile_doc))
        with tempfile.NamedTemporaryFile(suffix=".job.txt", mode="wt",
                                         prefix=self.params['sample_name'], dir=workdir, delete=False) as jobfile_fd:
            json.dump(jobfile_doc, jobfile_fd)

        # num_threads = params['resources']['vcpus']
        align_args += [
            "-r", ref_path,
            "-i", jobfile_fd.name,
            "-o", local_output_dir,
            "-w", workdir,
            "-m",       # autodetect readgroup info
            "-d", "1",  # mark duplicates
            "-stats"
        ]

        bunnies.run_cmd(align_args, stdout=sys.stdout, stderr=sys.stderr, cwd=workdir)

        def _check_output_file(field, url, is_optional=False):
            try:
                meta = bunnies.utils.get_blob_meta(url)
                return {"size": meta['ContentLength'], "url": url}

            except bunnies.NoSuchFile:
                if is_optional:
                    return None
                raise Exception("output %s missing: %s" % (field, url))

        sn = self.params['sample_name']
        output = {
            "bam": _check_output_file("bam", "%s/%s.bam" % (s3_output_prefix, sn)),
            "bamstats": _check_output_file("bamstats", "%s/%s.bamstats.txt" % (s3_output_prefix, sn)),
            "bai": _check_output_file("bai", "%s/%s.bai" % (s3_output_prefix, sn)),
            "illuminametrics": _check_output_file("illuminametrics", "%s/%s.illuminametrics.txt" % (s3_output_prefix, sn)),
            "dupmetrics": _check_output_file("dupmetrics", "%s/%s.dupmetrics.txt" % (s3_output_prefix, sn)),
            "bam_md5": _check_output_file("bam.md5", "%s/%s.bam.md5" % (s3_output_prefix, sn))
        }

        return output

bunnies.unmarshall.register_kind(Align)


class Merge(bunnies.Transform):
    """
    merge one or more bam files and modify the readgroup with the
    provided information. bams are merged in the order provided.
    """
    MERGE_IMAGE = ANALYTICS_IMAGE
    VERSION = "1"

    __slots__ = ("sample_name",)
    kind = "snpcalling.Merge"

    def __init__(self, sample_name=None, aligned_bams=None, manifest=None):
        super().__init__("merge", version=self.VERSION, image=self.MERGE_IMAGE)

        if manifest is not None:
            inputs, params = manifest['inputs'], manifest['params']
            sample_name = params.get('sample_name')
            aligned_bams = []
            for i in range(0, params.get('num_bams')):
                aligned_bams.append(inputs.get(str(i)).node)

        self.sample_name = sample_name
        self.params["sample_name"] = sample_name
        self.params["num_bams"] = len(aligned_bams)

        if not aligned_bams:
            raise ValueError("merging requires 1 or more aligned bam inputs")
        if not sample_name:
            raise ValueError("you must specify the sample name to write")

        for i, bam in enumerate(aligned_bams):
            # print(self.sample_name, bam)
            self.add_input(str(i), bam, desc="aligned input #%d" % (i,))

    @classmethod
    def task_template(cls, compute_env):
        scratchdisk = compute_env.get_disk('scratch')
        if not scratchdisk:
            raise Exception("Merge tasks require a scratch disk")

        return {
            'jobtype': 'batch',
            'image': cls.MERGE_IMAGE
        }

    def task_resources(self, **kwargs):
        # adjust resources based on inputs and job parameters
        return {
            'vcpus': 4,
            'memory': 4000,
            'timeout': 1*3600
        }

bunnies.unmarshall.register_kind(Merge)
