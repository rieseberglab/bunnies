# -*- charset: utf-8; -*-

import bunnies
import bunnies.unmarshall
import os


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
    ALIGN_IMAGE = "rieseberglab/analytics:5-2.3.2"
    VERSION = "1"

    __slots__ = ("sample_name", "r1", "r2", "ref", "ref_idx")

    kind = "snpcalling.Align"

    def __init__(self, sample_name=None, r1=None, r2=None, ref=None, ref_idx=None, lossy=False, manifest=None):
        super().__init__("align", version=self.VERSION, image=self.ALIGN_IMAGE, manifest=manifest)

        if manifest is not None:
            inputs, params = manifest['inputs'], manifest['params']
            r1 = inputs['r1'].node
            r2 = inputs['r2'].node
            ref = inputs['ref'].node,
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
            'memory': 4000,
            'timeout': 4*3600
        }

    @classmethod
    def run(self, **params):
        """ this runs in the image """
        workdir = params['workdir']
        outputdir = params['outdir']

        outputs = {
            'bam': None,
            'bai': None
        }

        s3_output_prefix = self.output_prefix()
        local_output_dir = os.path.join(workdir, "output")

        cas_dir = "/scratch/cas"
        os.makedirs(cas_dir)
        os.makedirs(local_output_dir)

        align_args = [
            "time",
            "align",
            "-cas", cas_dir
        ]
        if self.params['lossy']:
            align_args += ["-lossy"]

        jobfile_doc = {
            self.params['sample_name']: {
                "name": self.params['sample_name'],
                "locations": [
        num_threads = params['resources']['vcpus']
        align_args += [
            "-r", self.ref.url(),
            "-i", JOBFILE,
            "-o", s3_output_prefix,
            "-n", str(num_threads),
            "-w", workdir
        ]

        bunnies.run_cmd(
            show_out=True, cwd=workdir)


        time {params.align_bin} \
	  "\\${{cas_args[@]}}" \
	  -r file:{input.reference} \
	  -i file:{input.jobfile} \
	  -x file:{input.exclusion} \
	  -o file:\\$OUTDIR \
          -n {threads} \
          -w \\${{workdir}} \
          -sb \
          -m \
          -creds {params.credsfile} \
	  -lossy \
          -d 1
        """

        self.add_named_output("bam", self.sample_name + ".bam")
        self.add_named_output("bai", self.sample_name + ".bai")

        print("runtime: %s", runtime)
        print("params: %s", params)
        print("inputs: %s", inputs)
        print("outputs: %s", outputs)
        print("kwargs: %s", kwargs)
        return outputs

bunnies.unmarshall.register_kind(Align)


class Merge(bunnies.Transform):
    """
    merge one or more bam files and modify the readgroup with the
    provided information. bams are merged in the order provided.
    """
    MERGE_IMAGE = "rieseberglab/analytics:5-2.3.2"
    VERSION = "1"

    __slots__ = ("sample_name",)
    kind = "snpcalling.Merge"

    def __init__(self, sample_name=None, aligned_bams=None, manifest=None):
        super().__init__("merge", version=self.VERSION, image=self.MERGE_IMAGE)

        if manifest is not None:
            inputs, params = manifest.get('inputs', 'params')
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
