#!/bin/bash

set -euo pipefail
#set -x

function usage () {
    echo " Usage: $(basename "$0") --samplename SNAME [opts]* OUTPUTBAM [INPUTBAM]+ [INPUTBAI]*

  Merge input bam files INPUTBAM together into OUTPUTBAM, and verify
  read counts. The samplename of the readgroup information in the merged
  bam is updated to match the argument provided to --samplename.

  If a single input bam is provided, the output bam will be a symlink
  to the single input file.

  [opts] is one of:

    --help         show this text

    --dry-run      just print what will be done, without doing it

    --delete-old   delete input bam files after a merge is successful
                   (default is to keep them)

    --samplename NAME name to use in the manifest and headers

    --samtools SAMTOOLSBIN  path to samtools binary
    --sambamba SAMBIN  path to sambamba binary
    --picard   PICARDBIN path to picard jar file (gatk)
"
}

sambamba_cmd=${sambamba_cmd:-/usr/local/bin/sambamba_v0.6.6}
samtools_cmd=${samtools_cmd:-/usr/bin/samtools}
picard_bin=${picard_bin:-/gatk/gatk.jar}

# all files merged so far
#
# Format is:
#
# SAMPLEFOO /path/to/FILE1_FOO.BAM MERGE_OUTPUT_FOO.BAM
# SAMPLEFOO /path/to/FILE2_FOO.BAM MERGE_OUTPUT_FOO.BAM
# SAMPLEBAR /path/to/FILE1_BAR.BAM MERGE_OUTPUT_BAR.BAM
#...

# where temp bam files will be created.
# should be on the same filesystem as the new_bam_dir
tmp_bam_dir="scratch/tmp"

DRY_RUN=0
DELETE_OLD=0
TARGET_SAMPLE=""

#https://stackoverflow.com/questions/3963716/how-to-manually-expand-a-special-variable-ex-tilde-in-bash/27485157#27485157
function expand_home ()
{
    # ~foo/y => /home/foo/
    # ~/y    => /home/foo
    # blah   => blah
    (
	set +x
	local arg content content_quoted
	while read arg; do
	    case "$arg" in
		~*)
		    printf -v content_quoted "%q" "${arg:1}"
		    eval "content=~${content_quoted}"
		    printf '%s\n' "$content"
		    ;;
		*)
		    printf "%s\n" "$arg"
		    ;;
	    esac
	done
    )
}

function update_read_groups ()
{
    # SAMPLENAME INPLACEBAM
    local sname="$1"
    local inbam="$2"
    echo "listing read groups:"
    ${samtools_cmd} view -H "${inbam}" | grep @RG
    if ${samtools_cmd} view -H "${inbam}" | grep @RG | grep -q -v SM:"${sname}"; then
	echo "Updating read group in $inbam to SM:${sname}"
	time (${samtools_cmd} view -H "$inbam" | \
		     sed "s/\\bSM:[^\\t]*/SM:${sname}/g" | \
		     ${samtools_cmd} reheader - "$inbam" > "$inbam".tmp )
	mv "$inbam".tmp "$inbam" # overwrite
	rm "$inbam".bai || :
	time ${samtools_cmd} index "${inbam}"
    else
	echo "Readgroups match $sname already."
	if [[ ! -e "${inbam}.bai" ]]; then
	    time ${samtools_cmd} index "${inbam}"
	fi
    fi
}


POSARGS=()
while [[ "$#" -gt 0 ]]; do
    arg="$1"
    shift
    case "$arg" in
	--dry-run)
	    DRY_RUN=1
	    ;;
	--help|-h)
	    usage
	    exit 0;
	    ;;
	--sambamba)
	    sambamba_cmd="$1"
	    shift;
	    ;;
	--samtools)
	    samtools_cmd="$1"
	    shift;
	    ;;
	--picard)
	    picard_bin="$1"
	    shift;
	    ;;
	--samplename)
	    TARGET_SAMPLE="$1"
	    shift;
	    ;;
	--delete-old)
	    DELETE_OLD=1
	    ;;
	--tmpdir)
	    tmp_bam_dir="$1"
	    shift;
	    ;;
	-*)
	    echo "Invalid flag: $arg" >&2
	    exit 1
	    ;;
	*)
	    POSARGS+=("$arg")
	    ;;
    esac
done

if [[ "${#POSARGS[@]}" -lt 2 ]]; then
    echo "missing input and output bam files" >&2
    exit 1
fi

if [[ -z "$TARGET_SAMPLE" ]]; then
    echo "must specify a sample name" >&2
    exit 1
fi

outputbam="${POSARGS[0]}"
unset POSARGS[0]

declare -a target_bai target_bam
target_bam=( )
target_bai=( )

for posarg in "${POSARGS[@]}"; do
    case "$posarg" in
	*.bai|*.bai.*)
	    target_bai+=("$posarg")
	    ;;
	*.bam|*.bam.*)
	    target_bam+=("$posarg")
	    ;;
	*)
	    echo "unknown input format: $posarg" >&2
	    exit 1
	    ;;
    esac
done

new_bam_dir="$(dirname "${outputbam}")"
(
    echo "output file placed in directory ${new_bam_dir}"
    set -x
    mkdir -p "$new_bam_dir"
)

echo merging samples:
for bam in "${target_bam[@]}"; do
    echo "  ${bam}"
done

for bambai in "${target_bam[@]}" ${target_bai[@]+"${target_bai[@]}"}; do
    if [[ ! -s "${bambai}" ]]; then
	echo "input file missing: ${bambai}" >&2
	exit 1
    fi
done

if [[ "${#target_bam[@]}" -eq 0 ]]; then
    echo "No files to merge" >&2
    exit 1
fi

if [[ ${#target_bam[@]} -ne ${#target_bai[@]} ]]; then
    echo "the same number of bam and bai must be provided" >&2
    exit 1
fi

if [[ -s "${outputbam}" ]]
then
	echo "Output bam ${outputbam} already exists"
	exit 1
fi

if [[ "${DRY_RUN}" != "0" ]]; then
    echo "not merging. dry run."
    exit 0
fi

mkdir -p "${tmp_bam_dir}"
final_name="$(basename "${outputbam}")"
work_dir=$(mktemp -d -p "${tmp_bam_dir}" "tmp.lane_merger.${final_name}.XXXXXXXX")

# clean workdir on exit (success or not)
trap 'rm --one-file-system -r "${work_dir}"' EXIT

function markdup ()
{
    local infile="$1"
    local outfile="$2"
    local tmpdir="$3"

    java -jar "${picard_bin}" MarkDuplicates \
	 -I="$infile" \
	 -O="$outfile" \
	 -M="${outfile%%.bam}.dupmetrics.txt" \
	 --TMP_DIR="$tmpdir" \
	 --VALIDATION_STRINGENCY=LENIENT \
	 --CREATE_MD5_FILE=TRUE

    find "$(dirname "$outfile")"
}

if [[ "${#target_bam[@]}" -gt 1 ]]; then
    (
	set -x
	${sambamba_cmd} merge   -t 8 "${work_dir}/__merged__.bam" "${target_bam[@]}"
	markdup "${work_dir}/__merged__.bam" "${work_dir}/${final_name}" "${work_dir}"
	cat "${work_dir}/${final_name}".md5
	update_read_groups "${TARGET_SAMPLE}" "${work_dir}/${final_name}"
	${samtools_cmd} stats -d "${work_dir}/${final_name}" > "${work_dir}/${final_name}"stats.txt
    )

    echo "Files created in temp folder:"
    ls "${work_dir}"

    echo "Checking new bam to see if read sums match"
    #Now count reads to make sure it matches up.
    new_sum=$($sambamba_cmd view -t 8 -c "${work_dir}/${final_name}")
    echo "New bam has $new_sum reads"

    (
	old_sum=""
	for i in "${target_bam[@]}"; do
	    tmp_sum=$(set -x; $sambamba_cmd view -t 8 -c "${i}")
	    echo "$i has $tmp_sum reads"
	    old_sum=$((tmp_sum + old_sum))
	    echo "Running sum of old reads is $old_sum"
	done

	set -x;
	if [[ $new_sum -gt 0 ]]; then
	    if [[ $new_sum -eq $old_sum ]]; then
		echo "Read sums match"
		echo "Moving final file in place"
		mv -v -- \
		   "${work_dir}/${final_name}"{,.bai,.md5,stats.txt} \
		   "${work_dir}/${final_name%%.bam}".dupmetrics.txt \
		   "${new_bam_dir}"/

		if [[ "${DELETE_OLD}" -eq 1 ]]; then
		    echo "Read sums match. Deleting old files"
		    for i in "${target_bam[@]}"; do
			rm -f -v --one-file-system -- "$i"{,.bai}
		    done
		fi
	    else
		echo "Read sums don't match. Not deleting old files"
		false
	    fi
	fi
    )
else
    (
	set -x;
	echo "merging one file (trivial)..."
	ABSBAM="$(readlink -f -- "${target_bam[0]}")"
	ABSBAI="$(readlink -f -- "${target_bai[0]}")"
	echo "using symlink."
	ln -sfT "$ABSBAM" "${work_dir}/${final_name}"
	ln -sfT "$ABSBAI" "${work_dir}/${final_name}".bai

	update_read_groups "${TARGET_SAMPLE}" "${work_dir}/${final_name}"
	(cd "${work_dir}" && md5sum "${final_name}" > "${final_name}".md5; )
	${samtools_cmd} stats -d "${work_dir}/${final_name}" > "${work_dir}/${final_name}stats.txt"

	for final_file in "${work_dir}/${final_name}"{,.bai,.md5,stats.txt}; do
	    if [[ -f "${final_file}" || "${DELETE_OLD}" -eq 1 ]]; then
		real_file="$(readlink -f -- "${final_file}")"
		mv -- "${real_file}" "${new_bam_dir}/$(basename "${final_file}")"
	    elif [[ -e "${final_file}" ]]; then
		cp --preserve=timestamps -- "${final_file}" "${new_bam_dir}/"
	    fi
	done
    )
fi

echo "Completed merging ($outputbam $outputbam.bai)"
