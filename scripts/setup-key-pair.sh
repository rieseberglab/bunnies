#!/usr/bin/env bash

#
# Creates ../key-pair.pem (readonly)
# Creates ../key-pair-settings.json  containing keyname and fingerprint
#

KEYNAME="${KEYNAME:-reprodkey}"
SCRIPTSDIR=$(readlink -f "$(dirname "$0")")
REPODIR=$(readlink -f "$(dirname "$0")/..")

pemkey="$REPODIR/key-pair.pem"
jsonkey="$REPODIR/key-pair-settings.json"

set -exo pipefail

function cleanup ()
{
    if [[ -d "$workdir" ]]; then rm -rf --one-file-system -- "$workdir" || :; fi
}
trap cleanup EXIT

workdir=$(mktemp -d "keypair.XXXXX")

if [[ -f "$pemkey" ]]; then
    echo "output file $pemkey already exists." >&2
    exit 1
fi

if aws ec2 describe-key-pairs --key-names "$KEYNAME" > "$workdir"/keypairs.json; then
    echo "keypair already exists. To delete, invoke:

     aws ec2 delete-key-pair --key-name '$KEYNAME'
" >&2
    exit 1
fi

aws ec2 create-key-pair --key-name "$KEYNAME" > "$workdir/key.json"
"$SCRIPTSDIR/read_json_key" KeyMaterial < "$workdir/key.json" > "$pemkey"
aws ec2 describe-key-pairs --key-names "$KEYNAME" | "$SCRIPTSDIR"/read_json_key KeyPairs | head -n 1 > "$jsonkey"
chmod 400 "$pemkey"
