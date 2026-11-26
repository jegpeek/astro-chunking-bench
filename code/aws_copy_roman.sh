#!/bin/bash
# Copy the 18 Roman I-Sim L2 cal.asdf files from STScI's public bucket (us-east-1, anonymous reads)
# into the private Ohio bucket, streaming (no local disk). Usage: aws_copy_roman.sh <bucket> <file-list>
set -euo pipefail
B=$1; LIST=$2; SRC=s3://stpubdata/roman/nexus/soc_simulations/r00342/l2
n=0
while read -r f; do
  [ -z "$f" ] && continue
  if aws s3api head-object --region us-east-2 --bucket "$B" --key "roman_l2/$f" >/dev/null 2>&1; then n=$((n+1)); continue; fi
  aws s3 cp --no-sign-request --region us-east-1 "$SRC/$f" - | aws s3 cp - "s3://$B/roman_l2/$f" --region us-east-2 --expected-size 220000000
  n=$((n+1))
done < "$LIST"
echo "copied/present: $n files"; aws s3 ls "s3://$B/roman_l2/" --region us-east-2 --summarize | tail -2
