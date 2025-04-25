#!/usr/bin/bash
# simply check all the files in data/epf

set -e

DIR=$(pwd)
for f in $(find data/epf -name '*.md5'); do
  d="$(dirname "$f")"
  cd "${d}"
  printf "%s/%s\n" "${d}" "$(md5sum -c "$(basename "${f}")")"
  cd "${DIR}"
done
