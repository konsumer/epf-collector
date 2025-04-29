#!/bin/bash

# Some of the data has bad characters in lines. This will build a smaller test-set for those (so we can reverse it)

mkdir -p data/test

dir=$(pwd)

for file in $(find data/epf -name '*.tbz'); do
  pushd data/test
  table=$(basename "${file}" .tbz)
  echo "Creating test-set for ${table}"
  bunzip2 -c "${dir}/${file}" | node "${dir}/lib/testset.js" | bzip2 -c > "${table}.tbz"
  popd
done
