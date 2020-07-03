#!/usr/bin/env bash

set -eo pipefail

proto_dirs=$(find ./proto -path -prune -o -name '*.proto' -print0 | xargs -0 -n1 dirname | sort | uniq)
for dir in $proto_dirs; do
  protoc \
  -I "proto" \
  --gogofaster_out=source_relative:./internal/proto \
  $(find "${dir}" -maxdepth 1 -name '*.proto')
done

mv ./internal/proto/iavl/* ./internal/proto
rm -rf ./internal/proto/iavl
