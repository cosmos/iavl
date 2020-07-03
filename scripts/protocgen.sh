#!/usr/bin/env bash

set -eo pipefail

proto_dirs=$(find ./internal/proto -path -prune -o -name '*.proto' -print0 | xargs -0 -n1 dirname | sort | uniq)
for dir in $proto_dirs; do
  protoc \
  -I "internal/proto" \
  --gogofaster_out=$dir \
  $(find "${dir}" -maxdepth 1 -name '*.proto')
done
