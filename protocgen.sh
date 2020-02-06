#!/usr/bin/env bash

set -eo pipefail

proto_dirs=$(find . -path ./third_party -prune -o -name '*.proto' -print0 | xargs -0 -n1 dirname | sort | uniq)
for dir in $proto_dirs; do
  protoc \
  -I. \
  --go_out=plugins=grpc,paths=source_relative:. \
  --grpc-gateway_out=logtostderr=true:. \
  $(find "${dir}" -name '*.proto')
done 