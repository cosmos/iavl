#!/usr/bin/env bash

set -eo pipefail

proto_dirs=$(find . -path ./third_party -prune -o -name '*.proto' -print0 | xargs -0 -n1 dirname | sort | uniq)
for dir in $proto_dirs; do
  echo $dir
  protoc \
  -I. \
  --go_out=plugins=grpc,paths=source_relative:. \
  --grpc-gateway_out=logtostderr=true,paths=source_relative:. \
  $(find "${dir}" -name '*.proto')
done 

#mv ./internal/proto/iavl/* ./internal/proto
#rm -rf ./internal/proto/iavl
