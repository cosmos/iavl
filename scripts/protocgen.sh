#!/usr/bin/env bash

set -eo pipefail

proto_dirs=$(find . -path ./third_party -prune -o -name '*.proto' -print0 | xargs -0 -n1 dirname | sort | uniq)
for dir in $proto_dirs; do
  echo $dir
  protoc \
  --proto_path=third_party \
  --proto_path=proto \
  --gogofaster_out=plugins=grpc,paths=source_relative:./proto \
  --grpc-gateway_out=logtostderr=true,paths=source_relative:./proto \
  $(find "${dir}" -name '*.proto')
done 

mv ./proto/iavl/*.go ./proto
