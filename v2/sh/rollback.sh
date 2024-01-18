#!/usr/bin/env bash

set -o errexit
set -o nounset

db_path=$1
VERSION=1

find "$db_path" -type d -mindepth 1 | while read -r dir; do
  echo "Rolling back $dir back to $VERSION"
  sqlite3 "$dir"/changelog.sqlite "delete from leaf where version > $VERSION;"
  sqlite3 "$dir"/changelog.sqlite "delete from leaf_delete where version > $VERSION;"
  sqlite3 "$dir"/tree.sqlite "delete from root where version > $VERSION;"
done