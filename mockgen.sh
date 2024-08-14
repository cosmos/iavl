#!/usr/bin/env bash

mockgen_cmd="mockgen"
$mockgen_cmd -package mock -destination mock/db_mock.go  -source ./db/types.go DB 
$mockgen_cmd -package mock -destination mock/store_mock.go cosmossdk.io/core/store Iterator,Batch 
