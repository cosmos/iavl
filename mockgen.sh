#!/usr/bin/env bash

mockgen_cmd="mockgen"
$mockgen_cmd -package mock -destination mock/db_mock.go github.com/tendermint/tm-db DB,Iterator,Batch