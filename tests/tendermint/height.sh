#!/bin/sh

curl http://localhost:46657/status 2>/dev/null | jq .result.latest_block_height; 

curl curl http://localhost:46657/abci_query\?data\=0x1234\&path\=\"/key\" 2>/dev/null | jq .result.response

