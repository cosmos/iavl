#!/bin/sh

curl http://localhost:46657/status 2>/dev/null | jq '.result | .latest_block_height, .latest_block_hash, .latest_app_hash'

# store query for consistent parsing
curl http://localhost:46657/abci_query\?data\=0x1234\&path\=\"/key\"\&prove=true 2>/dev/null > query.txt

# get the apphash from the proof
cat query.txt | jq '.result.response.proof' | cut -c8-47

# get the height
cat query.txt | jq '.result.response'
