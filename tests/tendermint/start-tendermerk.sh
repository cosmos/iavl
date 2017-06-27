#!/bin/bash

./cleanup.sh

echo "Starting Merkleeyes and Tendermint"

merkleeyes start -d orphan-test-db >> merkleeyes.log &

rm -rf ~/.tendermint
tendermint init

tendermint node >> tendermint.log &

sleep 4

TPID=`pidof tendermint`
if [ -z "$TPID" ]; then
	tail tendermint.log
	exit 20
fi
