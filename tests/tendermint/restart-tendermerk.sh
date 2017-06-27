#!/bin/bash

echo "Restarting Merkleeyes and Tendermint"

merkleeyes start -d orphan-test-db >> merkleeyes.log &

tendermint node >> tendermint.log &

sleep 4

TPID=`pidof tendermint`
if [ -z "$TPID" ]; then
	tail tendermint.log
	exit 20
fi
