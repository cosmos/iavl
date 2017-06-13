#!/bin/bash

merkleeyes start -d orphan-test-db --address=tcp://127.0.0.1:46658 >> merkleeyes.log &

tendermint node >> tendermint.log &

sleep 4
