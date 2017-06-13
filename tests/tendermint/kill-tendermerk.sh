#!/bin/bash

TPID=`pidof tendermint`
MPID=`pidof merkleeyes`

echo "Stopping Merkleeyes and Tendermint"

if [ -n "$TPID" ]; then 
    kill $TPID
fi

if [ -n "$MPID" ]; then 
    kill $MPID
fi
