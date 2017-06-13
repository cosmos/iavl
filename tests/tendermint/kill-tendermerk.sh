#!/bin/bash

TPID=`pidof tendermint`
MPID=`pidof merkleeyes`

if [ -n "$TPID" ]; then 
    kill $TPID
fi

if [ -n "$MPID" ]; then 
    kill $MPID
fi
