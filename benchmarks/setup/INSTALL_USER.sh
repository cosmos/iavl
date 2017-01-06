#!/bin/bash

cat <<'EOF' > ~/.goenv
export GOROOT=/usr/local/go
export GOPATH=$HOME/go
export PATH=$GOPATH/bin:$GOROOT/bin:$PATH
EOF

. ~/.goenv

mkdir -p $GOPATH/src/github.com/tendermint
MERKLE=$GOPATH/src/github.com/tendermint/go-merkle
git clone https://github.com/ethanfrey/go-merkle.git $MERKLE
cd $MERKLE
git checkout benchmarking
