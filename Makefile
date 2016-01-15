.PHONY: all test get_deps

all: test install

install: get_deps
	go install github.com/tendermint/merkleeyes/cmd/...

test:
	go test github.com/tendermint/merkleeyes/...

get_deps:
	go get -d github.com/tendermint/merkleeyes/...
