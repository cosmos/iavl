.PHONY: all test get_deps

all: test install

install: 
	go install github.com/tendermint/merkleeyes/cmd/...

test:
	go test --race github.com/tendermint/merkleeyes/...

get_deps:
	go get -d github.com/tendermint/merkleeyes/...

get_vendor_deps:
	go get github.com/Masterminds/glide
	glide install
