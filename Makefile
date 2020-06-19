GOTOOLS := github.com/golangci/golangci-lint/cmd/golangci-lint
VERSION := $(shell echo $(shell git describe --tags) | sed 's/^v//')
COMMIT := $(shell git log -1 --format='%H')
BRANCH=$(shell git rev-parse --abbrev-ref HEAD)
DOCKER_BUF := docker run -v $(shell pwd):/workspace --workdir /workspace bufbuild/buf
HTTPS_GIT := https://github.com/cosmos/iavl.git

PDFFLAGS := -pdf --nodefraction=0.1
CMDFLAGS := -ldflags -X TENDERMINT_IAVL_COLORS_ON=on 
LDFLAGS := -ldflags "-X github.com/tendermint/iavl.Version=$(VERSION) -X github.com/tendermint/iavl.Commit=$(COMMIT) -X github.com/tendermint/iavl.Branch=$(BRANCH)"

all: lint test install

install:
ifeq ($(COLORS_ON),)
	go install ./cmd/iaviewer
else
	go install $(CMDFLAGS) ./cmd/iaviewer
endif
.PHONY: install

test:
	@echo "--> Running go test"
	@go test ./... $(LDFLAGS) -v --race
.PHONY: test

tools:
	go get -v $(GOTOOLS)
.PHONY: tools

format:
	find . -name '*.go' -type f -not -path "*.git*" -not -name '*.pb.go' -not -name '*pb_test.go' | xargs gofmt -w -s
	find . -name '*.go' -type f -not -path "*.git*"  -not -name '*.pb.go' -not -name '*pb_test.go' | xargs goimports -w 
.PHONY: format

# look into .golangci.yml for enabling / disabling linters
lint:
	@echo "--> Running linter"
	@golangci-lint run
	@go mod verify
.PHONY: lint

# bench is the basic tests that shouldn't crash an aws instance
bench:
	cd benchmarks && \
		go test $(LDFLAGS) -bench=RandomBytes . && \
		go test $(LDFLAGS) -bench=Small . && \
		go test $(LDFLAGS) -bench=Medium . && \
		go test $(LDFLAGS) -bench=BenchmarkMemKeySizes .
.PHONY: bench

# fullbench is extra tests needing lots of memory and to run locally
fullbench:
	cd benchmarks && \
		go test $(LDFLAGS) -bench=RandomBytes . && \
		go test $(LDFLAGS) -bench=Small . && \
		go test $(LDFLAGS) -bench=Medium . && \
		go test $(LDFLAGS) -timeout=30m -bench=Large . && \
		go test $(LDFLAGS) -bench=Mem . && \
		go test $(LDFLAGS) -timeout=60m -bench=LevelDB .
.PHONY: fullbench

benchprune:
	cd benchmarks && \
		go test -bench=PruningStrategies -timeout=24h
.PHONY: benchprune

# note that this just profiles the in-memory version, not persistence
profile:
	cd benchmarks && \
		go test $(LDFLAGS) -bench=Mem -cpuprofile=cpu.out -memprofile=mem.out . && \
		go tool pprof ${PDFFLAGS} benchmarks.test cpu.out > cpu.pdf && \
		go tool pprof --alloc_space ${PDFFLAGS} benchmarks.test mem.out > mem_space.pdf && \
		go tool pprof --alloc_objects ${PDFFLAGS} benchmarks.test mem.out > mem_obj.pdf
.PHONY: profile

explorecpu:
	cd benchmarks && \
		go tool pprof benchmarks.test cpu.out
.PHONY: explorecpu

exploremem:
	cd benchmarks && \
		go tool pprof --alloc_objects benchmarks.test mem.out
.PHONY: exploremem

delve:
	dlv test ./benchmarks -- -test.bench=.
.PHONY: delve

proto-gen:
	@sh scripts/protocgen.sh
.PHONY: proto-gen

proto-lint:
	@$(DOCKER_BUF) check lint --error-format=json
.PHONY: proto-lint

proto-check-breaking:
	@$(DOCKER_BUF) check breaking --against-input $(HTTPS_GIT)#branch=master
.PHONY: proto-check-breaking


all: tools
.PHONY: all

tools: protobuf
.PHONY: tools

check: check_tools
.PHONY: check

check_tools:
	@# https://stackoverflow.com/a/25668869
	@echo "Found tools: $(foreach tool,$(notdir $(GOTOOLS)),\
        $(if $(shell which $(tool)),$(tool),$(error "No $(tool) in PATH")))"
.PHONY: check_tools

protobuf: $(PROTOBUF)
$(PROTOBUF):
	@echo "Get GoGo Protobuf"
	@go get github.com/gogo/protobuf/protoc-gen-gogofaster@v1.3.1
.PHONY: protobuf

tools-clean:
	rm -f $(CERTSTRAP) $(PROTOBUF) $(GOX) $(GOODMAN)
	rm -rf /usr/local/include/google/protobuf
	rm -f /usr/local/bin/protoc
.PHONY: tooks-clean

###
# Non Go tools
###

# Choose protobuf binary based on OS (only works for 64bit Linux and Mac).
# NOTE: On Mac, installation via brew (brew install protoc) might be favorable.
PROTOC_ZIP=""
ifneq ($(OS),Windows_NT)
		UNAME_S := $(shell uname -s)
		ifeq ($(UNAME_S),Linux)
			PROTOC_ZIP="protoc-3.10.1-linux-x86_64.zip"
		endif
		ifeq ($(UNAME_S),Darwin)
			PROTOC_ZIP="protoc-3.10.1-osx-x86_64.zip"
		endif
endif

protoc:
	@echo "Get Protobuf"
	@echo "In case of any errors, please install directly from https://github.com/protocolbuffers/protobuf/releases"
	@curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v3.10.1/$(PROTOC_ZIP)
	@unzip -o $(PROTOC_ZIP) -d /usr/local bin/protoc
	@unzip -o $(PROTOC_ZIP) -d /usr/local 'include/*'
	@rm -f $(PROTOC_ZIP)
.PHONY: protoc
