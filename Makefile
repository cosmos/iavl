VERSION := $(shell echo $(shell git describe --tags) | sed 's/^v//')
COMMIT := $(shell git log -1 --format='%H')
BRANCH=$(shell git rev-parse --abbrev-ref HEAD)

PDFFLAGS := -pdf --nodefraction=0.1
CMDFLAGS := -ldflags -X TENDERMINT_IAVL_COLORS_ON=on 
LDFLAGS  := -ldflags "-X github.com/tendermint/iavl.Version=$(VERSION) -X github.com/tendermint/iavl.Commit=$(COMMIT) -X github.com/tendermint/iavl.Branch=$(BRANCH)"


all: lint test install

include tools.mk

install:
ifeq ($(COLORS_ON),)
	go install ./cmd/iaviewer
	go install ./cmd/iavlserver
else
	go install $(CMDFLAGS) ./cmd/iaviewer
	go install $(CMDFLAGS) ./cmd/iavlserver
endif

test:
	@echo "--> Running go test"
	@go test ./... $(LDFLAGS) -v --race

# look into .golangci.yml for enabling / disabling linters
lint:
	@echo "--> Running linter"
	@golangci-lint run
	@go mod verify

# bench is the basic tests that shouldn't crash an aws instance
bench:
	cd benchmarks && \
		go test $(LDFLAGS) -bench=RandomBytes . && \
		go test $(LDFLAGS) -bench=Small . && \
		go test $(LDFLAGS) -bench=Medium . && \
		go test $(LDFLAGS) -bench=BenchmarkMemKeySizes .

# fullbench is extra tests needing lots of memory and to run locally
fullbench:
	cd benchmarks && \
		go test $(LDFLAGS) -bench=RandomBytes . && \
		go test $(LDFLAGS) -bench=Small . && \
		go test $(LDFLAGS) -bench=Medium . && \
		go test $(LDFLAGS) -timeout=30m -bench=Large . && \
		go test $(LDFLAGS) -bench=Mem . && \
		go test $(LDFLAGS) -timeout=60m -bench=LevelDB .


# note that this just profiles the in-memory version, not persistence
profile:
	cd benchmarks && \
		go test $(LDFLAGS) -bench=Mem -cpuprofile=cpu.out -memprofile=mem.out . && \
		go tool pprof ${PDFFLAGS} benchmarks.test cpu.out > cpu.pdf && \
		go tool pprof --alloc_space ${PDFFLAGS} benchmarks.test mem.out > mem_space.pdf && \
		go tool pprof --alloc_objects ${PDFFLAGS} benchmarks.test mem.out > mem_obj.pdf

explorecpu:
	cd benchmarks && \
		go tool pprof benchmarks.test cpu.out

exploremem:
	cd benchmarks && \
		go tool pprof --alloc_objects benchmarks.test mem.out

delve:
	dlv test ./benchmarks -- -test.bench=.

protogen:
	protoc -I/usr/local/include -I. \
	-I$(GOPATH)/src \
	-I$(GOPATH)/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis \
	--go_out=plugins=grpc:. \
	--grpc-gateway_out=logtostderr=true:. \
	proto/iavl_api.proto

protolint:
	protoc -I/usr/local/include -I. \
	-I$(GOPATH)/src \
	-I$(GOPATH)/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis \
	--lint_out=. \
	proto/iavl_api.proto

.PHONY: lint test tools install delve exploremem explorecpu profile fullbench bench protogen protolint
