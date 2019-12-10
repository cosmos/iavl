###
# Go tools
###

TOOLS_DESTDIR  ?= $(GOPATH)/bin

GOLANGCI_LINT   = $(TOOLS_DESTDIR)/golangci-lint

all: tools

tools: protoc gogo-protobuf golangci-lint protoc-gen-grpc-gateway protoc-gen-lint

gogo-protobuf: 
	@echo "Get GoGo Protobuf codegen tools"
	@go get github.com/gogo/protobuf/protoc-gen-gogo@v1.3.1

protoc-gen-grpc-gateway: 
	@echo "Get grpc-gateway gRPC codegen tools"
	@go get -u github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway

protoc-gen-lint:
	@echo "Get protoc-gen-lint protobuf gen linter"
	@go get -u github.com/ckaznocha/protoc-gen-lint

golangci-lint: $(GOLANGCI_LINT)
$(GOLANGCI_LINT):
	@echo "Get golangci-lint"
	@go get github.com/golangci/golangci-lint/cmd/golangci-lint

tools-clean:
	rm -f $(GOLANGCI_LINT)
	rm -f tools-stamp
	rm -rf $(HOME)/.local/include/google/protobuf
	rm -f $(HOME)/.local/bin/protoc

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
	@unzip -o $(PROTOC_ZIP) -d $(HOME)/.local bin/protoc
	@unzip -o $(PROTOC_ZIP) -d $(HOME)/.local 'include/*'
	@rm -f $(PROTOC_ZIP)

.PHONY: all tools tools-clean protoc gogo-protobuf golangci-lint protoc-gen-grpc-gateway protoc-gen-lint