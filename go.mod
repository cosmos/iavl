module github.com/cosmos/iavl

go 1.23

toolchain go1.23.1

require (
	cosmossdk.io/core v1.0.0
	github.com/cosmos/ics23/go v0.11.0
	github.com/emicklei/dot v1.6.4
	github.com/gogo/protobuf v1.3.2
	github.com/google/btree v1.1.3
	github.com/stretchr/testify v1.10.0
	github.com/syndtr/goleveldb v1.0.1-0.20210819022825-2ae1ddf74ef7
	go.uber.org/mock v0.5.0
)

require (
	github.com/cosmos/gogoproto v1.7.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/fsnotify/fsnotify v1.5.4 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/onsi/gomega v1.26.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/crypto v0.31.0 // indirect
	golang.org/x/sys v0.28.0 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

retract (
	v2.0.0-alpha.5 // not latest v2
	[v1.1.0, v1.1.3]
	[v1.0.0, v1.0.3]
	// This version is not used by the Cosmos SDK and adds a maintenance burden.
	// Use v1.x.x instead.
	[v0.21.0, v0.21.2]
	v0.18.0
)
