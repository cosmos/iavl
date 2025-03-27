module github.com/cosmos/iavl/cmd

go 1.21
toolchain go1.23.4

require (
	cosmossdk.io/core v1.0.0
	cosmossdk.io/log v1.3.1
	github.com/cosmos/iavl v1.2.0
)

require (
	github.com/cosmos/gogoproto v1.7.0 // indirect
	github.com/cosmos/ics23/go v0.11.0 // indirect
	github.com/emicklei/dot v1.6.4 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/btree v1.1.3 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/rs/zerolog v1.32.0 // indirect
	github.com/syndtr/goleveldb v1.0.1-0.20210819022825-2ae1ddf74ef7 // indirect
	golang.org/x/crypto v0.31.0 // indirect
	golang.org/x/sys v0.28.0 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
)

replace github.com/cosmos/iavl => ../.
