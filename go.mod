module github.com/cosmos/iavl

go 1.20

require (
	cosmossdk.io/log v1.2.0
	github.com/confio/ics23/go v0.7.0
	github.com/emicklei/dot v1.4.2
	github.com/golang/mock v1.6.0
	github.com/stretchr/testify v1.8.4
	github.com/tendermint/tm-db v0.6.6
	golang.org/x/crypto v0.12.0
	google.golang.org/protobuf v1.30.0
)

require (
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/cosmos/gogoproto v1.4.3 // indirect
	github.com/cosmos/gorocksdb v1.2.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgraph-io/badger/v3 v3.2103.2 // indirect
	github.com/dgraph-io/ristretto v0.1.0 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/fsnotify/fsnotify v1.5.4 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b // indirect
	github.com/golang/groupcache v0.0.0-20190702054246-869f871628b6 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/btree v1.1.2 // indirect
	github.com/google/flatbuffers v1.12.1 // indirect
	github.com/jmhodges/levigo v1.0.0 // indirect
	github.com/klauspost/compress v1.12.3 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/onsi/gomega v1.26.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rs/zerolog v1.30.0 // indirect
	github.com/syndtr/goleveldb v1.0.1-0.20210819022825-2ae1ddf74ef7 // indirect
	go.etcd.io/bbolt v1.3.6 // indirect
	go.opencensus.io v0.22.5 // indirect
	golang.org/x/net v0.10.0 // indirect
	golang.org/x/sys v0.11.0 // indirect
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

retract (
	// This version is not used by the Cosmos SDK and adds a maintenance burden.
	// Use v1.x.x instead.
	[v0.21.0, v0.21.2]
	v0.18.0
)

replace github.com/tendermint/tm-db => /Users/kartikbhat/tm-db
