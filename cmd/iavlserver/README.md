# IAVL gRPC Gateway and gRPC Server

The IAVL gRPC Gateway and gRPC Server provide a language agnostic interface to IAVL.

The gRPC Gateway translates a RESTful HTTP API into gRPC and proxies the request to the gRPC server.

Below is a brief introduction.

## Installation

```shell
go get github.com/tendermint/iavl
cd ${GOPATH}/src/github.com/tendermint/iavl
make get_vendor_deps
make install
```

## Using the tool

Please make sure it is properly installed and you have `${GOPATH}/bin` in your `PATH`.
Typing `iavlserver -h` should print out the following usage message:

```
$ iavlserver -h
Usage of iavlserver:
  -cache-size int
        Tree cache size (default 10000)
  -cpuprofile string
        If set, write CPU profile to this file
  -datadir string
        The database data directory
  -db-backend string
        The database backend (default "goleveldb")
  -db-name string
        The database name
  -gateway-endpoint string
        The gRPC-Gateway server endpoint (host:port) (default "localhost:8091")
  -grpc-endpoint string
        The gRPC server endpoint (host:port) (default "localhost:8090")
  -memprofile string
        If set, write memory profile to this file
  -no-gateway
        Disables the gRPC-Gateway server
  -version int
        The IAVL version to load
```

### Example 

Below is an example to get the gRPC gateway and server running.

Run the following command to start the gRPC server and gateway:

```shell
mkdir -p tmp/datadir
iavlserver -db-name "example-db" -datadir ./tmp
```

Once it is up and running you can test it is working by running:

```shell
curl http://localhost:8091/v1/tree/version
```

The result should be:

```shell
$ curl http://localhost:8091/v1/tree/version
{
  "version": "0"
}
```
