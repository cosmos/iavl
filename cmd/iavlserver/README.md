# IAVL gRPC Gateway and gRPC Server

The IAVL gRPC Gateway and gRPC Server provide a language agnostic interface to IAVL.

The gRPC Gateway translates a RESTful HTTP API into gRPC and proxies the request to the gRPC server.

Below is a brief introduction.

## Installation

```shell
go get github.com/tendermint/iavl
cd ${GOPATH}/src/github.com/tendermint/iavl
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
curl http://localhost:8091/v1/version
```

The result should be:

```shell
$ curl http://localhost:8091/v1/version
{
  "version": "0"
}
```

We can also test a simple `set`/`get`:

```shell
curl -XPOST http://localhost:8091/v1/set -d '{"key": "'$(echo -n foo | base64)'", "value": "'$(echo -n bar | base64)'"}'
```

You should see

```shell
{
  "updated": false
}
```

where the `updated` field indicates that we did not overwrite a value, which makes sense for our fresh database. If
you want to check tha saved key/value pair, you can make use the `get` method

```shell
curl "http://localhost:8091/v1/get?key=$(echo -n foo | base64)"
```

and should see the response

```shell
{
  "index": "0",
  "value": "YmFy"
}
```

where `"YmFy"` is the base64 encoding of `"bar"`. If you would like to commit this version of the database, you can submit
a `save_version` request like

```shell
curl -XPOST "http://localhost:8091/v1/save_version"
```

which should yield the response

{
  "root_hash": "Xv1EBVNQtcw029JghTR6nbvkTqGSuShqn8EH9A6h+sU=",
  "version": "1"
}
```

This indicates that the version of the database just saved is `1` with root hash `"Xv1E...`. If you would like to view all
of the saved versions, you can use the `available_versions` method:

```shell
curl -XGET "http://localhost:8091/v1/available_versions"
```

which returns a list of all of the saved versions

```shell
{
  "versions": [
    "1"
  ]
}
```                                                          
