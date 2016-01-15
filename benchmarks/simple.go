package main

import (
	"fmt"
	"net"
	//"encoding/hex"

	. "github.com/tendermint/go-common"
	"github.com/tendermint/go-wire"
	_ "github.com/tendermint/merkleeyes/types"
	"github.com/tendermint/tmsp/types"
)

func main() {

	conn, err := Connect("tcp://127.0.0.1:46659")
	if err != nil {
		Exit(err.Error())
	}

	// Make a bunch of requests
	counter := 0
	for i := 0; ; i++ {
		req := types.RequestEcho{"foobar"}
		_, err := makeRequest(conn, req)
		if err != nil {
			Exit(err.Error())
		}
		counter += 1
		if counter%1000 == 0 {
			fmt.Println(counter)
		}
	}
}

func makeRequest(conn net.Conn, req types.Request) (types.Response, error) {
	var n int
	var err error

	// Write desired request
	wire.WriteBinaryLengthPrefixed(struct{ types.Request }{req}, conn, &n, &err)
	if err != nil {
		return nil, err
	}

	// Read desired response
	var res types.Response
	wire.ReadBinaryPtrLengthPrefixed(&res, conn, 0, &n, &err)
	if err != nil {
		return nil, err
	}

	return res, nil
}
