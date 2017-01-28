package testutil

import (
	"testing"

	"github.com/tendermint/abci/server"
	. "github.com/tendermint/go-common"
	"github.com/tendermint/merkleeyes/app"
	eyes "github.com/tendermint/merkleeyes/client"
)

// NOTE: don't forget to close the client & server.
func CreateEyes(t *testing.T) (svr Service, cli *eyes.Client) {
	addr := "unix://eyes.sock"

	// Start the listener
	mApp := app.NewMerkleEyesApp("", 0)
	svr, err := server.NewServer(addr, "socket", mApp)
	if err != nil {
		(err.Error())
		return
	}

	// Create client
	cli, err = eyes.NewClient(addr)
	if err != nil {
		t.Fatal(err.Error())
		return
	}

	return svr, cli
}
