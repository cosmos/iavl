package testutil

import (
	"github.com/tendermint/merkleeyes/app"
	eyes "github.com/tendermint/merkleeyes/client"
	"github.com/tendermint/tmsp/server"
	"testing"
)

// NOTE: don't forget to close the client & server.
func CreateEyes(t *testing.T) (svr *server.Server, cli *eyes.Client) {
	addr := "unix://eyes.sock"

	// Start the listener
	mApp := app.NewMerkleEyesApp()
	svr, err := server.NewServer(addr, mApp)
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
