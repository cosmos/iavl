package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	. "github.com/tendermint/go-common"
	"github.com/tendermint/merkleeyes/app"
	eyes "github.com/tendermint/merkleeyes/client"
	"github.com/tendermint/tmsp/server"
)

var tmspType = "socket"

func TestClient(t *testing.T) {

	addr := "tcp://127.0.0.1:46659"

	// Start the listener
	mApp := app.NewMerkleEyesApp()
	s, err := server.NewServer(addr, tmspType, mApp)
	if err != nil {
		t.Fatal(err.Error())
		return
	}
	defer s.Stop()

	// Create client
	cli, err := eyes.NewClient(addr, tmspType)
	if err != nil {
		t.Fatal(err.Error())
		return
	}
	defer cli.Stop()

	// Empty
	commit(t, cli, "")
	get(t, cli, "foo", "", "")
	get(t, cli, "bar", "", "")
	// Set foo=FOO
	set(t, cli, "foo", "FOO")
	commit(t, cli, "68DECA470D80183B5E979D167E3DD0956631A952")
	get(t, cli, "foo", "FOO", "")
	get(t, cli, "foa", "", "")
	get(t, cli, "foz", "", "")
	rem(t, cli, "foo")
	// Not empty until committed...
	get(t, cli, "foo", "FOO", "")
	commit(t, cli, "")
	get(t, cli, "foo", "", "")
	commit(t, cli, "")
	// Set foo1, foo2, foo3...
	set(t, cli, "foo1", "1")
	set(t, cli, "foo2", "2")
	set(t, cli, "foo3", "3")
	set(t, cli, "foo1", "4")
	// nothing commited yet...
	get(t, cli, "foo1", "", "")
	commit(t, cli, "45B7F856A16CB2F8BB9A4A25587FC71D062BD631")
	// now we got info
	get(t, cli, "foo1", "4", "")
	get(t, cli, "foo2", "2", "")
	get(t, cli, "foo3", "3", "")
	rem(t, cli, "foo3")
	rem(t, cli, "foo2")
	rem(t, cli, "foo1")
	// Empty
	commit(t, cli, "")
}

func get(t *testing.T, cli *eyes.Client, key string, value string, err string) {
	res := cli.GetSync([]byte(key))
	val := []byte(nil)
	if value != "" {
		val = []byte(value)
	}
	require.EqualValues(t, val, res.Data)
	if res.IsOK() {
		assert.Equal(t, "", err)
	} else {
		assert.NotEqual(t, "", err)
	}
}

func set(t *testing.T, cli *eyes.Client, key string, value string) {
	cli.SetSync([]byte(key), []byte(value))
}

func rem(t *testing.T, cli *eyes.Client, key string) {
	cli.RemSync([]byte(key))
}

func commit(t *testing.T, cli *eyes.Client, hash string) {
	res := cli.CommitSync()
	require.False(t, res.IsErr(), res.Error())
	assert.Equal(t, hash, Fmt("%X", res.Data))
}
