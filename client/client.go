package eyes

import (
	abcicli "github.com/tendermint/abci/client"
	abci "github.com/tendermint/abci/types"
	"github.com/tendermint/go-wire"
	"github.com/tendermint/merkleeyes/app"
)

type Client struct {
	abcicli.Client
}

// 'addr': if "local", will start an embedded one.
func NewClient(addr, abci string) (*Client, error) {
	if addr == "local" {
		return NewLocalClient(), nil
	}
	abciClient, err := abcicli.NewClient(addr, abci, false)
	if err != nil {
		return nil, err
	}
	client := &Client{
		Client: abciClient,
	}
	return client, nil
}

func NewLocalClient() *Client {
	eyesApp := app.NewMerkleEyesApp()
	abciClient := abcicli.NewLocalClient(nil, eyesApp)
	return &Client{
		Client: abciClient,
	}
}

func (client *Client) GetSync(key []byte) (res abci.Result) {
	query := make([]byte, 1+wire.ByteSliceSize(key))
	buf := query
	buf[0] = 0x01 // Get TypeByte
	buf = buf[1:]
	wire.PutByteSlice(buf, key)
	res = client.QuerySync(query)
	if res.IsErr() {
		return res
	}
	value := res.Data
	return abci.NewResultOK(value, "")
}

func (client *Client) SetSync(key []byte, value []byte) (res abci.Result) {
	tx := make([]byte, 1+wire.ByteSliceSize(key)+wire.ByteSliceSize(value))
	buf := tx
	buf[0] = 0x01 // Set TypeByte
	buf = buf[1:]
	n, err := wire.PutByteSlice(buf, key)
	if err != nil {
		return abci.ErrInternalError.SetLog("encoding key byteslice: " + err.Error())
	}
	buf = buf[n:]
	n, err = wire.PutByteSlice(buf, value)
	if err != nil {
		return abci.ErrInternalError.SetLog("encoding value byteslice: " + err.Error())
	}
	return client.DeliverTxSync(tx)
}

func (client *Client) RemSync(key []byte) (res abci.Result) {
	tx := make([]byte, 1+wire.ByteSliceSize(key))
	buf := tx
	buf[0] = 0x02 // Rem TypeByte
	buf = buf[1:]
	_, err := wire.PutByteSlice(buf, key)
	if err != nil {
		return abci.ErrInternalError.SetLog("encoding key byteslice: " + err.Error())
	}
	return client.DeliverTxSync(tx)
}

//----------------------------------------
// Convenience

func (client *Client) Get(key []byte) (value []byte) {
	res := client.GetSync(key)
	if res.IsErr() {
		panic(res.Error())
	}
	return res.Data
}

func (client *Client) Set(key []byte, value []byte) {
	res := client.SetSync(key, value)
	if res.IsErr() {
		panic(res.Error())
	}
}
