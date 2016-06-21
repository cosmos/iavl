package eyes

import (
	"github.com/tendermint/go-wire"
	"github.com/tendermint/merkleeyes/app"
	tmspcli "github.com/tendermint/tmsp/client"
	tmsp "github.com/tendermint/tmsp/types"
)

type Client struct {
	tmspcli.Client
}

// 'addr': if "local", will start an embedded one.
func NewClient(addr, tmsp string) (*Client, error) {
	if addr == "local" {
		return NewLocalClient(), nil
	}
	tmspClient, err := tmspcli.NewClient(addr, tmsp, false)
	if err != nil {
		return nil, err
	}
	client := &Client{
		Client: tmspClient,
	}
	return client, nil
}

func NewLocalClient() *Client {
	eyesApp := app.NewMerkleEyesApp()
	tmspClient := tmspcli.NewLocalClient(nil, eyesApp)
	return &Client{
		Client: tmspClient,
	}
}

func (client *Client) GetSync(key []byte) (res tmsp.Result) {
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
	return tmsp.NewResultOK(value, "")
}

func (client *Client) SetSync(key []byte, value []byte) (res tmsp.Result) {
	tx := make([]byte, 1+wire.ByteSliceSize(key)+wire.ByteSliceSize(value))
	buf := tx
	buf[0] = 0x01 // Set TypeByte
	buf = buf[1:]
	n, err := wire.PutByteSlice(buf, key)
	if err != nil {
		return tmsp.ErrInternalError.SetLog("encoding key byteslice: " + err.Error())
	}
	buf = buf[n:]
	n, err = wire.PutByteSlice(buf, value)
	if err != nil {
		return tmsp.ErrInternalError.SetLog("encoding value byteslice: " + err.Error())
	}
	return client.AppendTxSync(tx)
}

func (client *Client) RemSync(key []byte) (res tmsp.Result) {
	tx := make([]byte, 1+wire.ByteSliceSize(key))
	buf := tx
	buf[0] = 0x02 // Rem TypeByte
	buf = buf[1:]
	_, err := wire.PutByteSlice(buf, key)
	if err != nil {
		return tmsp.ErrInternalError.SetLog("encoding key byteslice: " + err.Error())
	}
	return client.AppendTxSync(tx)
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
