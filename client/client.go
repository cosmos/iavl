package eyes

import (
	"github.com/tendermint/go-wire"
	tmspcli "github.com/tendermint/tmsp/client"
	tmsp "github.com/tendermint/tmsp/types"
)

type Client struct {
	*tmspcli.Client
}

func NewClient(addr string) (*Client, error) {
	tmspClient, err := tmspcli.NewClient(addr, false)
	if err != nil {
		return nil, err
	}
	client := &Client{
		Client: tmspClient,
	}
	return client, nil
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
	result := res.Data
	value, n, err := wire.GetByteSlice(result)
	if err != nil {
		return tmsp.ErrInternalError.SetLog("decoding value byteslice: " + err.Error())
	}
	result = result[n:]
	if len(result) != 0 {
		return tmsp.ErrInternalError.SetLog("Got unexpected trailing bytes")
	}
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
