package mecli

import (
	"errors"
	"net"

	"github.com/tendermint/go-wire"
	tmspcli "github.com/tendermint/tmsp/client/golang"
)

type MEClient struct {
	*tmspcli.TMSPClient
}

func NewMEClient(conn net.Conn, bufferSize int) *MEClient {
	client := &MEClient{
		TMSPClient: tmspcli.NewTMSPClient(conn, bufferSize),
	}
	return client
}

func (client *MEClient) GetSync(key []byte) (idx int, value []byte, exists bool, err error) {
	query := make([]byte, 1+wire.ByteSliceSize(key))
	buf := query
	buf[0] = 0x01 // Get TypeByte
	buf = buf[1:]
	wire.PutByteSlice(buf, key)
	result, err := client.TMSPClient.QuerySync(query)
	if err != nil {
		return
	}
	idx, n, err := wire.GetVarint(result)
	if err != nil {
		return
	}
	result = result[n:]
	value, n, err = wire.GetByteSlice(result)
	if err != nil {
		return
	}
	result = result[n:]
	exists, err = wire.GetBool(result)
	if err != nil {
		return
	}
	result = result[1:]
	if len(result) != 0 {
		err = errors.New("Result too short for GetSync")
		return
	}
	return
}

func (client *MEClient) SetSync(key []byte, value []byte) (err error) {
	tx := make([]byte, 1+wire.ByteSliceSize(key)+wire.ByteSliceSize(value))
	buf := tx
	buf[0] = 0x01 // Set TypeByte
	buf = buf[1:]
	n, err := wire.PutByteSlice(buf, key)
	if err != nil {
		return
	}
	buf = buf[n:]
	n, err = wire.PutByteSlice(buf, value)
	if err != nil {
		return
	}
	err = client.TMSPClient.AppendTxSync(tx)
	return err
}

func (client *MEClient) RemSync(key []byte) (err error) {
	tx := make([]byte, 1+wire.ByteSliceSize(key))
	buf := tx
	buf[0] = 0x02 // Rem TypeByte
	buf = buf[1:]
	_, err = wire.PutByteSlice(buf, key)
	if err != nil {
		return
	}
	err = client.TMSPClient.AppendTxSync(tx)
	return err
}
