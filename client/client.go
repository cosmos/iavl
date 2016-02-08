package eyes

import (
	"errors"
	"fmt"

	"github.com/tendermint/go-wire"
	tmspcli "github.com/tendermint/tmsp/client"
	tmsp "github.com/tendermint/tmsp/types"
)

type MerkleEyesClient struct {
	*tmspcli.TMSPClient
}

func NewMerkleEyesClient(addr string) (*MerkleEyesClient, error) {
	tmspClient, err := tmspcli.NewTMSPClient(addr)
	if err != nil {
		return nil, err
	}
	client := &MerkleEyesClient{
		TMSPClient: tmspClient,
	}
	return client, nil
}

func (client *MerkleEyesClient) GetSync(key []byte) (value []byte, err error) {
	query := make([]byte, 1+wire.ByteSliceSize(key))
	buf := query
	buf[0] = 0x01 // Get TypeByte
	buf = buf[1:]
	wire.PutByteSlice(buf, key)
	code, result, _, err := client.TMSPClient.QuerySync(query)
	if err != nil {
		return
	}
	if code != tmsp.CodeType_OK {
		return nil, fmt.Errorf("Got unexpected code %v", code)
	}
	value, n, err := wire.GetByteSlice(result)
	if err != nil {
		return
	}
	result = result[n:]
	if len(result) != 0 {
		err = errors.New("Result too short for GetSync")
		return
	}
	return
}

func (client *MerkleEyesClient) SetSync(key []byte, value []byte) (err error) {
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
	_, _, _, err = client.TMSPClient.AppendTxSync(tx)
	return err
}

func (client *MerkleEyesClient) RemSync(key []byte) (err error) {
	tx := make([]byte, 1+wire.ByteSliceSize(key))
	buf := tx
	buf[0] = 0x02 // Rem TypeByte
	buf = buf[1:]
	_, err = wire.PutByteSlice(buf, key)
	if err != nil {
		return
	}
	_, _, _, err = client.TMSPClient.AppendTxSync(tx)
	return err
}
