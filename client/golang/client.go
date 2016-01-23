package mecli

import (
	"net"

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

func (client *MEClient) GetSync(key []byte) ([]byte, error) {
	return nil, nil
}

func (client *MEClient) SetSync(key []byte, value []byte) error {
	return nil
}

func (client *MEClient) RemSync(key []byte) error {
	return nil
}
