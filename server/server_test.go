package server_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
	dbm "github.com/tendermint/tm-db"

	pb "github.com/tendermint/iavl/proto"
	"github.com/tendermint/iavl/server"
)

type ServerTestSuite struct {
	suite.Suite

	server *server.IAVLServer
}

func (suite *ServerTestSuite) SetupTest() {
	db := dbm.NewDB("test", dbm.MemDBBackend, "")

	server, err := server.New(db, 1000, 0)
	suite.NoError(err)

	suite.server = server
	suite.populateItems(100)
}

func (suite *ServerTestSuite) populateItems(n int) {
	versionRes, err := suite.server.Version(context.TODO(), nil)
	suite.NoError(err)

	for i := 0; i < n; i++ {
		req := &pb.SetRequest{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}

		_, err := suite.server.Set(context.TODO(), req)
		suite.NoError(err)
	}

	res, err := suite.server.SaveVersion(context.TODO(), nil)
	suite.NoError(err)
	suite.Equal(versionRes.Version+1, res.Version)
}

func (suite *ServerTestSuite) TestHas() {
	testCases := []struct {
		name      string
		key       []byte
		version   int64
		expectErr bool
		result    bool
	}{
		{
			"existing key for valid version",
			[]byte("key-0"),
			1,
			false,
			true,
		},
		{
			"non-existent key for valid version",
			[]byte("key-100"),
			1,
			false,
			false,
		},
		{
			"existing key for invalid version",
			[]byte("key-0"),
			2,
			true,
			false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		suite.Run(tc.name, func() {
			res, err := suite.server.Has(context.TODO(), &pb.HasRequest{Version: tc.version, Key: tc.key})
			suite.Equal(tc.expectErr, err != nil)

			if !tc.expectErr {
				suite.Equal(tc.result, res.Result)
			}
		})
	}
}

func TestServerTestSuite(t *testing.T) {
	suite.Run(t, new(ServerTestSuite))
}
