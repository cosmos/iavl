package server_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/tendermint/iavl"
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

func (suite *ServerTestSuite) TestGet() {
	testCases := []struct {
		name      string
		preRun    func()
		key       []byte
		expectErr bool
		result    []byte
	}{
		{
			"existing key",
			nil,
			[]byte("key-0"),
			false,
			[]byte("value-0"),
		},
		{
			"existing modified key",
			func() {
				req := &pb.SetRequest{
					Key:   []byte("key-0"),
					Value: []byte("NEW_VALUE"),
				}

				_, err := suite.server.Set(context.TODO(), req)
				suite.NoError(err)

				_, err = suite.server.SaveVersion(context.TODO(), nil)
				suite.NoError(err)
			},
			[]byte("key-0"),
			false,
			[]byte("NEW_VALUE"),
		},
		{
			"non-existent key",
			nil,
			[]byte("key-1000"),
			false,
			nil,
		},
	}

	for _, tc := range testCases {
		tc := tc
		suite.Run(tc.name, func() {
			if tc.preRun != nil {
				tc.preRun()
			}

			res, err := suite.server.Get(context.TODO(), &pb.GetRequest{Key: tc.key})
			suite.Equal(tc.expectErr, err != nil)

			if !tc.expectErr {
				suite.Equal(tc.result, res.Value)
			}
		})
	}
}

// nolint:funlen
func (suite *ServerTestSuite) TestGetVersioned() {
	testCases := []struct {
		name      string
		preRun    func()
		key       []byte
		version   int64
		expectErr bool
		result    []byte
	}{
		{
			"existing key",
			nil,
			[]byte("key-0"),
			1,
			false,
			[]byte("value-0"),
		},
		{
			"existing modified key (new version)",
			func() {
				req := &pb.SetRequest{
					Key:   []byte("key-0"),
					Value: []byte("NEW_VALUE"),
				}

				_, err := suite.server.Set(context.TODO(), req)
				suite.NoError(err)

				_, err = suite.server.SaveVersion(context.TODO(), nil)
				suite.NoError(err)
			},
			[]byte("key-0"),
			2,
			false,
			[]byte("NEW_VALUE"),
		},
		{
			"existing modified key (previous version)",
			func() {
				req := &pb.SetRequest{
					Key:   []byte("key-0"),
					Value: []byte("NEW_VALUE"),
				}

				_, err := suite.server.Set(context.TODO(), req)
				suite.NoError(err)

				_, err = suite.server.SaveVersion(context.TODO(), nil)
				suite.NoError(err)
			},
			[]byte("key-0"),
			1,
			false,
			[]byte("value-0"),
		},
		{
			"non-existent key",
			nil,
			[]byte("key-1000"),
			1,
			false,
			nil,
		},
	}

	for _, tc := range testCases {
		tc := tc
		suite.Run(tc.name, func() {
			if tc.preRun != nil {
				tc.preRun()
			}

			res, err := suite.server.GetVersioned(context.TODO(), &pb.GetVersionedRequest{Version: tc.version, Key: tc.key})
			suite.Equal(tc.expectErr, err != nil)

			if !tc.expectErr {
				suite.Equal(tc.result, res.Value)
			}
		})
	}
}

// nolint:funlen
func (suite *ServerTestSuite) TestGetVersionedWithProof() {
	testCases := []struct {
		name      string
		preRun    func()
		key       []byte
		version   int64
		expectErr bool
		result    []byte
	}{
		{
			"existing key",
			nil,
			[]byte("key-0"),
			1,
			false,
			[]byte("value-0"),
		},
		{
			"existing modified key (new version)",
			func() {
				req := &pb.SetRequest{
					Key:   []byte("key-0"),
					Value: []byte("NEW_VALUE"),
				}

				_, err := suite.server.Set(context.TODO(), req)
				suite.NoError(err)

				_, err = suite.server.SaveVersion(context.TODO(), nil)
				suite.NoError(err)
			},
			[]byte("key-0"),
			2,
			false,
			[]byte("NEW_VALUE"),
		},
		{
			"existing key (old version)",
			nil,
			[]byte("key-0"),
			1,
			false,
			[]byte("value-0"),
		},
		{
			"non-existent key",
			nil,
			[]byte("key-1000"),
			1,
			false,
			nil,
		},
	}

	for _, tc := range testCases {
		tc := tc
		suite.Run(tc.name, func() {
			if tc.preRun != nil {
				tc.preRun()
			}

			res, err := suite.server.GetVersionedWithProof(context.TODO(), &pb.GetVersionedRequest{Version: tc.version, Key: tc.key})
			suite.Equal(tc.expectErr, err != nil)

			if !tc.expectErr {
				suite.Equal(tc.result, res.Value)

				if tc.result != nil {
					proof := iavl.ConvertProtoRangeProof(res.Proof)

					hashResp, err := suite.server.Hash(context.TODO(), nil)
					suite.NoError(err)

					suite.NoError(proof.Verify(hashResp.RootHash), fmt.Sprintf("root: %X\nproof: %s", hashResp.GetRootHash(), proof))
				}
			}
		})
	}
}

func (suite *ServerTestSuite) TestSet() {
	res, err := suite.server.Set(context.TODO(), &pb.SetRequest{Key: nil, Value: nil})
	suite.Error(err)
	suite.Nil(res)

	res, err = suite.server.Set(context.TODO(), &pb.SetRequest{Key: []byte("key"), Value: nil})
	suite.Error(err)
	suite.Nil(res)

	res, err = suite.server.Set(context.TODO(), &pb.SetRequest{Key: nil, Value: []byte("value")})
	suite.Error(err)
	suite.Nil(res)

	_, err = suite.server.Set(context.TODO(), &pb.SetRequest{Key: []byte("key"), Value: []byte("value")})
	suite.NoError(err)
}

func (suite *ServerTestSuite) TestRemove() {
	testCases := []struct {
		name      string
		key       []byte
		value     []byte
		removed   bool
		expectErr bool
	}{
		{
			"successfully remove existing key",
			[]byte("key-0"),
			[]byte("value-0"),
			true,
			false,
		},
		{
			"fail to remove non-existent key",
			[]byte("key-100"),
			nil,
			false,
			false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		suite.Run(tc.name, func() {
			res, err := suite.server.Remove(context.TODO(), &pb.RemoveRequest{Key: tc.key})
			suite.Equal(tc.expectErr, err != nil)

			if !tc.expectErr {
				suite.Equal(tc.value, res.Value)
				suite.Equal(tc.removed, res.Removed)
			}
		})
	}
}

func (suite *ServerTestSuite) TestDeleteVersion() {
	res, err := suite.server.DeleteVersion(context.TODO(), &pb.DeleteVersionRequest{Version: 0})
	suite.Error(err)
	suite.Nil(res)

	res, err = suite.server.DeleteVersion(context.TODO(), &pb.DeleteVersionRequest{Version: 1})
	suite.Error(err)
	suite.Nil(res)

	_, err = suite.server.SaveVersion(context.TODO(), nil)
	suite.NoError(err)

	res, err = suite.server.DeleteVersion(context.TODO(), &pb.DeleteVersionRequest{Version: 1})
	suite.NoError(err)
	suite.Equal(int64(1), res.Version)
	suite.Equal("B01CCD167F03233BC51C44116D0420935826A533473AE39829556D0665BACDA9", fmt.Sprintf("%X", res.RootHash))
}

func (suite *ServerTestSuite) TestHash() {
	res, err := suite.server.Hash(context.TODO(), nil)
	suite.NoError(err)
	suite.Equal("B01CCD167F03233BC51C44116D0420935826A533473AE39829556D0665BACDA9", fmt.Sprintf("%X", res.RootHash))

	req := &pb.SetRequest{
		Key:   []byte("key-0"),
		Value: []byte("NEW_VALUE"),
	}

	_, err = suite.server.Set(context.TODO(), req)
	suite.NoError(err)

	_, err = suite.server.SaveVersion(context.TODO(), nil)
	suite.NoError(err)

	res, err = suite.server.Hash(context.TODO(), nil)
	suite.NoError(err)
	suite.Equal("B708C71EA143DF334BB7DC9FBD7C47DA3A3B16C2E15F2990E5BEB3FABC8AE8CA", fmt.Sprintf("%X", res.RootHash))
}

func (suite *ServerTestSuite) TestVersionExists() {
	res, err := suite.server.VersionExists(context.TODO(), &pb.VersionExistsRequest{Version: 1})
	suite.NoError(err)
	suite.True(res.Result)

	res, err = suite.server.VersionExists(context.TODO(), &pb.VersionExistsRequest{Version: 2})
	suite.NoError(err)
	suite.False(res.Result)
}

func TestServerTestSuite(t *testing.T) {
	suite.Run(t, new(ServerTestSuite))
}
