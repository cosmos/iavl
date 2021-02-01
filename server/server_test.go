package server_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/cosmos/iavl"
	"github.com/stretchr/testify/suite"
	dbm "github.com/tendermint/tm-db"
	"google.golang.org/grpc"

	pb "github.com/cosmos/iavl/proto"
	"github.com/cosmos/iavl/server"
)

type ServerTestSuite struct {
	suite.Suite

	server *server.IAVLServer

	client pb.IAVLServiceClient
}

func (suite *ServerTestSuite) SetupTest() {
	db, err := dbm.NewDB("test", dbm.MemDBBackend, "")

	suite.NoError(err)

	server, err := server.New(db, 1000, 0)
	suite.NoError(err)

	suite.server = server
	suite.populateItems(100)

	grpcServer := grpc.NewServer()
	listener, err := net.Listen("tcp", ":0") // random available port
	pb.RegisterIAVLServiceServer(grpcServer, server)
	suite.NoError(err)
	addr := listener.Addr().String()

	go func() {
		err = grpcServer.Serve(listener)
		if err != nil {
			panic(err)
		}
	}()
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	suite.NoError(err)
	client := pb.NewIAVLServiceClient(conn)
	suite.client = client

}

func (suite *ServerTestSuite) populateItems(n int) {
	versionRes, err := suite.server.Version(context.Background(), nil)
	suite.NoError(err)

	for i := 0; i < n; i++ {
		req := &pb.SetRequest{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}

		_, err = suite.server.Set(context.Background(), req)
		suite.NoError(err)
	}

	res, err := suite.server.SaveVersion(context.Background(), nil)
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
			res, err := suite.server.HasVersioned(context.Background(), &pb.HasVersionedRequest{Version: tc.version, Key: tc.key})
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
		result    *pb.GetResponse
	}{
		{
			"existing key",
			nil,
			[]byte("key-0"),
			false,
			&pb.GetResponse{Index: 0, Value: []byte("value-0"), NotFound: false},
		},
		{
			"existing modified key",
			func() {
				req := &pb.SetRequest{
					Key:   []byte("key-0"),
					Value: []byte("NEW_VALUE"),
				}

				_, err := suite.server.Set(context.Background(), req)
				suite.NoError(err)

				_, err = suite.server.SaveVersion(context.Background(), nil)
				suite.NoError(err)
			},
			[]byte("key-0"),
			false,
			&pb.GetResponse{Index: 0, Value: []byte("NEW_VALUE"), NotFound: false},
		},
		{
			"non-existent key",
			nil,
			[]byte("key-1000"),
			false,
			&pb.GetResponse{Index: 3, Value: nil, NotFound: true},
		},
	}

	for _, tc := range testCases {
		tc := tc
		suite.Run(tc.name, func() {
			if tc.preRun != nil {
				tc.preRun()
			}

			res, err := suite.server.Get(context.Background(), &pb.GetRequest{Key: tc.key})
			suite.Equal(tc.expectErr, err != nil)

			if !tc.expectErr {
				suite.Equal(tc.result, res)
			}
		})
	}
}

func (suite *ServerTestSuite) TestGetByIndex() {
	testCases := []struct {
		name      string
		preRun    func()
		index     int64
		expectErr bool
		result    *pb.GetByIndexResponse
	}{
		{
			"existing index",
			nil,
			0,
			false,
			&pb.GetByIndexResponse{Key: []byte("key-0"), Value: []byte("value-0")},
		},
		{
			"existing modified index",
			func() {
				req := &pb.SetRequest{
					Key:   []byte("key-0"),
					Value: []byte("NEW_VALUE"),
				}

				_, err := suite.server.Set(context.Background(), req)
				suite.NoError(err)

				_, err = suite.server.SaveVersion(context.Background(), nil)
				suite.NoError(err)
			},
			0,
			false,
			&pb.GetByIndexResponse{Key: []byte("key-0"), Value: []byte("NEW_VALUE")},
		},
		{
			"non-existent index",
			nil,
			1000,
			true,
			nil,
		},
	}

	for _, tc := range testCases {
		tc := tc
		suite.Run(tc.name, func() {
			if tc.preRun != nil {
				tc.preRun()
			}

			res, err := suite.server.GetByIndex(context.Background(), &pb.GetByIndexRequest{Index: tc.index})
			suite.Equal(tc.expectErr, err != nil)

			if !tc.expectErr {
				suite.Equal(tc.result, res)
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

				_, err := suite.server.Set(context.Background(), req)
				suite.NoError(err)

				_, err = suite.server.SaveVersion(context.Background(), nil)
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

				_, err := suite.server.Set(context.Background(), req)
				suite.NoError(err)

				_, err = suite.server.SaveVersion(context.Background(), nil)
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

			res, err := suite.server.GetVersioned(context.Background(), &pb.GetVersionedRequest{Version: tc.version, Key: tc.key})
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

				_, err := suite.server.Set(context.Background(), req)
				suite.NoError(err)

				_, err = suite.server.SaveVersion(context.Background(), nil)
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
			true,
			nil,
		},
	}

	for _, tc := range testCases {
		tc := tc
		suite.Run(tc.name, func() {
			if tc.preRun != nil {
				tc.preRun()
			}

			res, err := suite.server.GetVersionedWithProof(context.Background(), &pb.GetVersionedRequest{Version: tc.version, Key: tc.key})
			suite.Equal(tc.expectErr, err != nil)

			if !tc.expectErr {
				suite.Equal(tc.result, res.Value)

				if tc.result != nil {
					proof, err := iavl.RangeProofFromProto(res.Proof)

					if err != nil {
						suite.Error(err)
					}

					rootHash := proof.ComputeRootHash()
					suite.Equal(tc.expectErr, rootHash == nil)

					suite.NoError(proof.Verify(rootHash), fmt.Sprintf("root: %X\nproof: %s", rootHash, proof.String()))
				}
			}
		})
	}
}

func (suite *ServerTestSuite) TestSet() {
	res, err := suite.server.Set(context.Background(), &pb.SetRequest{Key: nil, Value: nil})
	suite.Error(err)
	suite.Nil(res)

	res, err = suite.server.Set(context.Background(), &pb.SetRequest{Key: []byte("key"), Value: nil})
	suite.Error(err)
	suite.Nil(res)

	res, err = suite.server.Set(context.Background(), &pb.SetRequest{Key: nil, Value: []byte("value")})
	suite.Error(err)
	suite.Nil(res)

	_, err = suite.server.Set(context.Background(), &pb.SetRequest{Key: []byte("key"), Value: []byte("value")})
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
			res, err := suite.server.Remove(context.Background(), &pb.RemoveRequest{Key: tc.key})
			suite.Equal(tc.expectErr, err != nil)

			if !tc.expectErr {
				suite.Equal(tc.value, res.Value)
				suite.Equal(tc.removed, res.Removed)
			}
		})
	}
}

// nolint:funlen
func (suite *ServerTestSuite) TestVerify() {
	testCases := []struct {
		name      string
		preRun    func()
		key       []byte
		version   int64
		expectErr bool
	}{
		{
			"verify with existing key",
			nil,
			[]byte("key-0"),
			1,
			false,
		},
		{
			"verify with existing modified key",
			func() {
				req := &pb.SetRequest{
					Key:   []byte("key-0"),
					Value: []byte("NEW_VALUE"),
				}

				_, err := suite.server.Set(context.Background(), req)
				suite.NoError(err)

				_, err = suite.server.SaveVersion(context.Background(), nil)
				suite.NoError(err)
			},
			[]byte("key-0"),
			2,
			false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		suite.Run(tc.name, func() {
			if tc.preRun != nil {
				tc.preRun()
			}

			res, err := suite.server.GetVersionedWithProof(context.Background(), &pb.GetVersionedRequest{Version: tc.version, Key: tc.key})
			suite.Equal(tc.expectErr, err != nil)

			if !tc.expectErr {

				proof, err := iavl.RangeProofFromProto(res.Proof)

				if err != nil {
					suite.Error(err)
				}

				rootHash := proof.ComputeRootHash()
				suite.Equal(tc.expectErr, rootHash == nil)

				verifyReq := &pb.VerifyRequest{
					RootHash: rootHash,
					Proof:    res.Proof,
				}

				_, err = suite.server.Verify(context.Background(), verifyReq)
				suite.NoError(err)
			}
		})
	}
}

// nolint:funlen
func (suite *ServerTestSuite) TestVerifyAbsense() {
	testCases := []struct {
		name            string
		preRun          func()
		existingKey     []byte
		questionableKey []byte
		version         int64
		expectErr       bool
	}{
		{
			"verify absence with non-existing key",
			nil,
			[]byte("key-0"),
			[]byte("non-existing-key"),
			1,
			false,
		},
		{
			"verify with existing key",
			nil,
			[]byte("key-0"),
			[]byte("key-1"),
			1,
			true,
		},
	}

	for _, tc := range testCases {
		tc := tc
		suite.Run(tc.name, func() {
			if tc.preRun != nil {
				tc.preRun()
			}

			res, err := suite.server.GetVersionedWithProof(context.Background(), &pb.GetVersionedRequest{Version: tc.version, Key: tc.existingKey})
			if err != nil {
				proof, err := iavl.RangeProofFromProto(res.Proof)
				if err != nil {
					suite.Error(err)
				}
				rootHash := proof.ComputeRootHash()
				suite.Equal(tc.expectErr, rootHash == nil)

				verifyAbsReq := &pb.VerifyAbsenceRequest{
					RootHash: rootHash,
					Proof:    res.Proof,
					Key:      tc.questionableKey,
				}

				_, err = suite.server.VerifyAbsence(context.Background(), verifyAbsReq)
				suite.Equal(tc.expectErr, err != nil)
			}

		})
	}
}

// nolint:funlen
func (suite *ServerTestSuite) TestVerifyItem() {
	testCases := []struct {
		name      string
		preRun    func()
		key       []byte
		version   int64
		expectErr bool
		value     []byte
	}{
		{
			"verify item with existing key and value",
			nil,
			[]byte("key-0"),
			1,
			false,
			[]byte("value-0"),
		},
		{
			"fail to verify with existing key and incorrect value",
			nil,
			[]byte("key-0"),
			1,
			true,
			[]byte("invalid-value"),
		},
	}

	for _, tc := range testCases {
		tc := tc
		suite.Run(tc.name, func() {
			if tc.preRun != nil {
				tc.preRun()
			}

			res, err := suite.server.GetVersionedWithProof(context.Background(), &pb.GetVersionedRequest{Version: tc.version, Key: tc.key})
			if err != nil {
				proof, err := iavl.RangeProofFromProto(res.Proof)
				suite.NoError(err)
				rootHash := proof.ComputeRootHash()
				suite.Equal(tc.expectErr, rootHash == nil)

				verifyItemReq := &pb.VerifyItemRequest{
					RootHash: rootHash,
					Proof:    res.Proof,
					Key:      tc.key,
					Value:    tc.value,
				}

				_, err = suite.server.VerifyItem(context.Background(), verifyItemReq)
				suite.Equal(tc.expectErr, err != nil)
			}

		})
	}
}

func (suite *ServerTestSuite) TestDeleteVersion() {
	res, err := suite.server.DeleteVersion(context.Background(), &pb.DeleteVersionRequest{Version: 0})
	suite.Error(err)
	suite.Nil(res)

	res, err = suite.server.DeleteVersion(context.Background(), &pb.DeleteVersionRequest{Version: 1})
	suite.Error(err)
	suite.Nil(res)

	_, err = suite.server.SaveVersion(context.Background(), nil)
	suite.NoError(err)

	res, err = suite.server.DeleteVersion(context.Background(), &pb.DeleteVersionRequest{Version: 1})
	suite.NoError(err)
	suite.Equal(int64(1), res.Version)
	suite.Equal("B01CCD167F03233BC51C44116D0420935826A533473AE39829556D0665BACDA9", fmt.Sprintf("%X", res.RootHash))
}

func (suite *ServerTestSuite) TestHash() {
	res, err := suite.server.Hash(context.Background(), nil)
	suite.NoError(err)
	suite.Equal("B01CCD167F03233BC51C44116D0420935826A533473AE39829556D0665BACDA9", fmt.Sprintf("%X", res.RootHash))

	req := &pb.SetRequest{
		Key:   []byte("key-0"),
		Value: []byte("NEW_VALUE"),
	}

	_, err = suite.server.Set(context.Background(), req)
	suite.NoError(err)

	_, err = suite.server.SaveVersion(context.Background(), nil)
	suite.NoError(err)

	res, err = suite.server.Hash(context.Background(), nil)
	suite.NoError(err)
	suite.Equal("B708C71EA143DF334BB7DC9FBD7C47DA3A3B16C2E15F2990E5BEB3FABC8AE8CA", fmt.Sprintf("%X", res.RootHash))
}

func (suite *ServerTestSuite) TestVersionExists() {
	res, err := suite.server.VersionExists(context.Background(), &pb.VersionExistsRequest{Version: 1})
	suite.NoError(err)
	suite.True(res.Result)

	res, err = suite.server.VersionExists(context.Background(), &pb.VersionExistsRequest{Version: 2})
	suite.NoError(err)
	suite.False(res.Result)
}

// nolint:funlen
func (suite *ServerTestSuite) TestRollback() {
	testCases := []struct {
		name      string
		preRun    func()
		key       []byte
		version   int64
		expectErr bool
		value     []byte
	}{
		{
			"make changes and rollback",
			func() {
				req := &pb.SetRequest{
					Key:   []byte("key-0"),
					Value: []byte("NEW_VALUE"),
				}
				_, err := suite.server.Set(context.Background(), req)
				suite.NoError(err)
			},
			[]byte("key-0"),
			2,
			false,
			[]byte("value-0"),
		},
		{
			"make changes, save, and rollback keeping changes",
			func() {
				req := &pb.SetRequest{
					Key:   []byte("key-0"),
					Value: []byte("NEW_VALUE"),
				}
				_, err := suite.server.Set(context.Background(), req)
				suite.NoError(err)

				_, err = suite.server.SaveVersion(context.Background(), nil)
				suite.NoError(err)
			},
			[]byte("key-0"),
			2,
			false,
			[]byte("NEW_VALUE"),
		},
	}

	for _, tc := range testCases {
		tc := tc
		suite.Run(tc.name, func() {
			if tc.preRun != nil {
				tc.preRun()
			}

			_, err := suite.server.Rollback(context.Background(), nil)
			suite.Equal(tc.expectErr, err != nil)

			res, getErr := suite.server.Get(context.Background(), &pb.GetRequest{Key: tc.key})
			suite.Equal(tc.expectErr, getErr != nil)

			suite.Equal(res.Value, tc.value)
		})
	}
}

func (suite *ServerTestSuite) TestSize() {
	res1, err := suite.server.Size(context.Background(), nil)
	suite.NoError(err)

	req := &pb.SetRequest{
		Key:   []byte("test-size-key"),
		Value: []byte("test-size-value"),
	}

	_, err = suite.server.Set(context.Background(), req)
	suite.NoError(err)

	res2, err := suite.server.Size(context.Background(), nil)
	suite.NoError(err)

	suite.Equal(res1.Size_+1, res2.Size_)

	_, err = suite.server.Rollback(context.Background(), nil)
	suite.NoError(err)

	res3, err := suite.server.Size(context.Background(), nil)
	suite.NoError(err)
	suite.Equal(res3.Size_, res1.Size_)

}

func (suite *ServerTestSuite) TestList() {

	req1 := &pb.SetRequest{
		Key:   []byte("test-list-key1"),
		Value: []byte("test-list-value1"),
	}

	req2 := &pb.SetRequest{
		Key:   []byte("test-list-key2"),
		Value: []byte("test-list-value2"),
	}

	_, err := suite.server.Set(context.Background(), req1)
	suite.NoError(err)

	_, err = suite.server.Set(context.Background(), req2)
	suite.NoError(err)

	req3 := &pb.ListRequest{
		FromKey:    []byte("test-list-key1"),
		ToKey:      []byte("test-list-key2"),
		Descending: false,
	}

	stream, err := suite.client.List(context.Background(), req3)
	suite.NoError(err)

	keyIndex := 0
	for {
		res, err := stream.Recv()
		if err == io.EOF {
			break
		}
		suite.NoError(err)

		if keyIndex == 1 {
			suite.Equal(res.Key, req3.ToKey)
			keyIndex++
		}

		if keyIndex == 0 {
			suite.Equal(res.Key, req3.FromKey)
			keyIndex++
		}

		if keyIndex > 1 {
			panic("Unexpected key in List method")
		}

	}

}

func (suite *ServerTestSuite) TestAvailableVersions() {
	res1, err := suite.server.GetAvailableVersions(context.Background(), nil)
	suite.NoError(err)
	oldVersions := res1.Versions

	_, err = suite.server.SaveVersion(context.Background(), nil)
	suite.NoError(err)

	versionRes, err := suite.server.Version(context.Background(), nil)
	suite.NoError(err)
	newVersions := append(oldVersions, versionRes.Version)

	res2, err := suite.server.GetAvailableVersions(context.Background(), nil)
	suite.NoError(err)

	suite.Equal(res2.Versions, newVersions)

}

func TestServerTestSuite(t *testing.T) {
	suite.Run(t, new(ServerTestSuite))
}
