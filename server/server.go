package server

import (
	"context"
	"errors"

	"github.com/golang/protobuf/ptypes/empty"
	dbm "github.com/tendermint/tm-db"

	"github.com/tendermint/iavl"
	pb "github.com/tendermint/iavl/proto"
)

var _ pb.IAVLServiceServer = (*IAVLServer)(nil)

// iavlServer implements the gRPC IAVLServiceServer interface. It provides a gRPC
// API over an IAVL tree.
type IAVLServer struct {
	tree *iavl.MutableTree
}

func New(db dbm.DB, cacheSize, version int64) (*IAVLServer, error) {
	tree := iavl.NewMutableTree(db, int(cacheSize))

	if _, err := tree.LoadVersion(version); err != nil {
		return nil, err
	}

	return &IAVLServer{tree: tree}, nil
}

// Has returns a result containing a boolean on whether or not the IAVL tree
// has a given key at a specific tree version.
func (s *IAVLServer) Has(_ context.Context, req *pb.HasRequest) (*pb.HasResponse, error) {
	if !s.tree.VersionExists(req.Version) {
		return nil, iavl.ErrVersionDoesNotExist
	}

	iTree, err := s.tree.GetImmutable(req.Version)
	if err != nil {
		return nil, err
	}

	return &pb.HasResponse{Result: iTree.Has(req.Key)}, nil
}

// Get returns a result containing the IAVL tree version and value for a given
// key based on the current state (version) of the tree.
func (s *IAVLServer) Get(_ context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	idx, value := s.tree.Get(req.Key)
	return &pb.GetResponse{Index: idx, Value: value}, nil
}

// GetVersioned returns a result containing the IAVL tree version and value
// for a given key at a specific tree version.
func (s *IAVLServer) GetVersioned(_ context.Context, req *pb.GetVersionedRequest) (*pb.GetResponse, error) {
	if !s.tree.VersionExists(req.Version) {
		return nil, iavl.ErrVersionDoesNotExist
	}

	iTree, err := s.tree.GetImmutable(req.Version)
	if err != nil {
		return nil, err
	}

	idx, value := iTree.Get(req.Key)

	return &pb.GetResponse{Index: idx, Value: value}, nil
}

func (s *IAVLServer) GetVersionedWithProof(context.Context, *pb.GetVersionedRequest) (*pb.GetVersionedWithProofResponse, error) {
	panic("not implemented!")
}

// Set returns a result after inserting a key/value pair into the IAVL tree
// based on the current state (version) of the tree.
func (s *IAVLServer) Set(_ context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	if req.Key == nil {
		return nil, errors.New("key cannot be nil")
	}

	if req.Value == nil {
		return nil, errors.New("value cannot be nil")
	}

	return &pb.SetResponse{Result: s.tree.Set(req.Key, req.Value)}, nil
}

// Remove returns a result after removing a key/value pair from the IAVL tree
// based on the current state (version) of the tree.
func (s *IAVLServer) Remove(context.Context, *pb.RemoveRequest) (*pb.RemoveResponse, error) {
	panic("not implemented!")
}

// SaveVersion saves a new IAVL tree version to the DB based on the current
// state (version) of the tree. It returns a result containing the hash and
// new version number.
func (s *IAVLServer) SaveVersion(_ context.Context, _ *empty.Empty) (*pb.SaveVersionResponse, error) {
	root, version, err := s.tree.SaveVersion()
	if err != nil {
		return nil, err
	}

	return &pb.SaveVersionResponse{RootHash: root, Version: version}, nil
}

// DeleteVersion deletes an IAVL tree version from the DB. The version can then
// no longer be accessed. It returns a result containing the version and root
// hash of the versioned tree that was deleted.
func (s *IAVLServer) DeleteVersion(context.Context, *pb.DeleteVersionRequest) (*pb.DeleteVersionResponse, error) {
	panic("not implemented!")
}

// Version returns the IAVL tree version based on the current state.
func (s *IAVLServer) Version(_ context.Context, _ *empty.Empty) (*pb.VersionResponse, error) {
	return &pb.VersionResponse{Version: s.tree.Version()}, nil
}

// Hash returns the IAVL tree root hash based on the current state.
func (s *IAVLServer) Hash(context.Context, *empty.Empty) (*pb.HashResponse, error) {
	panic("not implemented!")
}

// VersionExists returns a result containing a boolean on whether or not a given
// version exists in the IAVL tree.
func (s *IAVLServer) VersionExists(context.Context, *pb.VersionExistsRequest) (*pb.VersionExistsResponse, error) {
	panic("not implemented!")
}
