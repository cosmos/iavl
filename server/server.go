package server

import (
	"context"

	"github.com/golang/protobuf/ptypes/empty"
	pb "github.com/tendermint/iavl/proto"
)

var _ pb.IAVLServiceServer = (*IAVLServer)(nil)

// iavlServer implements the gRPC IAVLServiceServer interface. It provides a gRPC
// API over an IAVL tree.
type IAVLServer struct {
}

func New() *IAVLServer {
	return &IAVLServer{}
}

// Has returns a result containing a boolean on whether or not the IAVL tree
// has a given key at a specific tree version.
func (s *IAVLServer) Has(context.Context, *pb.HasRequest) (*pb.HasResponse, error) {
	panic("not implemented!")
}

// Get returns a result containing the IAVL tree version and value for a given
// key based on the current state (version) of the tree.
func (s *IAVLServer) Get(context.Context, *pb.GetRequest) (*pb.GetResponse, error) {
	panic("not implemented!")
}

// GetVersioned returns a result containing the IAVL tree version and value
// for a given key at a specific tree version.
func (s *IAVLServer) GetVersioned(context.Context, *pb.GetVersionedRequest) (*pb.GetResponse, error) {
	panic("not implemented!")
}

func (s *IAVLServer) GetVersionedWithProof(context.Context, *pb.GetVersionedRequest) (*pb.GetVersionedWithProofResponse, error) {
	panic("not implemented!")
}

// Set returns a result after inserting a key/value pair into the IAVL tree
// based on the current state (version) of the tree.
func (s *IAVLServer) Set(context.Context, *pb.SetRequest) (*pb.SetResponse, error) {
	panic("not implemented!")
}

// Remove returns a result after removing a key/value pair from the IAVL tree
// based on the current state (version) of the tree.
func (s *IAVLServer) Remove(context.Context, *pb.RemoveRequest) (*pb.RemoveResponse, error) {
	panic("not implemented!")
}

// SaveVersion saves a new IAVL tree version to the DB based on the current
// state (version) of the tree. It returns a result containing the hash and
// new version number.
func (s *IAVLServer) SaveVersion(context.Context, *empty.Empty) (*pb.SaveVersionResponse, error) {
	panic("not implemented!")
}

// DeleteVersion deletes an IAVL tree version from the DB. The version can then
// no longer be accessed. It returns a result containing the version and root
// hash of the versioned tree that was deleted.
func (s *IAVLServer) DeleteVersion(context.Context, *pb.DeleteVersionRequest) (*pb.DeleteVersionResponse, error) {
	panic("not implemented!")
}

// Version returns the IAVL tree version based on the current state.
func (s *IAVLServer) Version(context.Context, *empty.Empty) (*pb.VersionResponse, error) {
	panic("not implemented!")
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
