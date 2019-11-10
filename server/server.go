package server

import (
	"context"

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

func (i *IAVLServer) Ping(_ context.Context, req *pb.PingRequest) (*pb.PongResponse, error) {
	return &pb.PongResponse{Reply: "pong"}, nil
}
