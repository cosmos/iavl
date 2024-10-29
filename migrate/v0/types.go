package v0

import (
	"time"

	"github.com/gogo/protobuf/proto"
)

type CommitID struct {
	Version int64  `protobuf:"varint,1,opt,name=version,proto3" json:"version,omitempty"`
	Hash    []byte `protobuf:"bytes,2,opt,name=hash,proto3" json:"hash,omitempty"`
}

func (c *CommitID) Reset() {
	*c = CommitID{}
}

func (c *CommitID) String() string {
	return ""
}

func (c *CommitID) ProtoMessage() {
}

type StoreInfo struct {
	Name     string   `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	CommitId CommitID `protobuf:"bytes,2,opt,name=commit_id,json=commitId,proto3" json:"commit_id"`
}

func (s *StoreInfo) Reset() {
	*s = StoreInfo{}
}

func (s *StoreInfo) String() string {
	return ""
}

func (s *StoreInfo) ProtoMessage() {
}

type CommitInfo struct {
	Version    int64       `protobuf:"varint,1,opt,name=version,proto3" json:"version,omitempty"`
	StoreInfos []StoreInfo `protobuf:"bytes,2,rep,name=store_infos,json=storeInfos,proto3" json:"store_infos"`
	Timestamp  time.Time   `protobuf:"bytes,3,opt,name=timestamp,proto3,stdtime" json:"timestamp"`
}

func (c *CommitInfo) Reset() {
	*c = CommitInfo{}
}

func (c *CommitInfo) String() string {
	return ""
}

func (c *CommitInfo) ProtoMessage() {}

var (
	_ proto.Message = (*CommitInfo)(nil)
	_ proto.Message = (*CommitID)(nil)
	_ proto.Message = (*StoreInfo)(nil)
)
