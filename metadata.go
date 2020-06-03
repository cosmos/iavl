package iavl

import (
	"google.golang.org/protobuf/proto"
)

// marshal returns Protobuf marshaled bytes of a VersionMetadata object.
func (vm *VersionMetadata) marshal() ([]byte, error) {
	return proto.Marshal(vm)
}

// unmarshalVersionMetadata attempts to decode a VersionMetadata object from Protobuf
// marshaled bytes.
func unmarshalVersionMetadata(bz []byte) (*VersionMetadata, error) {
	vm := &VersionMetadata{}
	err := proto.Unmarshal(bz, vm)
	return vm, err
}
