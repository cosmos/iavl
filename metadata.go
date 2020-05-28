package iavl

import (
	"google.golang.org/protobuf/proto"
)

// Marshal returns Protobuf marshaled bytes of a VersionMetadata object.
func (vm *VersionMetadata) Marshal() ([]byte, error) {
	return proto.Marshal(vm)
}

// VersionMetadata attempts to decode a VersionMetadata object from Protobuf
// marshaled bytes.
func UnmarshalVersionMetadata(bz []byte) (*VersionMetadata, error) {
	vm := &VersionMetadata{}
	err := proto.Unmarshal(bz, vm)
	return vm, err
}
