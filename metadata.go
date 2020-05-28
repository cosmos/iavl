package iavl

import (
	"google.golang.org/protobuf/proto"
)

// Marshal returns Protobuf marshaled bytes of a VersionMetadata object.
func (vm *VersionMetadata) Marshal() ([]byte, error) {
	return proto.Marshal(vm)
}

// Unmarshal attempts to decode a VersionMetadata object from Protobuf marshaled
// bytes.
func (vm *VersionMetadata) Unmarshal(bz []byte) error {
	return proto.Unmarshal(bz, vm)
}
