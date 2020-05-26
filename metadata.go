package iavl

import (
	"fmt"

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

// VersionMetadataKey returns the indexing key for a VersionMetadata object.
func VersionMetadataKey(version int64) []byte {
	return []byte(fmt.Sprintf("metadata/%d", version))
}
