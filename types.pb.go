// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: iavl/types.proto

package iavl

import (
	fmt "fmt"
	_ "github.com/gogo/protobuf/gogoproto"
	proto "github.com/gogo/protobuf/proto"
	io "io"
	math "math"
	math_bits "math/bits"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

// VersionMetadata defines the metadata associated with a committed IAVL version.
type VersionMetadata struct {
	// tree version for the corresponding metadata
	Version int64 `protobuf:"varint,1,opt,name=version,proto3" json:"version,omitempty"`
	// the UNIX timestamp of when the metadata was committed to disk
	Committed int64 `protobuf:"varint,2,opt,name=committed,proto3" json:"committed,omitempty"`
	// the UNIX timestamp of when the metadata was updated
	Updated int64 `protobuf:"varint,3,opt,name=updated,proto3" json:"updated,omitempty"`
	// the root hash of the tree for the corresponding metadata
	RootHash []byte `protobuf:"bytes,4,opt,name=root_hash,json=rootHash,proto3" json:"root_hash,omitempty"`
	// if this version corresponds to a version that is flushed to disk
	Snapshot bool `protobuf:"varint,5,opt,name=snapshot,proto3" json:"snapshot,omitempty"`
}

func (m *VersionMetadata) Reset()         { *m = VersionMetadata{} }
func (m *VersionMetadata) String() string { return proto.CompactTextString(m) }
func (*VersionMetadata) ProtoMessage()    {}
func (*VersionMetadata) Descriptor() ([]byte, []int) {
	return fileDescriptor_7ef37c124502d49e, []int{0}
}
func (m *VersionMetadata) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *VersionMetadata) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_VersionMetadata.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *VersionMetadata) XXX_Merge(src proto.Message) {
	xxx_messageInfo_VersionMetadata.Merge(m, src)
}
func (m *VersionMetadata) XXX_Size() int {
	return m.Size()
}
func (m *VersionMetadata) XXX_DiscardUnknown() {
	xxx_messageInfo_VersionMetadata.DiscardUnknown(m)
}

var xxx_messageInfo_VersionMetadata proto.InternalMessageInfo

func (m *VersionMetadata) GetVersion() int64 {
	if m != nil {
		return m.Version
	}
	return 0
}

func (m *VersionMetadata) GetCommitted() int64 {
	if m != nil {
		return m.Committed
	}
	return 0
}

func (m *VersionMetadata) GetUpdated() int64 {
	if m != nil {
		return m.Updated
	}
	return 0
}

func (m *VersionMetadata) GetRootHash() []byte {
	if m != nil {
		return m.RootHash
	}
	return nil
}

func (m *VersionMetadata) GetSnapshot() bool {
	if m != nil {
		return m.Snapshot
	}
	return false
}

// ProofOp defines an operation used for calculating Merkle root
// The data could be arbitrary format, providing nessecary data
// for example neighbouring node hash
type ProofOp struct {
	Type string `protobuf:"bytes,1,opt,name=type,proto3" json:"type,omitempty"`
	Key  []byte `protobuf:"bytes,2,opt,name=key,proto3" json:"key,omitempty"`
	Data []byte `protobuf:"bytes,3,opt,name=data,proto3" json:"data,omitempty"`
}

func (m *ProofOp) Reset()         { *m = ProofOp{} }
func (m *ProofOp) String() string { return proto.CompactTextString(m) }
func (*ProofOp) ProtoMessage()    {}
func (*ProofOp) Descriptor() ([]byte, []int) {
	return fileDescriptor_7ef37c124502d49e, []int{1}
}
func (m *ProofOp) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *ProofOp) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_ProofOp.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *ProofOp) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ProofOp.Merge(m, src)
}
func (m *ProofOp) XXX_Size() int {
	return m.Size()
}
func (m *ProofOp) XXX_DiscardUnknown() {
	xxx_messageInfo_ProofOp.DiscardUnknown(m)
}

var xxx_messageInfo_ProofOp proto.InternalMessageInfo

func (m *ProofOp) GetType() string {
	if m != nil {
		return m.Type
	}
	return ""
}

func (m *ProofOp) GetKey() []byte {
	if m != nil {
		return m.Key
	}
	return nil
}

func (m *ProofOp) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

// Proof is Merkle proof defined by the list of ProofOps
type Proof struct {
	Ops []ProofOp `protobuf:"bytes,1,rep,name=ops,proto3" json:"ops"`
}

func (m *Proof) Reset()         { *m = Proof{} }
func (m *Proof) String() string { return proto.CompactTextString(m) }
func (*Proof) ProtoMessage()    {}
func (*Proof) Descriptor() ([]byte, []int) {
	return fileDescriptor_7ef37c124502d49e, []int{2}
}
func (m *Proof) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *Proof) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_Proof.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *Proof) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Proof.Merge(m, src)
}
func (m *Proof) XXX_Size() int {
	return m.Size()
}
func (m *Proof) XXX_DiscardUnknown() {
	xxx_messageInfo_Proof.DiscardUnknown(m)
}

var xxx_messageInfo_Proof proto.InternalMessageInfo

func (m *Proof) GetOps() []ProofOp {
	if m != nil {
		return m.Ops
	}
	return nil
}

func init() {
	proto.RegisterType((*VersionMetadata)(nil), "iavl.VersionMetadata")
	proto.RegisterType((*ProofOp)(nil), "iavl.ProofOp")
	proto.RegisterType((*Proof)(nil), "iavl.Proof")
}

func init() { proto.RegisterFile("iavl/types.proto", fileDescriptor_7ef37c124502d49e) }

var fileDescriptor_7ef37c124502d49e = []byte{
	// 284 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x44, 0x90, 0xc1, 0x4a, 0xc3, 0x40,
	0x10, 0x86, 0xb3, 0x26, 0xb5, 0xed, 0x58, 0xb1, 0x2c, 0x1e, 0x96, 0x2a, 0x6b, 0x08, 0x08, 0x39,
	0xa5, 0xa0, 0x6f, 0x50, 0x2f, 0x5e, 0x44, 0xd9, 0x83, 0x57, 0x59, 0x4d, 0x6c, 0x82, 0xb6, 0xb3,
	0x64, 0xd7, 0x42, 0xdf, 0xc2, 0x07, 0xf0, 0x81, 0x7a, 0xec, 0xd1, 0x93, 0x48, 0xf2, 0x22, 0x32,
	0x9b, 0xaa, 0xb7, 0xff, 0xff, 0xfe, 0xdd, 0xe1, 0x9f, 0x81, 0x71, 0xa5, 0x57, 0xaf, 0x53, 0xb7,
	0x36, 0x85, 0xcd, 0x4c, 0x8d, 0x0e, 0x79, 0x44, 0x64, 0x72, 0x3c, 0xc7, 0x39, 0x7a, 0x30, 0x25,
	0xd5, 0x65, 0xc9, 0x07, 0x83, 0xa3, 0xfb, 0xa2, 0xb6, 0x15, 0x2e, 0x6f, 0x0a, 0xa7, 0x73, 0xed,
	0x34, 0x17, 0xd0, 0x5f, 0x75, 0x48, 0xb0, 0x98, 0xa5, 0xa1, 0xfa, 0xb5, 0xfc, 0x14, 0x86, 0x4f,
	0xb8, 0x58, 0x54, 0xce, 0x15, 0xb9, 0xd8, 0xf3, 0xd9, 0x3f, 0xa0, 0x7f, 0x6f, 0x26, 0xd7, 0x94,
	0x85, 0xdd, 0xbf, 0x9d, 0xe5, 0x27, 0x30, 0xac, 0x11, 0xdd, 0x43, 0xa9, 0x6d, 0x29, 0xa2, 0x98,
	0xa5, 0x23, 0x35, 0x20, 0x70, 0xad, 0x6d, 0xc9, 0x27, 0x30, 0xb0, 0x4b, 0x6d, 0x6c, 0x89, 0x4e,
	0xf4, 0x62, 0x96, 0x0e, 0xd4, 0x9f, 0x4f, 0xae, 0xa0, 0x7f, 0x57, 0x23, 0x3e, 0xdf, 0x1a, 0xce,
	0x21, 0xa2, 0xa5, 0x7c, 0xa5, 0xa1, 0xf2, 0x9a, 0x8f, 0x21, 0x7c, 0x29, 0xd6, 0xbe, 0xc9, 0x48,
	0x91, 0xa4, 0x57, 0xb4, 0x83, 0x2f, 0x30, 0x52, 0x5e, 0x27, 0x19, 0xf4, 0xfc, 0x10, 0x7e, 0x0e,
	0x21, 0x1a, 0x2b, 0x58, 0x1c, 0xa6, 0x07, 0x17, 0x87, 0x19, 0x9d, 0x25, 0xdb, 0x8d, 0x9f, 0x45,
	0x9b, 0xaf, 0xb3, 0x40, 0x51, 0x3e, 0x13, 0x9b, 0x46, 0xb2, 0x6d, 0x23, 0xd9, 0x77, 0x23, 0xd9,
	0x7b, 0x2b, 0x83, 0x6d, 0x2b, 0x83, 0xcf, 0x56, 0x06, 0x8f, 0xfb, 0xfe, 0x68, 0x97, 0x3f, 0x01,
	0x00, 0x00, 0xff, 0xff, 0x0e, 0xa7, 0xe8, 0xa5, 0x64, 0x01, 0x00, 0x00,
}

func (m *VersionMetadata) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *VersionMetadata) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *VersionMetadata) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.Snapshot {
		i--
		if m.Snapshot {
			dAtA[i] = 1
		} else {
			dAtA[i] = 0
		}
		i--
		dAtA[i] = 0x28
	}
	if len(m.RootHash) > 0 {
		i -= len(m.RootHash)
		copy(dAtA[i:], m.RootHash)
		i = encodeVarintTypes(dAtA, i, uint64(len(m.RootHash)))
		i--
		dAtA[i] = 0x22
	}
	if m.Updated != 0 {
		i = encodeVarintTypes(dAtA, i, uint64(m.Updated))
		i--
		dAtA[i] = 0x18
	}
	if m.Committed != 0 {
		i = encodeVarintTypes(dAtA, i, uint64(m.Committed))
		i--
		dAtA[i] = 0x10
	}
	if m.Version != 0 {
		i = encodeVarintTypes(dAtA, i, uint64(m.Version))
		i--
		dAtA[i] = 0x8
	}
	return len(dAtA) - i, nil
}

func (m *ProofOp) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *ProofOp) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *ProofOp) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Data) > 0 {
		i -= len(m.Data)
		copy(dAtA[i:], m.Data)
		i = encodeVarintTypes(dAtA, i, uint64(len(m.Data)))
		i--
		dAtA[i] = 0x1a
	}
	if len(m.Key) > 0 {
		i -= len(m.Key)
		copy(dAtA[i:], m.Key)
		i = encodeVarintTypes(dAtA, i, uint64(len(m.Key)))
		i--
		dAtA[i] = 0x12
	}
	if len(m.Type) > 0 {
		i -= len(m.Type)
		copy(dAtA[i:], m.Type)
		i = encodeVarintTypes(dAtA, i, uint64(len(m.Type)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *Proof) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Proof) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *Proof) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Ops) > 0 {
		for iNdEx := len(m.Ops) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := m.Ops[iNdEx].MarshalToSizedBuffer(dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintTypes(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0xa
		}
	}
	return len(dAtA) - i, nil
}

func encodeVarintTypes(dAtA []byte, offset int, v uint64) int {
	offset -= sovTypes(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *VersionMetadata) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Version != 0 {
		n += 1 + sovTypes(uint64(m.Version))
	}
	if m.Committed != 0 {
		n += 1 + sovTypes(uint64(m.Committed))
	}
	if m.Updated != 0 {
		n += 1 + sovTypes(uint64(m.Updated))
	}
	l = len(m.RootHash)
	if l > 0 {
		n += 1 + l + sovTypes(uint64(l))
	}
	if m.Snapshot {
		n += 2
	}
	return n
}

func (m *ProofOp) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Type)
	if l > 0 {
		n += 1 + l + sovTypes(uint64(l))
	}
	l = len(m.Key)
	if l > 0 {
		n += 1 + l + sovTypes(uint64(l))
	}
	l = len(m.Data)
	if l > 0 {
		n += 1 + l + sovTypes(uint64(l))
	}
	return n
}

func (m *Proof) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if len(m.Ops) > 0 {
		for _, e := range m.Ops {
			l = e.Size()
			n += 1 + l + sovTypes(uint64(l))
		}
	}
	return n
}

func sovTypes(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozTypes(x uint64) (n int) {
	return sovTypes(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *VersionMetadata) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTypes
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: VersionMetadata: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: VersionMetadata: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Version", wireType)
			}
			m.Version = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Version |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Committed", wireType)
			}
			m.Committed = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Committed |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Updated", wireType)
			}
			m.Updated = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Updated |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field RootHash", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthTypes
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthTypes
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.RootHash = append(m.RootHash[:0], dAtA[iNdEx:postIndex]...)
			if m.RootHash == nil {
				m.RootHash = []byte{}
			}
			iNdEx = postIndex
		case 5:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Snapshot", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				v |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Snapshot = bool(v != 0)
		default:
			iNdEx = preIndex
			skippy, err := skipTypes(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthTypes
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthTypes
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *ProofOp) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTypes
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: ProofOp: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: ProofOp: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Type", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthTypes
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthTypes
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Type = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Key", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthTypes
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthTypes
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Key = append(m.Key[:0], dAtA[iNdEx:postIndex]...)
			if m.Key == nil {
				m.Key = []byte{}
			}
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Data", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthTypes
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthTypes
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Data = append(m.Data[:0], dAtA[iNdEx:postIndex]...)
			if m.Data == nil {
				m.Data = []byte{}
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipTypes(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthTypes
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthTypes
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *Proof) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTypes
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Proof: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Proof: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Ops", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthTypes
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthTypes
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Ops = append(m.Ops, ProofOp{})
			if err := m.Ops[len(m.Ops)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipTypes(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthTypes
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthTypes
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipTypes(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowTypes
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowTypes
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
		case 1:
			iNdEx += 8
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowTypes
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthTypes
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupTypes
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthTypes
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthTypes        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowTypes          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupTypes = fmt.Errorf("proto: unexpected end of group")
)
