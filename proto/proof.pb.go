// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: iavl/proof.proto

package proto

import (
	fmt "fmt"
	proto "github.com/cosmos/gogoproto/proto"
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

// ValueOp is a Protobuf representation of iavl.ValueOp.
type ValueOp struct {
	Proof *RangeProof `protobuf:"bytes,1,opt,name=proof,proto3" json:"proof,omitempty"`
}

func (m *ValueOp) Reset()         { *m = ValueOp{} }
func (m *ValueOp) String() string { return proto.CompactTextString(m) }
func (*ValueOp) ProtoMessage()    {}
func (*ValueOp) Descriptor() ([]byte, []int) {
	return fileDescriptor_92b2514a05d2a2db, []int{0}
}
func (m *ValueOp) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *ValueOp) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_ValueOp.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *ValueOp) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ValueOp.Merge(m, src)
}
func (m *ValueOp) XXX_Size() int {
	return m.Size()
}
func (m *ValueOp) XXX_DiscardUnknown() {
	xxx_messageInfo_ValueOp.DiscardUnknown(m)
}

var xxx_messageInfo_ValueOp proto.InternalMessageInfo

func (m *ValueOp) GetProof() *RangeProof {
	if m != nil {
		return m.Proof
	}
	return nil
}

// AbsenceOp is a Protobuf representation of iavl.AbsenceOp.
type AbsenceOp struct {
	Proof *RangeProof `protobuf:"bytes,1,opt,name=proof,proto3" json:"proof,omitempty"`
}

func (m *AbsenceOp) Reset()         { *m = AbsenceOp{} }
func (m *AbsenceOp) String() string { return proto.CompactTextString(m) }
func (*AbsenceOp) ProtoMessage()    {}
func (*AbsenceOp) Descriptor() ([]byte, []int) {
	return fileDescriptor_92b2514a05d2a2db, []int{1}
}
func (m *AbsenceOp) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *AbsenceOp) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_AbsenceOp.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *AbsenceOp) XXX_Merge(src proto.Message) {
	xxx_messageInfo_AbsenceOp.Merge(m, src)
}
func (m *AbsenceOp) XXX_Size() int {
	return m.Size()
}
func (m *AbsenceOp) XXX_DiscardUnknown() {
	xxx_messageInfo_AbsenceOp.DiscardUnknown(m)
}

var xxx_messageInfo_AbsenceOp proto.InternalMessageInfo

func (m *AbsenceOp) GetProof() *RangeProof {
	if m != nil {
		return m.Proof
	}
	return nil
}

// RangeProof is a Protobuf representation of iavl.RangeProof.
type RangeProof struct {
	LeftPath   []*ProofInnerNode `protobuf:"bytes,1,rep,name=left_path,json=leftPath,proto3" json:"left_path,omitempty"`
	InnerNodes []*PathToLeaf     `protobuf:"bytes,2,rep,name=inner_nodes,json=innerNodes,proto3" json:"inner_nodes,omitempty"`
	Leaves     []*ProofLeafNode  `protobuf:"bytes,3,rep,name=leaves,proto3" json:"leaves,omitempty"`
}

func (m *RangeProof) Reset()         { *m = RangeProof{} }
func (m *RangeProof) String() string { return proto.CompactTextString(m) }
func (*RangeProof) ProtoMessage()    {}
func (*RangeProof) Descriptor() ([]byte, []int) {
	return fileDescriptor_92b2514a05d2a2db, []int{2}
}
func (m *RangeProof) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *RangeProof) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_RangeProof.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *RangeProof) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RangeProof.Merge(m, src)
}
func (m *RangeProof) XXX_Size() int {
	return m.Size()
}
func (m *RangeProof) XXX_DiscardUnknown() {
	xxx_messageInfo_RangeProof.DiscardUnknown(m)
}

var xxx_messageInfo_RangeProof proto.InternalMessageInfo

func (m *RangeProof) GetLeftPath() []*ProofInnerNode {
	if m != nil {
		return m.LeftPath
	}
	return nil
}

func (m *RangeProof) GetInnerNodes() []*PathToLeaf {
	if m != nil {
		return m.InnerNodes
	}
	return nil
}

func (m *RangeProof) GetLeaves() []*ProofLeafNode {
	if m != nil {
		return m.Leaves
	}
	return nil
}

// PathToLeaf is a Protobuf representation of iavl.PathToLeaf.
type PathToLeaf struct {
	Inners []*ProofInnerNode `protobuf:"bytes,1,rep,name=inners,proto3" json:"inners,omitempty"`
}

func (m *PathToLeaf) Reset()         { *m = PathToLeaf{} }
func (m *PathToLeaf) String() string { return proto.CompactTextString(m) }
func (*PathToLeaf) ProtoMessage()    {}
func (*PathToLeaf) Descriptor() ([]byte, []int) {
	return fileDescriptor_92b2514a05d2a2db, []int{3}
}
func (m *PathToLeaf) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *PathToLeaf) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_PathToLeaf.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *PathToLeaf) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PathToLeaf.Merge(m, src)
}
func (m *PathToLeaf) XXX_Size() int {
	return m.Size()
}
func (m *PathToLeaf) XXX_DiscardUnknown() {
	xxx_messageInfo_PathToLeaf.DiscardUnknown(m)
}

var xxx_messageInfo_PathToLeaf proto.InternalMessageInfo

func (m *PathToLeaf) GetInners() []*ProofInnerNode {
	if m != nil {
		return m.Inners
	}
	return nil
}

// ProofInnerNode is a Protobuf representation of iavl.ProofInnerNode.
type ProofInnerNode struct {
	Height  int32  `protobuf:"zigzag32,1,opt,name=height,proto3" json:"height,omitempty"`
	Size_   int64  `protobuf:"varint,2,opt,name=size,proto3" json:"size,omitempty"`
	Version int64  `protobuf:"varint,3,opt,name=version,proto3" json:"version,omitempty"`
	Left    []byte `protobuf:"bytes,4,opt,name=left,proto3" json:"left,omitempty"`
	Right   []byte `protobuf:"bytes,5,opt,name=right,proto3" json:"right,omitempty"`
}

func (m *ProofInnerNode) Reset()         { *m = ProofInnerNode{} }
func (m *ProofInnerNode) String() string { return proto.CompactTextString(m) }
func (*ProofInnerNode) ProtoMessage()    {}
func (*ProofInnerNode) Descriptor() ([]byte, []int) {
	return fileDescriptor_92b2514a05d2a2db, []int{4}
}
func (m *ProofInnerNode) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *ProofInnerNode) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_ProofInnerNode.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *ProofInnerNode) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ProofInnerNode.Merge(m, src)
}
func (m *ProofInnerNode) XXX_Size() int {
	return m.Size()
}
func (m *ProofInnerNode) XXX_DiscardUnknown() {
	xxx_messageInfo_ProofInnerNode.DiscardUnknown(m)
}

var xxx_messageInfo_ProofInnerNode proto.InternalMessageInfo

func (m *ProofInnerNode) GetHeight() int32 {
	if m != nil {
		return m.Height
	}
	return 0
}

func (m *ProofInnerNode) GetSize_() int64 {
	if m != nil {
		return m.Size_
	}
	return 0
}

func (m *ProofInnerNode) GetVersion() int64 {
	if m != nil {
		return m.Version
	}
	return 0
}

func (m *ProofInnerNode) GetLeft() []byte {
	if m != nil {
		return m.Left
	}
	return nil
}

func (m *ProofInnerNode) GetRight() []byte {
	if m != nil {
		return m.Right
	}
	return nil
}

// ProofLeafNode is a Protobuf representation of iavl.ProofInnerNode.
type ProofLeafNode struct {
	Key       []byte `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	ValueHash []byte `protobuf:"bytes,2,opt,name=value_hash,json=valueHash,proto3" json:"value_hash,omitempty"`
	Version   int64  `protobuf:"varint,3,opt,name=version,proto3" json:"version,omitempty"`
}

func (m *ProofLeafNode) Reset()         { *m = ProofLeafNode{} }
func (m *ProofLeafNode) String() string { return proto.CompactTextString(m) }
func (*ProofLeafNode) ProtoMessage()    {}
func (*ProofLeafNode) Descriptor() ([]byte, []int) {
	return fileDescriptor_92b2514a05d2a2db, []int{5}
}
func (m *ProofLeafNode) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *ProofLeafNode) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_ProofLeafNode.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *ProofLeafNode) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ProofLeafNode.Merge(m, src)
}
func (m *ProofLeafNode) XXX_Size() int {
	return m.Size()
}
func (m *ProofLeafNode) XXX_DiscardUnknown() {
	xxx_messageInfo_ProofLeafNode.DiscardUnknown(m)
}

var xxx_messageInfo_ProofLeafNode proto.InternalMessageInfo

func (m *ProofLeafNode) GetKey() []byte {
	if m != nil {
		return m.Key
	}
	return nil
}

func (m *ProofLeafNode) GetValueHash() []byte {
	if m != nil {
		return m.ValueHash
	}
	return nil
}

func (m *ProofLeafNode) GetVersion() int64 {
	if m != nil {
		return m.Version
	}
	return 0
}

func init() {
	proto.RegisterType((*ValueOp)(nil), "iavl.ValueOp")
	proto.RegisterType((*AbsenceOp)(nil), "iavl.AbsenceOp")
	proto.RegisterType((*RangeProof)(nil), "iavl.RangeProof")
	proto.RegisterType((*PathToLeaf)(nil), "iavl.PathToLeaf")
	proto.RegisterType((*ProofInnerNode)(nil), "iavl.ProofInnerNode")
	proto.RegisterType((*ProofLeafNode)(nil), "iavl.ProofLeafNode")
}

func init() { proto.RegisterFile("iavl/proof.proto", fileDescriptor_92b2514a05d2a2db) }

var fileDescriptor_92b2514a05d2a2db = []byte{
	// 373 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x94, 0x92, 0xb1, 0x4e, 0xe3, 0x40,
	0x10, 0x86, 0xb3, 0x71, 0xec, 0x5c, 0x26, 0xb9, 0x53, 0x6e, 0x2f, 0x3a, 0x6d, 0x73, 0x3e, 0xcb,
	0x05, 0xb2, 0x04, 0x0a, 0x4a, 0xd2, 0xd1, 0x41, 0x05, 0x12, 0x82, 0x68, 0x85, 0x28, 0xd2, 0x44,
	0x1b, 0xb2, 0x89, 0x2d, 0x2c, 0xaf, 0xe5, 0x35, 0x96, 0xa0, 0xe2, 0x11, 0x78, 0x03, 0x5e, 0x87,
	0x32, 0x25, 0x25, 0x4a, 0x5e, 0x04, 0xed, 0xc4, 0x51, 0x48, 0x01, 0x12, 0x95, 0x67, 0xfe, 0xf9,
	0x66, 0xfe, 0xf1, 0xd8, 0xd0, 0x8e, 0x44, 0x11, 0x1f, 0xa6, 0x99, 0x52, 0xb3, 0x6e, 0x9a, 0xa9,
	0x5c, 0xd1, 0x9a, 0x51, 0xfc, 0x1e, 0xd4, 0xaf, 0x45, 0x7c, 0x27, 0x2f, 0x53, 0xba, 0x07, 0x36,
	0xd6, 0x19, 0xf1, 0x48, 0xd0, 0xec, 0xb7, 0xbb, 0x06, 0xe8, 0x72, 0x91, 0xcc, 0xe5, 0xd0, 0xe8,
	0x7c, 0x5d, 0xf6, 0x07, 0xd0, 0x38, 0x9e, 0x68, 0x99, 0xdc, 0x7c, 0xa7, 0xe9, 0x99, 0x00, 0x6c,
	0x55, 0xda, 0x83, 0x46, 0x2c, 0x67, 0xf9, 0x38, 0x15, 0x79, 0xc8, 0x88, 0x67, 0x05, 0xcd, 0x7e,
	0x67, 0xdd, 0x8a, 0xf5, 0xb3, 0x24, 0x91, 0xd9, 0x85, 0x9a, 0x4a, 0xfe, 0xc3, 0x60, 0x43, 0x91,
	0x87, 0xb4, 0x07, 0xcd, 0xc8, 0xc8, 0xe3, 0x44, 0x4d, 0xa5, 0x66, 0x55, 0x6c, 0x2a, 0xfd, 0x0c,
	0x70, 0xa5, 0xce, 0xa5, 0x98, 0x71, 0x88, 0x36, 0xbd, 0x9a, 0xee, 0x83, 0x13, 0x4b, 0x51, 0x48,
	0xcd, 0x2c, 0xa4, 0xff, 0x7c, 0xb0, 0x30, 0x30, 0x3a, 0x94, 0x88, 0x7f, 0x04, 0xb0, 0x1d, 0x43,
	0x0f, 0xc0, 0xc1, 0x41, 0xfa, 0xcb, 0xed, 0x4a, 0xc6, 0x7f, 0x24, 0xf0, 0x6b, 0xb7, 0x44, 0xff,
	0x82, 0x13, 0xca, 0x68, 0x1e, 0xe6, 0x78, 0x99, 0xdf, 0xbc, 0xcc, 0x28, 0x85, 0x9a, 0x8e, 0x1e,
	0x24, 0xab, 0x7a, 0x24, 0xb0, 0x38, 0xc6, 0x94, 0x41, 0xbd, 0x90, 0x99, 0x8e, 0x54, 0xc2, 0x2c,
	0x94, 0x37, 0xa9, 0xa1, 0xcd, 0x01, 0x58, 0xcd, 0x23, 0x41, 0x8b, 0x63, 0x4c, 0x3b, 0x60, 0x67,
	0x38, 0xd8, 0x46, 0x71, 0x9d, 0xf8, 0x23, 0xf8, 0xb9, 0xf3, 0x5e, 0xb4, 0x0d, 0xd6, 0xad, 0xbc,
	0x47, 0xf7, 0x16, 0x37, 0x21, 0xfd, 0x07, 0x50, 0x98, 0x6f, 0x3d, 0x0e, 0x85, 0x0e, 0x71, 0x81,
	0x16, 0x6f, 0xa0, 0x72, 0x2a, 0x74, 0xf8, 0xf9, 0x16, 0x27, 0xff, 0x5f, 0x96, 0x2e, 0x59, 0x2c,
	0x5d, 0xf2, 0xb6, 0x74, 0xc9, 0xd3, 0xca, 0xad, 0x2c, 0x56, 0x6e, 0xe5, 0x75, 0xe5, 0x56, 0x46,
	0x36, 0xfe, 0x4b, 0x13, 0x07, 0x1f, 0x83, 0xf7, 0x00, 0x00, 0x00, 0xff, 0xff, 0x9e, 0x60, 0x25,
	0x89, 0x66, 0x02, 0x00, 0x00,
}

func (m *ValueOp) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *ValueOp) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *ValueOp) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.Proof != nil {
		{
			size, err := m.Proof.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintProof(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *AbsenceOp) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *AbsenceOp) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *AbsenceOp) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.Proof != nil {
		{
			size, err := m.Proof.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintProof(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *RangeProof) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *RangeProof) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *RangeProof) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Leaves) > 0 {
		for iNdEx := len(m.Leaves) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := m.Leaves[iNdEx].MarshalToSizedBuffer(dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintProof(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0x1a
		}
	}
	if len(m.InnerNodes) > 0 {
		for iNdEx := len(m.InnerNodes) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := m.InnerNodes[iNdEx].MarshalToSizedBuffer(dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintProof(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0x12
		}
	}
	if len(m.LeftPath) > 0 {
		for iNdEx := len(m.LeftPath) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := m.LeftPath[iNdEx].MarshalToSizedBuffer(dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintProof(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0xa
		}
	}
	return len(dAtA) - i, nil
}

func (m *PathToLeaf) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *PathToLeaf) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *PathToLeaf) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Inners) > 0 {
		for iNdEx := len(m.Inners) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := m.Inners[iNdEx].MarshalToSizedBuffer(dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintProof(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0xa
		}
	}
	return len(dAtA) - i, nil
}

func (m *ProofInnerNode) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *ProofInnerNode) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *ProofInnerNode) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Right) > 0 {
		i -= len(m.Right)
		copy(dAtA[i:], m.Right)
		i = encodeVarintProof(dAtA, i, uint64(len(m.Right)))
		i--
		dAtA[i] = 0x2a
	}
	if len(m.Left) > 0 {
		i -= len(m.Left)
		copy(dAtA[i:], m.Left)
		i = encodeVarintProof(dAtA, i, uint64(len(m.Left)))
		i--
		dAtA[i] = 0x22
	}
	if m.Version != 0 {
		i = encodeVarintProof(dAtA, i, uint64(m.Version))
		i--
		dAtA[i] = 0x18
	}
	if m.Size_ != 0 {
		i = encodeVarintProof(dAtA, i, uint64(m.Size_))
		i--
		dAtA[i] = 0x10
	}
	if m.Height != 0 {
		i = encodeVarintProof(dAtA, i, uint64((uint32(m.Height)<<1)^uint32((m.Height>>31))))
		i--
		dAtA[i] = 0x8
	}
	return len(dAtA) - i, nil
}

func (m *ProofLeafNode) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *ProofLeafNode) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *ProofLeafNode) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.Version != 0 {
		i = encodeVarintProof(dAtA, i, uint64(m.Version))
		i--
		dAtA[i] = 0x18
	}
	if len(m.ValueHash) > 0 {
		i -= len(m.ValueHash)
		copy(dAtA[i:], m.ValueHash)
		i = encodeVarintProof(dAtA, i, uint64(len(m.ValueHash)))
		i--
		dAtA[i] = 0x12
	}
	if len(m.Key) > 0 {
		i -= len(m.Key)
		copy(dAtA[i:], m.Key)
		i = encodeVarintProof(dAtA, i, uint64(len(m.Key)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func encodeVarintProof(dAtA []byte, offset int, v uint64) int {
	offset -= sovProof(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *ValueOp) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Proof != nil {
		l = m.Proof.Size()
		n += 1 + l + sovProof(uint64(l))
	}
	return n
}

func (m *AbsenceOp) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Proof != nil {
		l = m.Proof.Size()
		n += 1 + l + sovProof(uint64(l))
	}
	return n
}

func (m *RangeProof) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if len(m.LeftPath) > 0 {
		for _, e := range m.LeftPath {
			l = e.Size()
			n += 1 + l + sovProof(uint64(l))
		}
	}
	if len(m.InnerNodes) > 0 {
		for _, e := range m.InnerNodes {
			l = e.Size()
			n += 1 + l + sovProof(uint64(l))
		}
	}
	if len(m.Leaves) > 0 {
		for _, e := range m.Leaves {
			l = e.Size()
			n += 1 + l + sovProof(uint64(l))
		}
	}
	return n
}

func (m *PathToLeaf) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if len(m.Inners) > 0 {
		for _, e := range m.Inners {
			l = e.Size()
			n += 1 + l + sovProof(uint64(l))
		}
	}
	return n
}

func (m *ProofInnerNode) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Height != 0 {
		n += 1 + sozProof(uint64(m.Height))
	}
	if m.Size_ != 0 {
		n += 1 + sovProof(uint64(m.Size_))
	}
	if m.Version != 0 {
		n += 1 + sovProof(uint64(m.Version))
	}
	l = len(m.Left)
	if l > 0 {
		n += 1 + l + sovProof(uint64(l))
	}
	l = len(m.Right)
	if l > 0 {
		n += 1 + l + sovProof(uint64(l))
	}
	return n
}

func (m *ProofLeafNode) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Key)
	if l > 0 {
		n += 1 + l + sovProof(uint64(l))
	}
	l = len(m.ValueHash)
	if l > 0 {
		n += 1 + l + sovProof(uint64(l))
	}
	if m.Version != 0 {
		n += 1 + sovProof(uint64(m.Version))
	}
	return n
}

func sovProof(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozProof(x uint64) (n int) {
	return sovProof(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *ValueOp) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowProof
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
			return fmt.Errorf("proto: ValueOp: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: ValueOp: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Proof", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowProof
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
				return ErrInvalidLengthProof
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthProof
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Proof == nil {
				m.Proof = &RangeProof{}
			}
			if err := m.Proof.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipProof(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthProof
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
func (m *AbsenceOp) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowProof
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
			return fmt.Errorf("proto: AbsenceOp: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: AbsenceOp: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Proof", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowProof
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
				return ErrInvalidLengthProof
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthProof
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Proof == nil {
				m.Proof = &RangeProof{}
			}
			if err := m.Proof.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipProof(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthProof
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
func (m *RangeProof) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowProof
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
			return fmt.Errorf("proto: RangeProof: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: RangeProof: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field LeftPath", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowProof
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
				return ErrInvalidLengthProof
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthProof
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.LeftPath = append(m.LeftPath, &ProofInnerNode{})
			if err := m.LeftPath[len(m.LeftPath)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field InnerNodes", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowProof
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
				return ErrInvalidLengthProof
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthProof
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.InnerNodes = append(m.InnerNodes, &PathToLeaf{})
			if err := m.InnerNodes[len(m.InnerNodes)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Leaves", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowProof
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
				return ErrInvalidLengthProof
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthProof
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Leaves = append(m.Leaves, &ProofLeafNode{})
			if err := m.Leaves[len(m.Leaves)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipProof(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthProof
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
func (m *PathToLeaf) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowProof
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
			return fmt.Errorf("proto: PathToLeaf: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: PathToLeaf: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Inners", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowProof
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
				return ErrInvalidLengthProof
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthProof
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Inners = append(m.Inners, &ProofInnerNode{})
			if err := m.Inners[len(m.Inners)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipProof(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthProof
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
func (m *ProofInnerNode) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowProof
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
			return fmt.Errorf("proto: ProofInnerNode: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: ProofInnerNode: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Height", wireType)
			}
			var v int32
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowProof
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				v |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			v = int32((uint32(v) >> 1) ^ uint32(((v&1)<<31)>>31))
			m.Height = v
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Size_", wireType)
			}
			m.Size_ = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowProof
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Size_ |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Version", wireType)
			}
			m.Version = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowProof
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
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Left", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowProof
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
				return ErrInvalidLengthProof
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthProof
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Left = append(m.Left[:0], dAtA[iNdEx:postIndex]...)
			if m.Left == nil {
				m.Left = []byte{}
			}
			iNdEx = postIndex
		case 5:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Right", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowProof
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
				return ErrInvalidLengthProof
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthProof
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Right = append(m.Right[:0], dAtA[iNdEx:postIndex]...)
			if m.Right == nil {
				m.Right = []byte{}
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipProof(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthProof
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
func (m *ProofLeafNode) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowProof
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
			return fmt.Errorf("proto: ProofLeafNode: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: ProofLeafNode: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Key", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowProof
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
				return ErrInvalidLengthProof
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthProof
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Key = append(m.Key[:0], dAtA[iNdEx:postIndex]...)
			if m.Key == nil {
				m.Key = []byte{}
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field ValueHash", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowProof
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
				return ErrInvalidLengthProof
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthProof
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.ValueHash = append(m.ValueHash[:0], dAtA[iNdEx:postIndex]...)
			if m.ValueHash == nil {
				m.ValueHash = []byte{}
			}
			iNdEx = postIndex
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Version", wireType)
			}
			m.Version = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowProof
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
		default:
			iNdEx = preIndex
			skippy, err := skipProof(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthProof
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
func skipProof(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowProof
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
					return 0, ErrIntOverflowProof
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
					return 0, ErrIntOverflowProof
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
				return 0, ErrInvalidLengthProof
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupProof
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthProof
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthProof        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowProof          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupProof = fmt.Errorf("proto: unexpected end of group")
)
