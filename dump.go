package iavl

import (
	"encoding/binary"
	"fmt"
	"io"
)

type dumper struct {
	keyDump  io.Writer
	keyPos   int
	nodeDump io.Writer
	nodeSeq  uint32
}

func newDumper(keyDump io.Writer, nodeDump io.Writer) *dumper {
	return &dumper{
		keyDump:  keyDump,
		nodeDump: nodeDump,
	}
}

func (d *dumper) DumpNode(node *Node) error {
	d.nodeSeq++
	if d.nodeSeq != node.nodeKey.sequence {
		return fmt.Errorf("node sequence mismatch: %d != %d", d.nodeSeq, node.nodeKey.sequence)
	}

	bz, err := d.MarshalNode(node)
	if err != nil {
		return err
	}
	n, err := d.nodeDump.Write(bz[:])
	if err != nil {
		return err
	}
	if n != len(bz) {
		return io.ErrShortWrite
	}
	return nil
}

func (d *dumper) MarshalNode(node *Node) ([]byte, error) {
	keyOffset, err := d.DumpKey(node.key)
	if err != nil {
		return nil, err
	}

	var buf [77]byte
	copy(buf[:], node.nodeKey.GetKey())
	copy(buf[12:], node.leftNodeKey)
	copy(buf[24:], node.rightNodeKey)
	copy(buf[36:], node.hash)
	binary.BigEndian.PutUint32(buf[68:], uint32(node.size))
	binary.BigEndian.PutUint32(buf[72:], uint32(keyOffset))
	buf[76] = byte(node.subtreeHeight)
	return buf[:], nil
}

func (d *dumper) ReadNode(bz []byte) (*Node, error) {
	n := &Node{
		nodeKey:       GetNodeKey(bz[:12]),
		leftNodeKey:   bz[12:24],
		rightNodeKey:  bz[24:36],
		hash:          bz[36:68],
		size:          int64(binary.BigEndian.Uint32(bz[68:72])),
		subtreeHeight: int8(bz[76]),
	}
	return n, nil
}

func (d *dumper) DumpKey(key []byte) (int, error) {
	n, err := d.keyDump.Write(key)
	if err != nil {
		return n, err
	}
	if n != len(key) {
		return n, io.ErrShortWrite
	}
	d.keyPos += n
	return d.keyPos, nil
}
