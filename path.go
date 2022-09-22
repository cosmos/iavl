package iavl

import (
	"encoding/binary"
)

type Path struct {
	Depth uint8
	// the depth of the node counting from root, or the number of directions to take
	// to get to the node from root

	// the binary representation of Directions shows the directions to the node from root
	// 1 means right direction and 0 means left direction
	// for example Path with Depth = 4 and Directions = 13 (.......1101)
	// means that we have to take 4 directions from the root that is right, left, right, right
	Directions uint64
}

func (p Path) MakePathToRightChild() Path {
	return Path{
		Depth:      p.Depth + 1,
		Directions: p.Directions | (1 << p.Depth),
	}
}

func (p Path) MakePathToLeftChild() Path {
	return Path{
		Depth:      p.Depth + 1,
		Directions: p.Directions,
	}
}

func (p Path) Bytes() []byte {
	b := make([]byte, 9)

	b[0] = p.Depth
	binary.BigEndian.PutUint64(b[1:], p.Directions)
	return b
}
