package iavl

var right = uint64(1) << uint64(63)

type Path struct {
	Depth uint8

	// the binary representation of  indicate the directions to the node from root
	// 1 means right direction and 0 means left direction
	// for example 9 (......00000001001)
	//
	Directions uint64
}

func (p Path) MakePathToRightChild() Path {
	return Path{
		Depth:      p.Depth + 1,
		Directions: p.Directions | (right >> p.Depth),
	}
}

func (p Path) MakePathToLeftChild() Path {
	return Path{
		Depth:      p.Depth + 1,
		Directions: p.Directions,
	}
}

func (p Path) Bytes() {

}
