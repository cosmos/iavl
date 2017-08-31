package iavl

func dummyPathToKey(t *IAVLTree, key []byte) *PathToKey {
	path, _, err := t.root.pathToKey(t, key)
	if err != nil {
		panic(err)
	}
	return path
}

func dummyLeafNode(key, val []byte) IAVLProofLeafNode {
	return IAVLProofLeafNode{key, val}
}
