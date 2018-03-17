package iavl

import (
	"fmt"
)

func PrintTree(tree *Tree) {
	ndb, root := tree.ndb, tree.root
	printNode(ndb, root, 0)
}

func printNode(ndb *nodeDB, node *Node, indent int) {
	indentPrefix := ""
	for i := 0; i < indent; i++ {
		indentPrefix += "    "
	}

	if node == nil {
		fmt.Printf("%s<nil>\n", indentPrefix)
	} else if node.isLeaf() {
		fmt.Printf("%s%X = %X (%v)\n", indentPrefix, node.key, node.value, node.version)
	} else {
		if node.rightNode != nil {
			printNode(ndb, node.rightNode, indent+1)
		} else if node.rightHash != nil {
			printNode(ndb, ndb.GetNode(node.rightHash), indent+1)
		} else {
			fmt.Printf("%s    MISSING RIGHT NODE W/ HASH = %X\n", indentPrefix, node.rightHash)
		}

		fmt.Printf("%skey:%X height:%v (%v)\n", indentPrefix, node.key, node.height, node.version)

		if node.leftNode != nil {
			printNode(ndb, node.leftNode, indent+1)
		} else if node.leftHash != nil {
			printNode(ndb, ndb.GetNode(node.leftHash), indent+1)
		} else {
			fmt.Printf("%s    MISSING LEFT NODE W/ HASH = %X\n", indentPrefix, node.leftHash)
		}
	}

}

func maxInt8(a, b int8) int8 {
	if a > b {
		return a
	}
	return b
}
