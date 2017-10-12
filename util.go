package iavl

import (
	"fmt"
)

func printIAVLNode(node *IAVLNode, indent int) {
	indentPrefix := ""
	for i := 0; i < indent; i++ {
		indentPrefix += "    "
	}

	if node.rightNode != nil {
		printIAVLNode(node.rightNode, indent+1)
	} else if node.rightHash != nil {
		fmt.Printf("%s    %X\n", indentPrefix, node.rightHash)
	}

	fmt.Printf("%s%v:%v\n", indentPrefix, node.key, node.height)

	if node.leftNode != nil {
		printIAVLNode(node.leftNode, indent+1)
	} else if node.leftHash != nil {
		fmt.Printf("%s    %X\n", indentPrefix, node.leftHash)
	}

}

func maxInt8(a, b int8) int8 {
	if a > b {
		return a
	}
	return b
}
