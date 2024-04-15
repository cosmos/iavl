package iavl

import (
	"fmt"

	"github.com/emicklei/dot"
)

func writeDotGraph(root *Node, lastGraph *dot.Graph) *dot.Graph {
	graph := dot.NewGraph(dot.Directed)

	var traverse func(node *Node) dot.Node
	var i int
	traverse = func(node *Node) dot.Node {
		if node == nil {
			return dot.Node{}
		}
		i++
		nodeKey := fmt.Sprintf("%s-%d", node.key, node.subtreeHeight)
		nodeLabel := fmt.Sprintf("%s - %d", string(node.key), node.subtreeHeight)
		n := graph.Node(nodeKey).Label(nodeLabel)
		if _, found := lastGraph.FindNodeById(nodeKey); !found {
			n.Attr("color", "red")
		}
		if node.isLeaf() {
			return n
		}
		leftNode := traverse(node.leftNode)
		rightNode := traverse(node.rightNode)

		leftEdge := n.Edge(leftNode, "l")
		rightEdge := n.Edge(rightNode, "r")
		if edges := lastGraph.FindEdges(n, leftNode); len(edges) == 0 {
			leftEdge.Attr("color", "red")
		}
		if edges := lastGraph.FindEdges(n, rightNode); len(edges) == 0 {
			rightEdge.Attr("color", "red")
		}

		return n
	}

	traverse(root)
	return graph
}
