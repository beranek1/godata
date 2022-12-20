package godata

import (
	"testing"
)

func checkNode(t *testing.T, node *DataNodeRBT, height uint, name string, color RBColor) {
	t.Log("Checking node (", node, ",", height, ",", name, ",", color, ")")
	if node == nil {
		t.Error("Node is nil.")
	} else if node.Height() != height {
		t.Error("Subtree has height other than ", height, ": ", node.Height())
	} else if node.GetName() != name {
		t.Error("Node has wrong name: ", node.GetName())
	} else if node.Color != color {
		t.Error("Node has wrong color: ", node.Color)
	}
}

func checkNodeIgnoreColor(t *testing.T, node *DataNodeRBT, height uint, name string) {
	t.Log("Checking node (", node, ",", height, ",", name, ",)")
	if node == nil {
		t.Error("Node is nil.")
	} else if node.Height() != height {
		t.Error("Subtree has height other than ", height, ": ", node.Height())
	} else if node.GetName() != name {
		t.Error("Node has wrong name: ", node.GetName())
	}
}

func TestCreateDataNode(t *testing.T) {
	root := CreateDataNode("b", "root", 0, false, nil, nil, nil)
	if root.Height() != 1 {
		t.Error("Tree with single node has height other than 1")
	}
	left := CreateDataNode("a", "left", 0, false, root, nil, nil)
	root.Left = left
	if root.Height() != 2 {
		t.Error("Tree with two nodes has height other than 2: ", root.Height())
	}
	right := CreateDataNode("c", "right", 0, false, root, nil, nil)
	root.Right = right
	if root.Height() != 2 {
		t.Error("Tree with one root and l+r children has height other than 2:", root.Height())
	}
}

func TestBalanceThree(t *testing.T) {
	var root = CreateDataNode("c", "root", 0, true, nil, nil, nil)
	left := CreateDataNode("b", "left", 0, true, root, nil, nil)
	leftleft := CreateDataNode("a", "leftleft", 0, true, left, nil, nil)
	root.Left = left
	root = Balance(root, left)
	if root.Height() != 2 {
		t.Error("Tree with two nodes has height other than 2: ", root.Height())
	}
	left.Left = leftleft
	root = Balance(root, leftleft)
	if root.Height() != 2 {
		t.Error("Tree with two nodes has height other than 2: ", root.Height())
	}
	checkNode(t, root.Left, 1, "a", true)
	checkNode(t, root.Right, 1, "c", true)

	root = CreateDataNode("a", "root", 0, true, nil, nil, nil)
	right := CreateDataNode("b", "right", 0, true, root, nil, nil)
	rightright := CreateDataNode("c", "rightright", 0, true, right, nil, nil)
	root.Right = right
	root = Balance(root, right)
	if root.Height() != 2 {
		t.Error("Tree with two nodes has height other than 2: ", root.Height())
	}
	right.Right = rightright
	root = Balance(root, rightright)
	if root.Height() != 2 {
		t.Error("Tree with two nodes has height other than 2: ", root.Height())
	}

	checkNode(t, root.Left, 1, "a", true)
	checkNode(t, root.Right, 1, "c", true)

}

func TestBalanceSeven(t *testing.T) {
	root := CreateDataNode("d", "root", 0, true, nil, nil, nil)
	left := CreateDataNode("a", "left", 0, true, root, nil, nil)
	root.Left = left
	println("Balance 1")
	root = Balance(root, left)
	right := CreateDataNode("g", "right", 0, true, root, nil, nil)
	root.Right = right
	println("Balance 2")
	root = Balance(root, right)
	leftright := CreateDataNode("b", "leftright", 0, true, left, nil, nil)
	left.Right = leftright
	println("Balance 3")
	root = Balance(root, leftright)
	checkNodeIgnoreColor(t, root, 3, "d")
	checkNodeIgnoreColor(t, root.Left, 2, "a")
	checkNodeIgnoreColor(t, root.Right, 1, "g")

	leftrightright := CreateDataNode("c", "leftrightright", 0, true, leftright, nil, nil)
	leftright.Right = leftrightright
	root = Balance(root, leftrightright)
	rightleft := CreateDataNode("f", "rightleft", 0, true, right, nil, nil)
	right.Right = rightleft
	root = Balance(root, rightleft)
	rightleftleft := CreateDataNode("e", "rightleftleft", 0, true, rightleft, nil, nil)
	rightleft.Right = rightleftleft
	root = Balance(root, rightleftleft)
	checkNodeIgnoreColor(t, root, 3, "d")
	checkNodeIgnoreColor(t, root.Left, 2, "b")
	checkNodeIgnoreColor(t, root.Right, 2, "f")
}
