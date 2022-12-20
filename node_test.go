package godata

import (
	"testing"
)

func checkNode(t *testing.T, node *DataNodeRBT, height uint, name string, color bool) {
	t.Log("Checking node (", node, ",", height, ",", name, ",", color, ")")
	if node == nil {
		t.Error("Node is nil.")
	} else if node.Height() != height {
		t.Error("Subtree has height other than ", height, ": ", node.Height())
	} else if node.GetName() != name {
		t.Error("Node has wrong name: ", node.GetName())
	} else if node.IsRed() != color {
		t.Error("Node has wrong color: ", node.IsRed())
	}
}

func TestCreateDataNode(t *testing.T) {
	root := CreateDataNode("b", "root", 0, false, nil, nil, nil)
	if root.Height() != 1 {
		t.Error("Tree with single node has height other than 1")
	}
	left := CreateDataNode("a", "left", 0, false, root, nil, nil)
	root.SetLeft(left)
	if root.Height() != 2 {
		t.Error("Tree with two nodes has height other than 2: ", root.Height())
	}
	right := CreateDataNode("c", "right", 0, false, root, nil, nil)
	root.SetRight(right)
	if root.Height() != 2 {
		t.Error("Tree with one root and l+r children has height other than 2:", root.Height())
	}
}

func TestBalanceThree(t *testing.T) {
	var root = CreateDataNode("c", "root", 0, true, nil, nil, nil)
	left := CreateDataNode("b", "left", 0, true, root, nil, nil)
	leftleft := CreateDataNode("a", "leftleft", 0, true, left, nil, nil)
	root.SetLeft(left)
	root = Balance(root, left)
	if root.Height() != 2 {
		t.Error("Tree with two nodes has height other than 2: ", root.Height())
	}
	left.SetLeft(leftleft)
	root = Balance(root, leftleft)
	if root.Height() != 2 {
		t.Error("Tree with two nodes has height other than 2: ", root.Height())
	}
	checkNode(t, root.GetLeft(), 1, "a", true)
	checkNode(t, root.GetRight(), 1, "c", true)

	root = CreateDataNode("a", "root", 0, true, nil, nil, nil)
	right := CreateDataNode("b", "right", 0, true, root, nil, nil)
	rightright := CreateDataNode("c", "rightright", 0, true, right, nil, nil)
	root.SetRight(right)
	root = Balance(root, right)
	if root.Height() != 2 {
		t.Error("Tree with two nodes has height other than 2: ", root.Height())
	}
	right.SetRight(rightright)
	root = Balance(root, rightright)
	if root.Height() != 2 {
		t.Error("Tree with two nodes has height other than 2: ", root.Height())
	}

	checkNode(t, root.GetLeft(), 1, "a", true)
	checkNode(t, root.GetRight(), 1, "c", true)

}
func TestBalanceSeven(t *testing.T) {
	root := CreateDataNode("d", "root", 0, true, nil, nil, nil)
	left := CreateDataNode("a", "left", 0, true, root, nil, nil)
	root.SetLeft(left)
	println("Balance 1")
	root = Balance(root, left)
	right := CreateDataNode("g", "right", 0, true, root, nil, nil)
	root.SetRight(right)
	println("Balance 2")
	root = Balance(root, right)
	leftright := CreateDataNode("b", "leftright", 0, true, left, nil, nil)
	left.SetRight(leftright)
	println("Balance 3")
	root = Balance(root, leftright)
	if root.Height() != 3 {
		t.Error("Tree with 4 nodes has height other than 3: ", root.Height())
	}
	checkNode(t, root, 3, "d", false)

	checkNode(t, root.GetLeft(), 2, "a", true)
	checkNode(t, root.GetRight(), 1, "c", true)

	leftrightright := CreateDataNode("c", "leftrightright", 0, true, leftright, nil, nil)
	leftright.SetRight(leftrightright)
	root = Balance(root, leftrightright)
	rightleft := CreateDataNode("f", "rightleft", 0, true, right, nil, nil)
	right.SetRight(rightleft)
	root = Balance(root, rightleft)
	rightleftleft := CreateDataNode("e", "rightleftleft", 0, true, rightleft, nil, nil)
	rightleft.SetRight(rightleftleft)
	root = Balance(root, rightleftleft)
	if root.Height() != 3 {
		t.Error("Tree with 7 nodes has height other than 3: ", root.Height())
	}

	if root.GetLeft() == nil {
		t.Error("Root left child is nil.")
	} else if root.GetLeft().GetName() != "a" {
		t.Error("Wrong root left child: ", root.GetLeft().GetName())
	} else if root.GetLeft().IsBlack() {
		t.Error("Wrong root left child color.")
	}

	if root.GetRight() == nil {
		t.Error("Root right child is nil.")
	} else if root.GetRight().GetName() != "c" {
		t.Error("Wrong root right child: ", root.GetRight().GetName())
	} else if root.GetRight().IsBlack() {
		t.Error("Wrong root right child color.")
	}
}
