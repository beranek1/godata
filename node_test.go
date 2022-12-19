package godata

import (
	"testing"
)

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

func TestBalance(t *testing.T) {
	var root = CreateDataNode("c", "root", 0, true, nil, nil, nil)
	left := CreateDataNode("b", "left", 0, true, root, nil, nil)
	leftleft := CreateDataNode("a", "left", 0, true, left, nil, nil)
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

	if root.GetLeft() == nil {
		t.Error("Root left child is nil.")
	} else if root.GetLeft().GetName() != "a" {
		t.Error("Wrong root left child.")
	} else if root.GetLeft().IsBlack() {
		t.Error("Wrong root left child color.")
	}

	if root.GetRight() == nil {
		t.Error("Root right child is nil.")
	} else if root.GetRight().GetName() != "c" {
		t.Error("Wrong root right child.")
	} else if root.GetRight().IsBlack() {
		t.Error("Wrong root right child color.")
	}
}
