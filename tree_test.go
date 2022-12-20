package godata

import (
	"testing"
)

func checkTreeHeight(t *testing.T, tree *DataTreeRBT, height uint) {
	t.Log("Checking tree (", tree, height, ")")
	if tree == nil {
		t.Error("Tree creation failed.")
	} else if tree.Height() != height {
		t.Error("Wrong tree height: ", tree.Height())
	}
}

func TestCreateDataTree(t *testing.T) {
	tree := CreateDataTree()
	checkTreeHeight(t, tree, 0)
}

func TestTreeInsertDataAt(t *testing.T) {
	tree := CreateDataTree()
	tree.InsertDataAt("a", "a", 0)
	checkTreeHeight(t, tree, 1)
	tree.InsertDataAt("b", "b", 0)
	checkTreeHeight(t, tree, 2)
	tree.InsertDataAt("c", "c", 0)
	checkTreeHeight(t, tree, 2)
	tree.InsertDataAt("d", "d", 0)
	checkTreeHeight(t, tree, 3)
	tree.InsertDataAt("e", "e", 0)
	checkTreeHeight(t, tree, 3)
	tree.InsertDataAt("f", "f", 0)
	checkTreeHeight(t, tree, 4)
	tree.InsertDataAt("g", "g", 0)
	checkTreeHeight(t, tree, 4)
	tree.InsertDataAt("h", "h", 0)
	checkTreeHeight(t, tree, 4)
	tree.InsertDataAt("i", "i", 0)
	checkTreeHeight(t, tree, 4)
	tree.InsertDataAt("j", "j", 0)
	checkTreeHeight(t, tree, 5)
}
