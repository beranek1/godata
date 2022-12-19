package godata

type DataNode interface {
	InsertDataAt(string, any, int64) DataNode
	GetData(string) any
	GetDataAt(string, int64) any
	DeleteVersionsAt(int64)
}

type DataRedBlackTree interface {
	DataNode
	RBTInsertDataAt(string, any, int64) DataRedBlackTree
	RBTDeleteVersionsAt(int64)
	GetName() string
	GetLeft() DataRedBlackTree
	GetRight() DataRedBlackTree
	GetParent() DataRedBlackTree
	SetLeft(DataRedBlackTree)
	SetRight(DataRedBlackTree)
	SetParent(DataRedBlackTree)
	MakeRed()
	MakeBlack()
	IsRed() bool
	IsBlack() bool
	Height() uint
}

type DataNodeRBT struct {
	color   bool
	left    *DataNodeRBT
	name    string
	parent  *DataNodeRBT
	right   *DataNodeRBT
	version DataVersion
}

func CreateDataNode(name string, data any, timestamp int64, color bool, parent *DataNodeRBT, left *DataNodeRBT, right *DataNodeRBT) *DataNodeRBT {
	dv := CreateDataVersion().InsertDataAt(data, timestamp)
	return &DataNodeRBT{color, left, name, parent, right, dv}
}

func (dn *DataNodeRBT) InsertDataAt(name string, data any, timestamp int64) DataNode {
	return dn.RBTInsertDataAt(name, data, timestamp)
}

func (dn *DataNodeRBT) RBTInsertDataAt(name string, data any, timestamp int64) *DataNodeRBT {
	if dn.name == name {
		dn.version = dn.version.InsertDataAt(data, timestamp)
		return nil
	} else {
		if dn.name > name {
			if dn.left == nil {
				dn.left = CreateDataNode(name, data, timestamp, true, dn, nil, nil)
				return dn.left
			} else {
				return dn.left.RBTInsertDataAt(name, data, timestamp)
			}
		} else {
			if dn.right == nil {
				dn.right = CreateDataNode(name, data, timestamp, true, dn, nil, nil)
				return dn.right
			} else {
				return dn.right.RBTInsertDataAt(name, data, timestamp)
			}
		}
	}
}

func (dn *DataNodeRBT) GetData(name string) any {
	if dn.name == name {
		return dn.version.GetData()
	} else if dn.name > name {
		if dn.left == nil {
			return nil
		}
		return dn.left.GetData(name)
	} else {
		if dn.right == nil {
			return nil
		}
		return dn.right.GetData(name)
	}
}

func (dn *DataNodeRBT) GetDataAt(name string, timestamp int64) any {
	if dn.name == name {
		return dn.version.GetDataAt(timestamp)
	} else if dn.name > name {
		if dn.left == nil {
			return nil
		}
		return dn.left.GetDataAt(name, timestamp)
	} else {
		if dn.right == nil {
			return nil
		}
		return dn.right.GetDataAt(name, timestamp)
	}
}

func (dn *DataNodeRBT) DeleteVersionsAt(timestamp int64) {
	dn.RBTDeleteVersionsAt(timestamp)
}

func (dn *DataNodeRBT) RBTDeleteVersionsAt(timestamp int64) {
	if dn.left != nil {
		dn.left.RBTDeleteVersionsAt(timestamp)
	}
	if dn.right != nil {
		dn.right.RBTDeleteVersionsAt(timestamp)
	}
	dn.version = dn.version.DeleteVersionsAt(timestamp)
}

func (dn *DataNodeRBT) GetName() string {
	return dn.name
}

func (dn *DataNodeRBT) GetLeft() *DataNodeRBT {
	return dn.left
}

func (dn *DataNodeRBT) GetRight() *DataNodeRBT {
	return dn.right
}

func (dn *DataNodeRBT) GetParent() *DataNodeRBT {
	return dn.parent
}

func (dn *DataNodeRBT) SetLeft(left *DataNodeRBT) {
	dn.left = left
}

func (dn *DataNodeRBT) SetRight(right *DataNodeRBT) {
	dn.right = right
}

func (dn *DataNodeRBT) SetParent(parent *DataNodeRBT) {
	dn.parent = parent
}

func (dn *DataNodeRBT) MakeRed() {
	dn.color = true
}

func (dn *DataNodeRBT) MakeBlack() {
	dn.color = false
}

func (dn *DataNodeRBT) IsRed() bool {
	return dn.color
}

func (dn *DataNodeRBT) IsBlack() bool {
	return !dn.IsRed()
}

func (dn *DataNodeRBT) Height() uint {
	if dn.left != nil {
		hleft := dn.left.Height()
		if dn.right != nil {
			hright := dn.right.Height()
			if hleft > hright {
				return 1 + hleft
			} else {
				return 1 + hright
			}
		} else {
			return 1 + hleft
		}
	} else if dn.right != nil {
		hright := dn.right.Height()
		return 1 + hright
	} else {
		return 1
	}
}

func Balance(root *DataNodeRBT, node *DataNodeRBT) *DataNodeRBT {
	var parent = node.GetParent()
	var gparent, tmp *DataNodeRBT
	for {
		if parent == nil {
			node.MakeBlack()
			return node
		} else if parent.IsBlack() {
			return root
		}
		gparent = parent.GetParent()
		if gparent == nil {
			parent.MakeBlack()
			return root
		}
		tmp = gparent.GetRight()
		if parent != tmp {
			if tmp != nil && tmp.IsRed() {
				tmp.MakeBlack()
				parent.MakeBlack()
				node = gparent
				parent = node.GetParent()
				parent.MakeRed()
				continue
			}

			tmp = parent.GetRight()
			if node == tmp {
				tmp = node.GetLeft()
				parent.SetRight(tmp)
				if tmp != nil {
					tmp.MakeBlack()
					tmp.SetParent(parent)
				}
				node.SetLeft(parent)
				parent.SetParent(node)
				node.SetParent(gparent)
				gparent.SetLeft(node)
				parent.MakeRed()
				parent = node
				tmp = node.GetRight()
			}
			gtmp := gparent.GetParent()
			gparent.SetLeft(tmp)
			if tmp != nil {
				tmp.MakeBlack()
				tmp.SetParent(gparent)
			}
			parent.SetRight(gparent)
			gparent.SetParent(parent)
			parent.SetParent(gtmp)
			// if gtmp != nil {
			// 	if gtmp.GetLeft() == gparent {
			// 		gtmp.SetLeft(parent)
			// 	} else {
			// 		gtmp.SetRight(parent)
			// 	}
			// }
			parent.MakeBlack()
			gparent.MakeRed()
			if gtmp == nil {
				return parent
			}
			break
		} else {
			tmp = gparent.GetLeft()
			if tmp != nil && tmp.IsRed() {
				tmp.MakeBlack()
				parent.MakeBlack()
				node = gparent
				parent = node.GetParent()
				parent.MakeRed()
				continue
			}

			tmp = parent.GetLeft()
			if node == tmp {
				tmp = node.GetRight()
				parent.SetLeft(tmp)
				if tmp != nil {
					tmp.MakeBlack()
					tmp.SetParent(parent)
				}
				node.SetRight(parent)
				parent.SetParent(node)
				if parent == gparent.GetLeft() {
					gparent.SetLeft(node)
				}
				node.SetParent(gparent)
				gparent.SetRight(node)
				parent.MakeRed()
				parent = node
				tmp = node.GetLeft()
			}
			gtmp := gparent.GetParent()
			gparent.SetRight(tmp)
			if tmp != nil {
				tmp.MakeBlack()
				tmp.SetParent(gparent)
			}
			parent.SetLeft(gparent)
			gparent.SetParent(parent)
			parent.SetParent(gtmp)
			// if gtmp != nil {
			// 	if gtmp.GetLeft() == gparent {
			// 		gtmp.SetLeft(parent)
			// 	} else {
			// 		gtmp.SetRight(parent)
			// 	}
			// }
			parent.MakeBlack()
			gparent.MakeRed()
			if gtmp == nil {
				return parent
			}
			break
		}
	}
	return root
}
