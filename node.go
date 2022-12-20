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
	for parent != nil {
		if parent.IsBlack() {
			return root
		}
		gparent = parent.GetParent()
		if gparent == nil {
			parent.MakeBlack()
			return root
		}

		dir := true
		tmp = gparent.GetRight()
		if parent == tmp {
			tmp = gparent.GetLeft()
			dir = false
		}
		if tmp == nil || tmp.IsBlack() {
			if dir && node == parent.GetRight() {
				rotate(root, parent, dir)
				node = parent
				parent = gparent.GetLeft()
			} else if !dir && node == parent.GetLeft() {
				rotate(root, parent, dir)
				node = parent
				parent = gparent.GetRight()
			}
			root = rotate(root, gparent, !dir)
			parent.MakeBlack()
			gparent.MakeRed()
			return root
		}
		parent.MakeBlack()
		tmp.MakeBlack()
		gparent.MakeRed()
		node = gparent
	}
	return root
}

func rotate(root *DataNodeRBT, parent *DataNodeRBT, dir bool) *DataNodeRBT {
	gparent := parent.GetParent()
	var s, c *DataNodeRBT
	if dir {
		s = parent.GetRight()
		if s != nil {
			c = s.GetLeft()
			parent.SetRight(c)
			if c != nil {
				c.SetParent(parent)
			}
			s.SetLeft(parent)
		}
	} else {
		s = parent.GetLeft()
		if s != nil {
			c = s.GetRight()
			parent.SetLeft(c)
			if c != nil {
				c.SetParent(parent)
			}
			s.SetRight(parent)
		}
	}
	if s != nil {
		parent.SetParent(s)
		s.SetParent(gparent)
		if gparent != nil {
			if parent == gparent.GetRight() {
				gparent.SetRight(s)
			} else {
				gparent.SetLeft(s)
			}
		} else {
			root = s
		}
	}
	return root
}
