package godata

type DataNode interface {
	InsertDataAt(string, any, int64) DataNode
	GetData(string) any
	GetDataAt(string, int64) any
	DeleteVersionsAt(int64) DataNode
}

type DataRedBlackTree interface {
	DataNode
	RBTInsertDataAt(string, any, int64) DataRedBlackTree
	RBTDeleteVersionsAt(int64) DataRedBlackTree
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
	IsLeaf() bool
	Height() uint
}

type DataNodeRBT struct {
	color   bool
	leaf    bool
	left    DataRedBlackTree
	name    string
	parent  DataRedBlackTree
	right   DataRedBlackTree
	version DataVersion
}

func CreateDataNode() DataRedBlackTree {
	return DataNodeRBT{false, true, nil, "", nil, nil, nil}
}

func (dn DataNodeRBT) InsertDataAt(name string, data any, timestamp int64) DataNode {
	return dn.RBTInsertDataAt(name, data, timestamp)
}

func (dn DataNodeRBT) RBTInsertDataAt(name string, data any, timestamp int64) DataRedBlackTree {
	if dn.leaf {
		dv := CreateDataVersion().InsertDataAt(data, timestamp)
		return DataNodeRBT{true, false, dn, name, dn.parent, CreateDataNode(), dv}
	}
	if dn.name == name {
		dn.version = dn.version.InsertDataAt(data, timestamp)
		return dn
	} else {
		if dn.name > name {
			dn.left = dn.left.RBTInsertDataAt(name, data, timestamp)
			return dn
		} else {
			dn.right = dn.right.RBTInsertDataAt(name, data, timestamp)
			return dn
		}
	}
}

func (dn DataNodeRBT) GetData(name string) any {
	if dn.leaf {
		return nil
	}
	if dn.name == name {
		return dn.version.GetData()
	} else if dn.name > name {
		return dn.left.GetData(name)
	} else {
		return dn.right.GetData(name)
	}
}

func (dn DataNodeRBT) GetDataAt(name string, timestamp int64) any {
	if dn.leaf {
		return nil
	}
	if dn.name == name {
		return dn.version.GetDataAt(timestamp)
	} else if dn.name > name {
		return dn.left.GetDataAt(name, timestamp)
	} else {
		return dn.right.GetDataAt(name, timestamp)
	}
}

func (dn DataNodeRBT) DeleteVersionsAt(timestamp int64) DataNode {
	return dn.RBTDeleteVersionsAt(timestamp)
}

func (dn DataNodeRBT) RBTDeleteVersionsAt(timestamp int64) DataRedBlackTree {
	if dn.leaf {
		return dn
	}
	dn.left = dn.left.RBTDeleteVersionsAt(timestamp)
	dn.right = dn.right.RBTDeleteVersionsAt(timestamp)
	dn.version = dn.version.DeleteVersionsAt(timestamp)
	return dn
}

func (dn DataNodeRBT) GetName() string {
	return dn.name
}

func (dn DataNodeRBT) GetLeft() DataRedBlackTree {
	return dn.left
}

func (dn DataNodeRBT) GetRight() DataRedBlackTree {
	return dn.right
}

func (dn DataNodeRBT) GetParent() DataRedBlackTree {
	return dn.parent
}

func (dn DataNodeRBT) SetLeft(left DataRedBlackTree) {
	dn.left = left
}

func (dn DataNodeRBT) SetRight(right DataRedBlackTree) {
	dn.right = right
}

func (dn DataNodeRBT) SetParent(parent DataRedBlackTree) {
	dn.parent = parent
}

func (dn DataNodeRBT) MakeRed() {
	dn.color = true
}

func (dn DataNodeRBT) MakeBlack() {
	dn.color = false
}

func (dn DataNodeRBT) IsRed() bool {
	return dn.color
}

func (dn DataNodeRBT) IsBlack() bool {
	return !dn.IsRed()
}

func (dn DataNodeRBT) IsLeaf() bool {
	return dn.leaf
}

func (dn DataNodeRBT) Height() uint {
	if dn.IsLeaf() {
		return 0
	}
	return 1 + dn.GetLeft().Height() + dn.right.Height()
}

func (dn DataNodeRBT) balance(childDir bool) DataRedBlackTree {
	if dn.IsBlack() {
		return dn
	} else if dn.parent.IsLeaf() {
		dn.MakeBlack()
		return dn
	}
	g := dn.GetParent()
	nodeDir := dn.GetName() < g.GetName()
	var other DataRedBlackTree
	if nodeDir {
		other = g.GetRight()
	} else {
		other = g.GetLeft()
	}
	if other == nil || other.IsBlack() {
		var p DataRedBlackTree = dn
		if childDir != nodeDir {
			if nodeDir {
				p = dn.rotateLeft()
			} else {
				p = dn.rotateRight()
			}
		}
		// if nodeDir {
		// 	g.rotateRight()
		// } else {
		// 	g.rotateLeft()
		// }
		// p.Color = false
		// dn.Color = true
		return p
	}
	dn.MakeBlack()
	other.MakeBlack()
	g.MakeRed()
	return dn
}

func (dn DataNodeRBT) rotateLeft() DataRedBlackTree {
	var g DataRedBlackTree = dn.GetParent()
	var s DataRedBlackTree = dn.GetRight()
	var c DataRedBlackTree = s.GetLeft()
	dn.SetRight(c)
	if !c.IsLeaf() {
		c.SetParent(dn)
	}
	s.SetLeft(dn)
	dn.SetParent(s)
	s.SetParent(g)
	return s
}

func (dn DataNodeRBT) rotateRight() DataRedBlackTree {
	var g DataRedBlackTree = dn.GetParent()
	var s DataRedBlackTree = dn.GetLeft()
	var c DataRedBlackTree = s.GetRight()
	dn.SetLeft(c)
	if !c.IsLeaf() {
		c.SetParent(dn)
	}
	s.SetRight(dn)
	dn.SetParent(s)
	s.SetParent(g)
	return s
}
