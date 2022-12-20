package godata

type DataNode interface {
	InsertDataAt(string, any, int64) DataNode
	GetData(string) any
	GetDataAt(string, int64) any
	DeleteVersionsAt(int64)
}

type DataRedBlackTree interface {
	DataNode
	GetName() string
	Height() uint
}

type RBColor bool

const (
	RED   RBColor = true
	BLACK RBColor = false
)

type RBDir bool

const (
	LEFT  RBDir = true
	RIGHT RBDir = false
)

type DataNodeRBT struct {
	Color   RBColor
	Left    *DataNodeRBT
	name    string
	Parent  *DataNodeRBT
	Right   *DataNodeRBT
	version DataVersion
}

func CreateDataNode(name string, data any, timestamp int64, color RBColor, parent *DataNodeRBT, left *DataNodeRBT, right *DataNodeRBT) *DataNodeRBT {
	dv := CreateDataVersion().InsertDataAt(data, timestamp)
	return &DataNodeRBT{color, left, name, parent, right, dv}
}

func (dn *DataNodeRBT) InsertDataAt(name string, data any, timestamp int64) *DataNodeRBT {
	if dn.name == name {
		dn.version = dn.version.InsertDataAt(data, timestamp)
		return nil
	} else {
		if dn.name > name {
			if dn.Left == nil {
				dn.Left = CreateDataNode(name, data, timestamp, true, dn, nil, nil)
				return dn.Left
			} else {
				return dn.Left.InsertDataAt(name, data, timestamp)
			}
		} else {
			if dn.Right == nil {
				dn.Right = CreateDataNode(name, data, timestamp, true, dn, nil, nil)
				return dn.Right
			} else {
				return dn.Right.InsertDataAt(name, data, timestamp)
			}
		}
	}
}

func (dn *DataNodeRBT) GetData(name string) any {
	if dn.name == name {
		return dn.version.GetData()
	} else if dn.name > name {
		if dn.Left == nil {
			return nil
		}
		return dn.Left.GetData(name)
	} else {
		if dn.Right == nil {
			return nil
		}
		return dn.Right.GetData(name)
	}
}

func (dn *DataNodeRBT) GetDataAt(name string, timestamp int64) any {
	if dn.name == name {
		return dn.version.GetDataAt(timestamp)
	} else if dn.name > name {
		if dn.Left == nil {
			return nil
		}
		return dn.Left.GetDataAt(name, timestamp)
	} else {
		if dn.Right == nil {
			return nil
		}
		return dn.Right.GetDataAt(name, timestamp)
	}
}

func (dn *DataNodeRBT) DeleteVersionsAt(timestamp int64) {
	if dn.Left != nil {
		dn.Left.DeleteVersionsAt(timestamp)
	}
	if dn.Right != nil {
		dn.Right.DeleteVersionsAt(timestamp)
	}
	dn.version = dn.version.DeleteVersionsAt(timestamp)
}

func (dn *DataNodeRBT) GetName() string {
	return dn.name
}

func (dn *DataNodeRBT) Height() uint {
	if dn.Left != nil {
		hleft := dn.Left.Height()
		if dn.Right != nil {
			hright := dn.Right.Height()
			if hleft > hright {
				return 1 + hleft
			} else {
				return 1 + hright
			}
		} else {
			return 1 + hleft
		}
	} else if dn.Right != nil {
		hright := dn.Right.Height()
		return 1 + hright
	} else {
		return 1
	}
}

func Balance(root *DataNodeRBT, node *DataNodeRBT) *DataNodeRBT {
	var parent = node.Parent
	var gparent, tmp *DataNodeRBT
	for parent != nil {
		if parent.Color == BLACK {
			return root
		}
		gparent = parent.Parent
		if gparent == nil {
			parent.Color = BLACK
			return root
		}

		dir := LEFT
		tmp = gparent.Right
		if parent == tmp {
			tmp = gparent.Left
			dir = RIGHT
		}
		if tmp == nil || tmp.Color == BLACK {
			if dir == LEFT && node == parent.Right {
				rotate(root, parent, dir)
				node = parent
				parent = gparent.Left
			} else if !dir && node == parent.Left {
				rotate(root, parent, dir)
				node = parent
				parent = gparent.Right
			}
			root = rotate(root, gparent, !dir)
			parent.Color = BLACK
			gparent.Color = RED
			return root
		}
		parent.Color = BLACK
		tmp.Color = BLACK
		gparent.Color = RED
		node = gparent
	}
	return root
}

func rotate(root *DataNodeRBT, parent *DataNodeRBT, dir RBDir) *DataNodeRBT {
	gparent := parent.Parent
	var s, c *DataNodeRBT
	if dir == LEFT {
		s = parent.Right
		if s != nil {
			c = s.Left
			parent.Right = c
			if c != nil {
				c.Parent = parent
			}
			s.Left = parent
		}
	} else {
		s = parent.Left
		if s != nil {
			c = s.Right
			parent.Left = c
			if c != nil {
				c.Parent = parent
			}
			s.Right = parent
		}
	}
	if s != nil {
		parent.Parent = s
		s.Parent = gparent
		if gparent != nil {
			if parent == gparent.Right {
				gparent.Right = s
			} else {
				gparent.Left = s
			}
		} else {
			root = s
		}
	}
	return root
}
