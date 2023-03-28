package godata

import "os"

type DataNode interface {
	InsertDataAt(string, any, int64) DataNode
	GetData(string) any
	GetDataAt(string, int64) any
	GetDataRange(string, int64, int64) map[int64]any
	GetDataRangeInterval(string, int64, int64, int64) map[int64]any
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
	version *DataVersionLinkedSortedList
}

func CreateDataNode(name string, data any, timestamp int64, color RBColor, parent *DataNodeRBT, left *DataNodeRBT, right *DataNodeRBT) *DataNodeRBT {
	dv := CreateDataVersion(data, timestamp)
	return &DataNodeRBT{color, left, name, parent, right, dv}
}

func ImportDataNode(name string, raw []byte, color RBColor, parent *DataNodeRBT, left *DataNodeRBT, right *DataNodeRBT) *DataNodeRBT {
	dv, err := ImportDataVersion(raw)
	if err == nil {
		return &DataNodeRBT{color, left, name, parent, right, dv}
	} else {
		return nil
	}
}

func (dn *DataNodeRBT) ImportDataVersion(name string, raw []byte) *DataNodeRBT {
	if dn.name == name {
		dn.version = dn.version.ImportVersion(raw)
		return nil
	} else {
		if dn.name > name {
			if dn.Left == nil {
				dn.Left = ImportDataNode(name, raw, RED, dn, nil, nil)
				return dn.Left
			} else {
				return dn.Left.ImportDataVersion(name, raw)
			}
		} else {
			if dn.Right == nil {
				dn.Right = ImportDataNode(name, raw, RED, dn, nil, nil)
				return dn.Right
			} else {
				return dn.Right.ImportDataVersion(name, raw)
			}
		}
	}
}

func (dn *DataNodeRBT) InsertDataAt(name string, data any, timestamp int64) *DataNodeRBT {
	if dn.name == name {
		dn.version = dn.version.InsertDataAt(data, timestamp)
		return nil
	} else {
		if dn.name > name {
			if dn.Left == nil {
				dn.Left = CreateDataNode(name, data, timestamp, RED, dn, nil, nil)
				return dn.Left
			} else {
				return dn.Left.InsertDataAt(name, data, timestamp)
			}
		} else {
			if dn.Right == nil {
				dn.Right = CreateDataNode(name, data, timestamp, RED, dn, nil, nil)
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

func (dn *DataNodeRBT) GetDataRange(name string, start int64, end int64) map[int64]any {
	if dn.name == name {
		return dn.version.GetDataRange(start, end)
	} else if dn.name > name {
		if dn.Left == nil {
			return nil
		}
		return dn.Left.GetDataRange(name, start, end)
	} else {
		if dn.Right == nil {
			return nil
		}
		return dn.Right.GetDataRange(name, start, end)
	}
}

func (dn *DataNodeRBT) GetDataRangeInterval(name string, start int64, end int64, interval int64) map[int64]any {
	if dn.name == name {
		return dn.version.GetDataRangeInterval(start, end, interval)
	} else if dn.name > name {
		if dn.Left == nil {
			return nil
		}
		return dn.Left.GetDataRangeInterval(name, start, end, interval)
	} else {
		if dn.Right == nil {
			return nil
		}
		return dn.Right.GetDataRangeInterval(name, start, end, interval)
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

func (dn *DataNodeRBT) PersistChanges(dir string) error {
	if dn.version != nil {
		exp, err := dn.version.Export()
		if err != nil {
			return err
		}
		err = os.WriteFile(dir+"/"+dn.name, exp, 0664)
		if err != nil {
			return err
		}
	}
	if dn.Left != nil {
		err := dn.Left.PersistChanges(dir)
		if err != nil {
			return err
		}
	}
	if dn.Right != nil {
		err := dn.Right.PersistChanges(dir)
		if err != nil {
			return err
		}
	}
	return nil
}

// Adapted from: https://github.com/torvalds/linux/blob/master/lib/rbtree.c
//
// Balance Linux
func Balance(root *DataNodeRBT, node *DataNodeRBT) *DataNodeRBT {
	if node == nil {
		return root
	}
	var parent = node.Parent
	var gparent, tmp *DataNodeRBT
	for {
		if parent == nil {
			node.Color = BLACK
			return root
		} else if parent.Color == BLACK {
			return root
		}
		gparent = parent.Parent
		if gparent == nil {
			parent.Color = BLACK
			return parent
		}
		dir := LEFT
		tmp = gparent.Right
		if parent == tmp {
			dir = RIGHT
			tmp = gparent.Left
		}

		if tmp != nil && tmp.Color == RED {
			rb_set_parent_color(tmp, gparent, BLACK)
			rb_set_parent_color(parent, gparent, BLACK)
			node = gparent
			parent = node.Parent
			rb_set_parent_color(node, parent, RED)
			continue
		}

		if dir == LEFT {
			tmp = parent.Right
			if node == tmp {
				tmp = node.Left
				parent.Right = tmp
				node.Left = parent
				if tmp != nil {
					rb_set_parent_color(tmp, parent, BLACK)
				}
				rb_set_parent_color(parent, node, RED)
				// augment_rotate(parent, node)
				parent = node
				tmp = node.Right
			}

			gparent.Left = tmp
			parent.Right = gparent
		} else {
			tmp = parent.Left
			if node == tmp {
				tmp = node.Right
				parent.Left = tmp
				node.Right = parent
				if tmp != nil {
					rb_set_parent_color(tmp, parent, BLACK)
				}
				rb_set_parent_color(parent, node, RED)
				// augment_rotate(parent, node)
				parent = node
				tmp = node.Left
			}

			gparent.Right = tmp
			parent.Left = gparent
		}
		if tmp != nil {
			rb_set_parent_color(tmp, gparent, BLACK)
		}
		tmp = gparent.Parent
		parent.Parent = gparent.Parent
		parent.Color = gparent.Color
		rb_set_parent_color(gparent, parent, RED)
		if tmp != nil {
			if tmp.Left == gparent {
				tmp.Left = parent
			} else {
				tmp.Right = parent
			}
			break
		}
		//augment_rotate(gparent, parent)
		return parent
	}
	return root
}

func rb_set_parent_color(node *DataNodeRBT, parent *DataNodeRBT, color RBColor) {
	node.Parent = parent
	node.Color = color
}
