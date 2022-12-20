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

// Adapted from: https://en.wikipedia.org/wiki/Red%E2%80%93black_tree
//
// BALANCE Wikipedia
//
// func Balance(root *DataNodeRBT, node *DataNodeRBT) *DataNodeRBT {
// 	var parent = node.Parent
// 	var gparent, tmp *DataNodeRBT
// 	for parent != nil {
// 		if parent.Color == BLACK {
// 			return root
// 		}
// 		gparent = parent.Parent
// 		if gparent == nil {
// 			parent.Color = BLACK
// 			return root
// 		}

// 		dir := LEFT
// 		tmp = gparent.Right
// 		if parent == tmp {
// 			tmp = gparent.Left
// 			dir = RIGHT
// 		}
// 		if tmp == nil || tmp.Color == BLACK {
// 			if dir == LEFT && node == parent.Right {
// 				rotate(root, parent, dir)
// 				node = parent
// 				parent = gparent.Left
// 			} else if dir == RIGHT && node == parent.Left {
// 				rotate(root, parent, dir)
// 				node = parent
// 				parent = gparent.Right
// 			}
// 			root = rotate(root, gparent, !dir)
// 			parent.Color = BLACK
// 			gparent.Color = RED
// 			return root
// 		}
// 		parent.Color = BLACK
// 		tmp.Color = BLACK
// 		gparent.Color = RED
// 		node = gparent
// 	}
// 	return root
// }

// func rotate(root *DataNodeRBT, parent *DataNodeRBT, dir RBDir) *DataNodeRBT {
// 	gparent := parent.Parent
// 	var s, c *DataNodeRBT
// 	if dir == LEFT {
// 		s = parent.Right
// 		if s != nil {
// 			c = s.Left
// 			parent.Right = c
// 			if c != nil {
// 				c.Parent = parent
// 			}
// 			s.Left = parent
// 		}
// 	} else {
// 		s = parent.Left
// 		if s != nil {
// 			c = s.Right
// 			parent.Left = c
// 			if c != nil {
// 				c.Parent = parent
// 			}
// 			s.Right = parent
// 		}
// 	}
// 	if s != nil {
// 		parent.Parent = s
// 		s.Parent = gparent
// 		if gparent != nil {
// 			if parent == gparent.Right {
// 				gparent.Right = s
// 			} else {
// 				gparent.Left = s
// 			}
// 		} else {
// 			return s
// 		}
// 	}
// 	return root
// }

// Adapted from: https://github.com/torvalds/linux/blob/master/lib/rbtree.c
//
// Balance Linux
func Balance(root *DataNodeRBT, node *DataNodeRBT) *DataNodeRBT {
	var parent = node.Parent
	var gparent, tmp *DataNodeRBT
	for {
		if parent == nil {
			node.Color = BLACK
			return root
		}
		if parent.Color == BLACK {
			return root
		}
		gparent = parent.Parent
		if gparent == nil {
			parent.Color = BLACK
			return parent
		}
		tmp = gparent.Right
		if parent != tmp {
			if tmp != nil && tmp.Color == RED {
				rb_set_parent_color(tmp, gparent, BLACK)
				rb_set_parent_color(parent, gparent, BLACK)
				node = gparent
				parent = node.Parent
				rb_set_parent_color(node, parent, RED)
				continue
			}

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
			if tmp != nil {
				rb_set_parent_color(tmp, gparent, BLACK)
			}
			root = rb_rotate_set_parents(gparent, parent, root, RED)
			// augment_rotate(gparent, parent)
			break
		} else {
			tmp = gparent.Left
			if tmp != nil && tmp.Color == RED {
				rb_set_parent_color(tmp, gparent, BLACK)
				rb_set_parent_color(parent, gparent, BLACK)
				node = gparent
				parent = node.Parent
				rb_set_parent_color(node, parent, RED)
				continue
			}

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
			if tmp != nil {
				rb_set_parent_color(tmp, gparent, BLACK)
			}
			root = rb_rotate_set_parents(gparent, parent, root, RED)
			//augment_rotate(gparent, parent)
			break
		}
	}
	return root
}

func rb_set_parent_color(node *DataNodeRBT, parent *DataNodeRBT, color RBColor) {
	node.Parent = parent
	node.Color = color
}

func rb_change_child(old *DataNodeRBT, new *DataNodeRBT, parent *DataNodeRBT, root *DataNodeRBT) *DataNodeRBT {
	if parent != nil {
		if parent.Left == old {
			parent.Left = new
		} else {
			parent.Right = new
		}
	} else {
		return new
	}
	return root
}

func rb_rotate_set_parents(old *DataNodeRBT, new *DataNodeRBT, root *DataNodeRBT, color RBColor) *DataNodeRBT {
	parent := old.Parent
	new.Parent = old.Parent
	new.Color = old.Color
	rb_set_parent_color(old, new, color)
	return rb_change_child(old, new, parent, root)
}
