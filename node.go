package godata

type DataNode struct {
	Color   bool
	Left    *DataNode
	Name    string
	Right   *DataNode
	Version *DataVersion
}

func (dn DataNode) InsertVersion(name string, dv DataVersion) bool {
	if dn.Name == name {
		if dn.Version == nil {
			*(dn.Version) = dv
			return true
		} else if dn.Version.Timestamp <= dv.Timestamp {
			dv.InsertVersion(dn.Version)
			*(dn.Version) = dv
			return true
		} else {
			return dn.Version.InsertVersion(&dv)
		}
	} else if dn.Name > name {
		if dn.Left == nil {
			var node = DataNode{true, nil, name, nil, &dv}
			*(dn.Left) = node
			return true
		} else {
			return dn.Left.InsertVersion(name, dv)
		}
	} else {
		if dn.Right == nil {
			var node = DataNode{true, nil, name, nil, &dv}
			*(dn.Right) = node
			return true
		} else {
			return dn.Right.InsertVersion(name, dv)
		}
	}
}

func (dn DataNode) GetData(name string, data *any) bool {
	if dn.Name == name {
		if dn.Version == nil {
			return false
		}
		*data = dn.Version.Data
		return true
	} else if dn.Name > name {
		if dn.Left == nil {
			return false
		}
		return dn.Left.GetData(name, data)
	} else {
		if dn.Right == nil {
			return false
		}
		return dn.Right.GetData(name, data)
	}
}

func (dn DataNode) GetDataAt(name string, data *any, timestamp int64) bool {
	if dn.Name == name {
		if dn.Version == nil {
			return false
		}
		return dn.Version.GetDataAt(data, timestamp)
	} else if dn.Name > name {
		if dn.Left == nil {
			return false
		}
		return dn.Left.GetDataAt(name, data, timestamp)
	} else {
		if dn.Right == nil {
			return false
		}
		return dn.Right.GetDataAt(name, data, timestamp)
	}
}
