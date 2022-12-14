package godata

type DataTree interface {
	InsertDataAt(string, any, int64)
	GetData(string) any
	GetDataAt(string, int64) any
	GetDataRange(string, int64, int64) map[int64]any
	GetDataRangeInterval(string, int64, int64, int64) map[int64]any
	DeleteVersionsAt(int64)
}

type DataTreeRBT struct {
	root *DataNodeRBT
}

func CreateDataTree() *DataTreeRBT {
	return &DataTreeRBT{nil}
}

func (dt *DataTreeRBT) InsertDataAt(name string, data any, timestamp int64) {
	if dt.root == nil {
		dt.root = CreateDataNode(name, data, timestamp, BLACK, nil, nil, nil)
		return
	}
	node := dt.root.InsertDataAt(name, data, timestamp)
	dt.root = Balance(dt.root, node)
}

func (dt *DataTreeRBT) GetData(name string) any {
	if dt.root == nil {
		return nil
	}
	return dt.root.GetData(name)
}

func (dt *DataTreeRBT) GetDataAt(name string, timestamp int64) any {
	if dt.root == nil {
		return nil
	}
	return dt.root.GetDataAt(name, timestamp)
}

func (dt *DataTreeRBT) GetDataRange(name string, start int64, end int64) map[int64]any {
	if dt.root == nil {
		return nil
	}
	return dt.root.GetDataRange(name, start, end)
}

func (dt *DataTreeRBT) GetDataRangeInterval(name string, start int64, end int64, interval int64) map[int64]any {
	if dt.root == nil {
		return nil
	}
	return dt.root.GetDataRangeInterval(name, start, end, interval)
}

func (dt *DataTreeRBT) DeleteVersionsAt(timestamp int64) {
	if dt.root == nil {
		return
	}
	dt.root.DeleteVersionsAt(timestamp)
}

func (dt *DataTreeRBT) Height() uint {
	if dt.root == nil {
		return 0
	}
	return dt.root.Height()
}
