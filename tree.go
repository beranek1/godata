package godata

type DataTree interface {
	PersistChanges(string) error
	ImportDataVersion(string, []byte)
	InsertDataAt(string, any, int64)
	GetData(string) any
	GetDataAt(string, int64) any
	GetDataRange(string, int64, int64) *DataVersionLinkedSortedList
	GetDataRangeInterval(string, int64, int64, int64) *DataVersionLinkedSortedList
	DeleteVersionsAt(int64)
}

type DataTreeRBT struct {
	root *DataNodeRBT
}

func CreateDataTree() *DataTreeRBT {
	return &DataTreeRBT{nil}
}

func (dt *DataTreeRBT) ImportDataVersion(name string, raw []byte) {
	if dt.root == nil {
		dt.root = ImportDataNode(name, raw, BLACK, nil, nil, nil)
		return
	}
	node := dt.root.ImportDataVersion(name, raw)
	dt.root = Balance(dt.root, node)
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

func (dt *DataTreeRBT) GetDataRange(name string, start int64, end int64) *DataVersionLinkedSortedList {
	if dt.root == nil {
		return nil
	}
	return dt.root.GetDataRange(name, start, end)
}

func (dt *DataTreeRBT) GetDataRangeInterval(name string, start int64, end int64, interval int64) *DataVersionLinkedSortedList {
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

func (dt *DataTreeRBT) PersistChanges(dir string) error {
	if dt.root == nil {
		return nil
	}
	return dt.root.PersistChanges(dir)
}
