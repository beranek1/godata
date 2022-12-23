package godata

type DataVersion interface {
	InsertDataAt(any, int64) DataVersion
	GetData() any
	GetDataAt(int64) any
	DeleteVersionsAt(int64) DataVersion
	IsEmpty() bool
	GetTimestamp() int64
}

type DataVersionLinkedSortedList struct {
	data      any
	next      *DataVersionLinkedSortedList
	timestamp int64
}

func CreateDataVersion(data any, timestamp int64) *DataVersionLinkedSortedList {
	return &DataVersionLinkedSortedList{data, nil, timestamp}
}

func (dv *DataVersionLinkedSortedList) InsertDataAt(data any, timestamp int64) *DataVersionLinkedSortedList {
	if dv == nil || dv.timestamp <= timestamp {
		return &DataVersionLinkedSortedList{data, dv, timestamp}
	} else if dv.next == nil {
		dv.next = &DataVersionLinkedSortedList{data, nil, timestamp}
	} else {
		dv.next = dv.next.InsertDataAt(data, timestamp)
	}
	return dv
}

func (dv *DataVersionLinkedSortedList) GetData() any {
	if dv == nil {
		return nil
	}
	return dv.data
}

func (dv *DataVersionLinkedSortedList) GetDataAt(timestamp int64) any {
	if dv == nil {
		return nil
	} else if dv.timestamp <= timestamp {
		return dv.data
	} else if dv.next == nil {
		return nil
	}
	return dv.next.GetDataAt(timestamp)
}

func (dv *DataVersionLinkedSortedList) DeleteVersionsAt(timestamp int64) *DataVersionLinkedSortedList {
	if dv == nil || dv.timestamp <= timestamp {
		return nil
	} else if dv.next != nil {
		dv.next = dv.next.DeleteVersionsAt(timestamp)
	}
	return dv
}

func (dv *DataVersionLinkedSortedList) GetTimestamp() int64 {
	return dv.timestamp
}
