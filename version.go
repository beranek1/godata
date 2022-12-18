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
	end       bool
	next      DataVersion
	timestamp int64
}

func CreateDataVersion() DataVersion {
	return DataVersionLinkedSortedList{nil, true, nil, 0}
}

func (dv DataVersionLinkedSortedList) InsertDataAt(data any, timestamp int64) DataVersion {
	if dv.end {
		return DataVersionLinkedSortedList{data, false, &dv, timestamp}
	} else if dv.timestamp <= timestamp {
		return DataVersionLinkedSortedList{data, false, &dv, timestamp}
	} else {
		dv.next = dv.next.InsertDataAt(data, timestamp)
		return dv
	}
}

func (dv DataVersionLinkedSortedList) GetData() any {
	if dv.end {
		return nil
	}
	return dv.data
}

func (dv DataVersionLinkedSortedList) GetDataAt(timestamp int64) any {
	if dv.end {
		return nil
	}
	if dv.timestamp <= timestamp {
		return dv.data
	}
	return dv.next.GetDataAt(timestamp)
}

func (dv DataVersionLinkedSortedList) DeleteVersionsAt(timestamp int64) DataVersion {
	if dv.end {
		return dv
	} else if dv.timestamp <= timestamp {
		return dv.next
	} else {
		next := dv.next.DeleteVersionsAt(timestamp)
		if next != nil {
			dv.next = next
		}
		return dv
	}
}

func (dv DataVersionLinkedSortedList) IsEmpty() bool {
	return dv.end
}

func (dv DataVersionLinkedSortedList) GetTimestamp() int64 {
	return dv.timestamp
}
