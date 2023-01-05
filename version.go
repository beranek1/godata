package godata

type DataVersion interface {
	InsertDataAt(any, int64) DataVersion
	GetData() any
	GetDataAt(int64) any
	GetDataRange(int64, int64) map[int64]any
	GetDataRangeInterval(int64, int64, int64) map[int64]any
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
	}
	return dv.next.GetDataAt(timestamp)
}

func (dv *DataVersionLinkedSortedList) GetDataRange(start int64, end int64) map[int64]any {
	if dv == nil || start > end || dv.timestamp <= start {
		return nil
	} else if dv.timestamp <= end {
		m := map[int64]any{}
		m[dv.timestamp] = dv.data
		return dv.next.getDataRangeI(start, end, m)
	}
	return dv.next.GetDataRange(start, end)
}

func (dv *DataVersionLinkedSortedList) getDataRangeI(start int64, end int64, m map[int64]any) map[int64]any {
	if dv == nil || start > end || dv.timestamp <= start {
		return m
	} else if dv.timestamp <= end {
		m[dv.timestamp] = dv.data
		return dv.next.getDataRangeI(start, end, m)
	}
	return m
}

func (dv *DataVersionLinkedSortedList) GetDataRangeInterval(start int64, end int64, interval int64) map[int64]any {
	if dv == nil || start > end || interval <= 0 || dv.timestamp <= start {
		return nil
	} else if dv.timestamp > end {
		return dv.next.GetDataRangeInterval(start, end, interval)
	} else if dv.timestamp <= end && dv.timestamp > (end-interval) {
		m := map[int64]any{}
		m[end] = dv.data
		return dv.next.getDataRangeIntervalI(start, end-interval, interval, m)
	}
	return dv.GetDataRangeInterval(start, end-interval, interval)
}

func (dv *DataVersionLinkedSortedList) getDataRangeIntervalI(start int64, end int64, interval int64, m map[int64]any) map[int64]any {
	if dv == nil || start > end || interval <= 0 || dv.timestamp <= start {
		return m
	} else if dv.timestamp > end {
		return dv.next.getDataRangeIntervalI(start, end, interval, m)
	} else if dv.timestamp <= end && dv.timestamp > (end-interval) {
		m[end] = dv.data
		return dv.next.getDataRangeIntervalI(start, end-interval, interval, m)
	}
	return dv.getDataRangeIntervalI(start, end-interval, interval, m)
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
