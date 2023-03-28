package godata

import (
	"encoding/json"
	"reflect"
)

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
	Data       any                          `json:"data"`
	Next       *DataVersionLinkedSortedList `json:"next"`
	Timestamps []int64                      `json:"timestamps"`
}

func CreateDataVersion(data any, timestamp int64) *DataVersionLinkedSortedList {
	return &DataVersionLinkedSortedList{data, nil, []int64{timestamp}}
}

func ImportDataVersion(raw []byte) (*DataVersionLinkedSortedList, error) {
	var dve DataVersionLinkedSortedList
	err := json.Unmarshal(raw, &dve)
	if err == nil {
		return &dve, nil
	}
	return nil, err
}

func (dv *DataVersionLinkedSortedList) ImportVersion(raw []byte) *DataVersionLinkedSortedList {
	dvi, err := ImportDataVersion(raw)
	if err == nil {
		return dv.InsertVersion(dvi)
	}
	return dv
}

func (dv *DataVersionLinkedSortedList) InsertVersion(dvi *DataVersionLinkedSortedList) *DataVersionLinkedSortedList {
	if dv == nil {
		return dvi
	} else if dv.Timestamps[0] <= dvi.Timestamps[0] {
		dvi.Next = dv
		return dvi
	} else if dv.Next == nil {
		dv.Next = dvi
	} else {
		dv.Next = dv.Next.InsertVersion(dvi)
	}
	return dv
}

func (dv *DataVersionLinkedSortedList) InsertDataAt(data any, timestamp int64) *DataVersionLinkedSortedList {
	if dv == nil {
		return &DataVersionLinkedSortedList{data, dv, []int64{timestamp}}
	} else if dv.Timestamps[0] <= timestamp {
		if reflect.TypeOf(dv.Data) == reflect.TypeOf(data) && reflect.DeepEqual(dv.Data, data) {
			if dv.Timestamps[0] != timestamp {
				dv.Timestamps = append(dv.Timestamps, timestamp)
			}
			return dv
		}
		if dv.Timestamps[len(dv.Timestamps)-1] > timestamp {
			pos := -1
			for i := 0; i < len(dv.Timestamps); i++ {
				if dv.Timestamps[i] > timestamp {
					pos = i
					break
				}
			}
			old := make([]int64, 0)
			new := make([]int64, 0)
			old = append(old, dv.Timestamps[:pos]...)
			new = append(new, dv.Timestamps[pos:]...)
			dv.Timestamps = old
			return &DataVersionLinkedSortedList{dv.Data, &DataVersionLinkedSortedList{data, dv, []int64{timestamp}}, new}
		}
		return &DataVersionLinkedSortedList{data, dv, []int64{timestamp}}
	} else if dv.Next == nil {
		dv.Next = &DataVersionLinkedSortedList{data, nil, []int64{timestamp}}
	} else {
		dv.Next = dv.Next.InsertDataAt(data, timestamp)
	}
	return dv
}

func (dv *DataVersionLinkedSortedList) GetData() any {
	if dv == nil {
		return nil
	}
	return dv.Data
}

func (dv *DataVersionLinkedSortedList) GetDataAt(timestamp int64) any {
	if dv == nil {
		return nil
	} else if dv.Timestamps[0] <= timestamp {
		return dv.Data
	}
	return dv.Next.GetDataAt(timestamp)
}

func (dv *DataVersionLinkedSortedList) GetDataRange(start int64, end int64) map[int64]any {
	if dv == nil || start > end || dv.Timestamps[0] <= start {
		return nil
	} else if dv.Timestamps[0] <= end {
		m := map[int64]any{}
		m[dv.Timestamps[0]] = dv.Data
		return dv.Next.getDataRangeI(start, end, m)
	}
	return dv.Next.GetDataRange(start, end)
}

func (dv *DataVersionLinkedSortedList) getDataRangeI(start int64, end int64, m map[int64]any) map[int64]any {
	if dv == nil || start > end || dv.Timestamps[0] <= start {
		return m
	} else if dv.Timestamps[0] <= end {
		m[dv.Timestamps[0]] = dv.Data
		return dv.Next.getDataRangeI(start, end, m)
	}
	return m
}

func (dv *DataVersionLinkedSortedList) GetDataRangeInterval(start int64, end int64, interval int64) map[int64]any {
	if dv == nil || start > end || interval <= 0 || dv.Timestamps[0] <= start {
		return nil
	} else if dv.Timestamps[0] > end {
		return dv.Next.GetDataRangeInterval(start, end, interval)
	} else if dv.Timestamps[0] <= end && dv.Timestamps[0] > (end-interval) {
		m := map[int64]any{}
		m[end] = dv.Data
		return dv.Next.getDataRangeIntervalI(start, end-interval, interval, m)
	}
	return dv.GetDataRangeInterval(start, end-interval, interval)
}

func (dv *DataVersionLinkedSortedList) getDataRangeIntervalI(start int64, end int64, interval int64, m map[int64]any) map[int64]any {
	if dv == nil || start > end || interval <= 0 || dv.Timestamps[0] <= start {
		return m
	} else if dv.Timestamps[0] > end {
		return dv.Next.getDataRangeIntervalI(start, end, interval, m)
	} else if dv.Timestamps[0] <= end && dv.Timestamps[0] > (end-interval) {
		m[end] = dv.Data
		return dv.Next.getDataRangeIntervalI(start, end-interval, interval, m)
	}
	return dv.getDataRangeIntervalI(start, end-interval, interval, m)
}

func (dv *DataVersionLinkedSortedList) DeleteVersionsAt(timestamp int64) *DataVersionLinkedSortedList {
	if dv == nil {
		return nil
	} else if dv.Timestamps[0] <= timestamp {
		if dv.Timestamps[len(dv.Timestamps)-1] <= timestamp {
			return nil
		}
		pos := -1
		for i := 0; i < len(dv.Timestamps); i++ {
			if dv.Timestamps[i] > timestamp {
				pos = i
				break
			}
		}
		new := make([]int64, 0)
		new = append(new, dv.Timestamps[pos:]...)
		dv.Next = nil
		dv.Timestamps = new
		return dv
	} else if dv.Next != nil {
		dv.Next = dv.Next.DeleteVersionsAt(timestamp)
	}
	return dv
}

func (dv *DataVersionLinkedSortedList) GetTimestamp() int64 {
	return dv.Timestamps[0]
}

func (dv *DataVersionLinkedSortedList) GetEnd() int64 {
	return dv.Timestamps[len(dv.Timestamps)-1]
}

func (dv *DataVersionLinkedSortedList) Export() ([]byte, error) {
	exp, err := json.Marshal(dv)
	if err != nil {
		return nil, err
	}
	return exp, nil
}
