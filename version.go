package godata

import (
	"encoding/json"
	"os"
	"reflect"
	"strconv"
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
	data       any
	next       *DataVersionLinkedSortedList
	timestamp  int64
	end        int64
	partitions []int64
	changed    bool
}

type DataVersionPartitionedExport struct {
	Data       any
	Timestamp  int64
	End        int64
	Partitions []int64
}

func CreateDataVersion(data any, timestamp int64) *DataVersionLinkedSortedList {
	return &DataVersionLinkedSortedList{data, nil, timestamp, timestamp, []int64{}, true}
}

func ImportDataVersion(raw []byte) (*DataVersionLinkedSortedList, error) {
	var dve DataVersionPartitionedExport
	err := json.Unmarshal(raw, &dve)
	if err == nil {
		return &DataVersionLinkedSortedList{dve.Data, nil, dve.Timestamp, dve.End, dve.Partitions, false}, nil
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
	} else if dv.timestamp <= dvi.timestamp {
		dvi.next = dv
		return dvi
	} else if dv.next == nil {
		dv.next = dvi
	} else {
		dv.next = dv.next.InsertVersion(dvi)
	}
	return dv
}

func (dv *DataVersionLinkedSortedList) InsertDataAt(data any, timestamp int64) *DataVersionLinkedSortedList {
	if dv == nil {
		return &DataVersionLinkedSortedList{data, dv, timestamp, timestamp, []int64{}, true}
	} else if dv.timestamp <= timestamp {
		if reflect.TypeOf(dv.data) == reflect.TypeOf(data) && reflect.DeepEqual(dv.data, data) {
			if dv.timestamp != timestamp {
				if dv.timestamp != dv.end {
					if dv.end < timestamp {
						dv.partitions = append(dv.partitions, dv.end)
						dv.end = timestamp
					} else {
						dv.partitions = append(dv.partitions, timestamp)
					}
				} else {
					dv.end = timestamp
				}
			}
			return dv
		}
		if dv.end > timestamp {
			if len(dv.partitions) == 0 {
				new_timestamp := dv.end
				dv.end = dv.timestamp
				dv.changed = true
				return &DataVersionLinkedSortedList{dv.data, &DataVersionLinkedSortedList{data, dv, timestamp, timestamp, []int64{}, true}, new_timestamp, new_timestamp, []int64{}, true}
			}
			pos := -1
			for i := 0; i < len(dv.partitions); i++ {
				if dv.partitions[i] > timestamp {
					pos = i
					break
				}
			}
			if pos < 0 {
				new_timestamp := dv.end
				dv.end = dv.partitions[len(dv.partitions)-1]
				old := make([]int64, 0)
				old = append(old, dv.partitions[:len(dv.partitions)-1]...)
				dv.partitions = old
				dv.changed = true
				return &DataVersionLinkedSortedList{dv.data, &DataVersionLinkedSortedList{data, dv, timestamp, timestamp, []int64{}, true}, new_timestamp, new_timestamp, []int64{}, true}
			}
			new_timestamp := dv.partitions[pos]
			new_end := dv.end
			if pos > 0 {
				dv.end = dv.partitions[pos-1]
			} else {
				dv.end = dv.timestamp
			}
			old := make([]int64, 0)
			new := make([]int64, 0)
			if pos > 0 {
				old = append(old, dv.partitions[:pos-1]...)
			}
			new = append(new, dv.partitions[pos+1:]...)
			dv.partitions = old
			dv.changed = true
			return &DataVersionLinkedSortedList{dv.data, &DataVersionLinkedSortedList{data, dv, timestamp, timestamp, []int64{}, true}, new_timestamp, new_end, new, true}
		}
		return &DataVersionLinkedSortedList{data, dv, timestamp, timestamp, []int64{}, true}
	} else if dv.next == nil {
		dv.next = &DataVersionLinkedSortedList{data, nil, timestamp, timestamp, []int64{}, true}
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

func (dv *DataVersionLinkedSortedList) GetEnd() int64 {
	return dv.end
}

func (dv *DataVersionLinkedSortedList) Export() ([]byte, error) {
	dve := DataVersionPartitionedExport{dv.data, dv.timestamp, dv.end, dv.partitions}
	return json.Marshal(dve)
}

func (dv *DataVersionLinkedSortedList) PersistChanges(dir string) error {
	if dv == nil || !dv.changed {
		return nil
	}
	raw, err := dv.Export()
	if err != nil {
		return err
	}
	err = os.WriteFile(dir+"/"+strconv.FormatInt(dv.timestamp, timestampBase), raw, 0664)
	if err != nil {
		return err
	}
	return dv.next.PersistChanges(dir)
}
