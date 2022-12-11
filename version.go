package godata

type DataVersion struct {
	Data      any
	Next      *DataVersion
	Timestamp int64
}

func (dv DataVersion) InsertVersion(v *DataVersion) bool {
	if dv.Next == nil {
		*(dv.Next) = *v
		return true
	} else if dv.Next.Timestamp <= v.Timestamp {
		v.InsertVersion(dv.Next)
		*(dv.Next) = *v
		return true
	} else {
		return dv.Next.InsertVersion(v)
	}
}

func (dv DataVersion) GetDataAt(data *any, timestamp int64) bool {
	if dv.Timestamp <= timestamp {
		*data = dv.Data
		return true
	}
	if dv.Next == nil {
		return false
	}
	return dv.Next.GetDataAt(data, timestamp)
}
