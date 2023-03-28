package godata

import (
	"errors"
)

// Wrapper for DataManager to support godatainterface definitions
type DataStore struct {
	manager *DataManager
}

func Create(path string) (*DataStore, error) {
	manager, err := Manage(path)
	if err != nil {
		return &DataStore{}, err
	}
	return &DataStore{manager: manager}, nil
}

func (ds DataStore) Put(key string, value any) error {
	if ds.manager.InsertData(key, value) {
		return nil
	}
	return errors.New("inserting data unsuccesful")
}

func (ds DataStore) PutAt(key string, value any, timestamp int64) error {
	if ds.manager.InsertDataAt(key, value, timestamp) {
		return nil
	}
	return errors.New("inserting data unsuccesful")
}

func (ds DataStore) Get(key string) (any, error) {
	value := ds.manager.GetData(key)
	if value == nil {
		return value, errors.New("data not found")
	}
	return value, nil
}

func (ds DataStore) GetAt(key string, timestamp int64) (any, error) {
	value := ds.manager.GetDataAt(key, timestamp)
	if value == nil {
		return value, errors.New("data not found")
	}
	return value, nil
}

func (ds DataStore) Range(key string, start int64, end int64) (map[int64]any, error) {
	return ds.manager.GetDataRange(key, start, end), nil
}

func (ds DataStore) From(key string, start int64) (map[int64]any, error) {
	return ds.manager.GetDataFrom(key, start), nil
}

func (ds DataStore) RangeInterval(key string, start int64, end int64, interval int64) (map[int64]any, error) {
	return ds.manager.GetDataRangeInterval(key, start, end, interval), nil
}

func (ds DataStore) FromInterval(key string, start int64, interval int64) (map[int64]any, error) {
	return ds.manager.GetDataFromInterval(key, start, interval), nil
}

func (ds DataStore) DeleteVersionsAt(timestamp int64) {
	ds.manager.DeleteVersionsAt(timestamp)
}

func (ds DataStore) PersistChanges() {
	ds.manager.PersistChanges()
}
