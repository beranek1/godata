package godata

import (
	"encoding/json"
	"os"
	"strconv"
	"time"
)

var timestampBase = 36

type DataManager struct {
	Path string
	tree DataTree
}

// Creates instance of DataManager at given path, initializes instance with previous data located at path.
func Manage(path string) (*DataManager, error) {
	dm := &DataManager{Path: path, tree: CreateDataTree()}
	err := os.MkdirAll(dm.Path, 0750)
	if err != nil && !os.IsExist(err) {
		return dm, err
	}
	err = dm.importData()
	return dm, err
}

func (dm DataManager) importData() error {
	nodes, err := os.ReadDir(dm.Path)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if node.IsDir() {
			versions, err := os.ReadDir(dm.Path + "/" + node.Name())
			if err == nil {
				for _, version := range versions {
					timestamp, err := strconv.ParseInt(version.Name(), timestampBase, 64)
					if err == nil {
						dataBytes, err := os.ReadFile(dm.Path + "/" + node.Name() + "/" + version.Name())
						if err == nil {
							var data any
							err = json.Unmarshal(dataBytes, &data)
							if err == nil {
								dm.InsertDataAt(node.Name(), data, timestamp)
							}
						}
					}
				}
			}
		}
	}
	return nil
}

// Inserts latest data at current timestamp. See below.
func (dm DataManager) InsertData(name string, data any) bool {
	timestamp := time.Now().UnixNano()
	return dm.InsertDataAt(name, data, timestamp)
}

// Inserts data into DataManager with given key and timestamp, additionally updates local files if successful. Returns bool indicating whether operation was successful.
func (dm DataManager) InsertDataAt(name string, data any, timestamp int64) bool {
	dm.tree.InsertDataAt(name, data, timestamp)
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return false
	}
	err = os.Mkdir(dm.Path+"/"+name, 0750)
	if err != nil && !os.IsExist(err) {
		return false
	}
	err = os.WriteFile(dm.Path+"/"+name+"/"+strconv.FormatInt(timestamp, timestampBase), dataBytes, 0664)
	return err == nil
}

// Looks for latest version with given key. Returns value or nil if unsuccessful.
func (dm DataManager) GetData(name string) any {
	return dm.tree.GetData(name)
}

// Looks for version at given timestamp with given key. Returns value or nil if unsuccessful.
func (dm DataManager) GetDataAt(name string, timestamp int64) any {
	return dm.tree.GetDataAt(name, timestamp)
}

// Looks for versions in given timestamp range with given key. Returns map of values or nil if unsuccessful.
func (dm DataManager) GetDataRange(name string, start int64, end int64) map[int64]any {
	return dm.tree.GetDataRange(name, start, end)
}

// Looks for versions starting at given timestamp. Returns map of values or nil if unsuccessful.
func (dm DataManager) GetDataFrom(name string, start int64) map[int64]any {
	timestamp := time.Now().UnixNano()
	return dm.tree.GetDataRange(name, start, timestamp)
}

// Looks for versions in interval within given timestamp range with given key. Returns map of values or nil if unsuccessful.
func (dm DataManager) GetDataRangeInterval(name string, start int64, end int64, interval int64) map[int64]any {
	return dm.tree.GetDataRangeInterval(name, start, end, interval)
}

// Looks for versions in interval starting at given timestamp. Returns map of values or nil if unsuccessful.
func (dm DataManager) GetDataFromInterval(name string, start int64, interval int64) map[int64]any {
	timestamp := time.Now().UnixNano()
	return dm.tree.GetDataRangeInterval(name, start, timestamp, interval)
}

// Remove all versions equal or older than the provided timestamp
func (dm DataManager) DeleteVersionsAt(timestamp int64) {
	nodes, err := os.ReadDir(dm.Path)
	if err == nil {
		for _, node := range nodes {
			if node.IsDir() {
				versions, err := os.ReadDir(dm.Path + "/" + node.Name())
				if err == nil {
					for _, version := range versions {
						t, err := strconv.ParseInt(version.Name(), timestampBase, 64)
						if err == nil && t <= timestamp {
							os.Remove(dm.Path + "/" + node.Name() + "/" + version.Name())
						}
					}
				}
			}
		}
	}
	dm.tree.DeleteVersionsAt(timestamp)
}
