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
	root *DataNode
}

// Creates instance of DataManager at given path, initializes instance with previous data located at path.
func Manage(path string) (DataManager, error) {
	var dm DataManager
	dm.Path = path
	dm.root = nil
	err := os.MkdirAll(dm.Path, 0750)
	if err != nil {
		if os.IsExist(err) {
			nodes, err := os.ReadDir(dm.Path)
			if err != nil {
				return dm, err
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
		} else {
			return dm, err
		}
	}
	return dm, nil
}

// Inserts latest data at current timestamp. See below.
func (dm DataManager) InsertData(name string, data any) bool {
	timestamp := time.Now().UnixNano()
	return dm.InsertDataAt(name, data, timestamp)
}

// Inserts data into DataManager with given key and timestamp, additionally updates local files if successful. Returns bool indicating whether operation was successful.
func (dm DataManager) InsertDataAt(name string, data any, timestamp int64) bool {
	var dv = DataVersion{data, nil, timestamp}
	success := true
	if dm.root != nil {
		success = dm.root.InsertVersion(name, dv)
	} else {
		var dn = DataNode{true, nil, name, nil, &dv}
		*(dm.root) = dn
	}
	if success {
		dataBytes, err := json.Marshal(data)
		if err != nil {
			return false
		}
		err = os.WriteFile(dm.Path+"/"+name+"/"+strconv.FormatInt(timestamp, timestampBase), dataBytes, 0664)
		if err != nil {
			return false
		}
	}
	return success
}

// Looks for latest version with given key and writes to data if successful. Returns bool indicating whether operation was successful.
func (dm DataManager) GetData(name string, data *any) bool {
	if dm.root == nil {
		return false
	}
	return dm.root.GetData(name, data)
}

// Looks for version at given timestamp with given key and writes to data if successful. Returns bool indicating whether operation was successful.
func (dm DataManager) GetDataAt(name string, data *any, timestamp int64) bool {
	if dm.root == nil {
		return false
	}
	return dm.root.GetDataAt(name, data, timestamp)
}
