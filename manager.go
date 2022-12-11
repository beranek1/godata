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
	Root *DataNode
}

func Manage(path string) (DataManager, error) {
	var dm DataManager
	dm.Path = path
	dm.Root = nil
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

func (dm DataManager) InsertData(name string, data any) bool {
	timestamp := time.Now().UnixNano()
	return dm.InsertDataAt(name, data, timestamp)
}

func (dm DataManager) InsertDataAt(name string, data any, timestamp int64) bool {
	var dv = DataVersion{data, nil, timestamp}
	success := true
	if dm.Root != nil {
		success = dm.Root.InsertVersion(name, dv)
	} else {
		var dn = DataNode{true, nil, name, nil, &dv}
		*(dm.Root) = dn
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

func (dm DataManager) GetData(name string, data *any) bool {
	if dm.Root == nil {
		return false
	}
	return dm.Root.GetData(name, data)
}

func (dm DataManager) GetDataAt(name string, data *any, timestamp int64) bool {
	if dm.Root == nil {
		return false
	}
	return dm.Root.GetDataAt(name, data, timestamp)
}
