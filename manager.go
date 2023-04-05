package godata

import (
	"os"
	"time"

	"github.com/beranek1/godatainterface"
)

type DataManager struct {
	Path   string
	tree   DataTree
	ticker *time.Ticker
	done   chan bool
}

// Creates instance of DataManager at given path, initializes instance with previous data located at path.
func Manage(path string) (*DataManager, error) {
	dm := &DataManager{Path: path, tree: CreateDataTree(), ticker: time.NewTicker(5 * time.Second), done: make(chan bool)}
	err := os.MkdirAll(dm.Path, 0750)
	if err != nil && !os.IsExist(err) {
		return dm, err
	}
	err = dm.importData()
	dm.startPersisting()
	return dm, err
}

func (dm DataManager) startPersisting() {
	go func() {
		for {
			select {
			case <-dm.done:
				return
			case <-dm.ticker.C:
				dm.PersistChanges()
			}
		}
	}()
}

func (dm DataManager) importData() error {
	nodes, err := os.ReadDir(dm.Path)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		raw, err := os.ReadFile(dm.Path + "/" + node.Name())
		if err == nil {
			dm.tree.ImportDataVersion(node.Name(), raw)
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
	return true
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
func (dm DataManager) GetDataRange(name string, start int64, end int64) godatainterface.DataVersionLinked {
	return dm.tree.GetDataRange(name, start, end)
}

// Looks for versions starting at given timestamp. Returns map of values or nil if unsuccessful.
func (dm DataManager) GetDataFrom(name string, start int64) godatainterface.DataVersionLinked {
	timestamp := time.Now().UnixNano()
	return dm.tree.GetDataRange(name, start, timestamp)
}

// Looks for versions in interval within given timestamp range with given key. Returns map of values or nil if unsuccessful.
func (dm DataManager) GetDataRangeInterval(name string, start int64, end int64, interval int64) godatainterface.DataVersionLinked {
	return dm.tree.GetDataRangeInterval(name, start, end, interval)
}

// Looks for versions in interval starting at given timestamp. Returns map of values or nil if unsuccessful.
func (dm DataManager) GetDataFromInterval(name string, start int64, interval int64) godatainterface.DataVersionLinked {
	timestamp := time.Now().UnixNano()
	return dm.tree.GetDataRangeInterval(name, start, timestamp, interval)
}

// Remove all versions equal or older than the provided timestamp
func (dm DataManager) DeleteVersionsAt(timestamp int64) {
	dm.tree.DeleteVersionsAt(timestamp)
}

func (dm DataManager) PersistChanges() {
	dm.tree.PersistChanges(dm.Path)
}
