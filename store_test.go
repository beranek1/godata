package godata

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/beranek1/godatainterface"
)

func TestInterface(t *testing.T) {
	var _ godatainterface.DataStore = DataStore{}
	var _ godatainterface.DataStore = (*DataStore)(nil)
	var _ godatainterface.DataStoreVersioned = DataStore{}
	var _ godatainterface.DataStoreVersioned = (*DataStore)(nil)
	var _ godatainterface.DataStoreVersionedRange = DataStore{}
	var _ godatainterface.DataStoreVersionedRange = (*DataStore)(nil)
}

func TestCreateEmptyDir(t *testing.T) {
	os.RemoveAll("ds_test")
	_, err0 := Create("ds_test")
	if err0 != nil {
		t.Error("Managing not existing data dir failed: " + err0.Error())
	}
	_, err1 := Create("ds_test")
	if err1 != nil {
		t.Error("Managing existing data dir failed: " + err1.Error())
	}
	_, err2 := Create("ds_test/sub")
	if err2 != nil {
		t.Error("Managing not existing data sub dir failed: " + err2.Error())
	}
	_, err3 := Create("ds_test/sub")
	if err3 != nil {
		t.Error("Managing existing data sub dir failed: " + err3.Error())
	}
	os.RemoveAll("ds_test")
	_, err4 := Create("ds_test/sub")
	if err4 != nil {
		t.Error("Managing not existing data sub dir in not existing dir failed: " + err4.Error())
	}
	os.RemoveAll("ds_test")
}

func TestPut(t *testing.T) {
	os.RemoveAll("ds_test")
	ds, err := Create("ds_test")
	if err != nil {
		t.Error("Managing not existing data dir failed: " + err.Error())
	}
	for i := 0; i < 1000; i++ {
		if err := ds.Put(fmt.Sprint(i), i); err != nil {
			t.Error("Inserting element ", fmt.Sprint(i), " failed.")
		}
	}
	os.RemoveAll("ds_test")
}

func TestGet(t *testing.T) {
	os.RemoveAll("ds_test")
	ds, err := Create("ds_test")
	if err != nil {
		t.Error("Managing not existing data dir failed: " + err.Error())
	}
	for i := 0; i < 1000; i++ {
		if err := ds.Put(fmt.Sprint(i), i); err != nil {
			t.Error("Inserting element ", fmt.Sprint(i), " failed.")
		}
	}
	for i := 0; i < 1000; i++ {
		_, err := ds.Get(fmt.Sprint(i))
		if err != nil {
			t.Error("Getting element ", fmt.Sprint(i), " failed.")
		}
	}
	os.RemoveAll("ds_test")
}

func TestCreateExistingDir(t *testing.T) {
	os.RemoveAll("ds_test")
	ds0, err0 := Create("ds_test")
	if err0 != nil {
		t.Error("Managing not existing data dir failed: " + err0.Error())
	}
	for i := 0; i < 1000; i++ {
		if err := ds0.Put(fmt.Sprint(i), i); err != nil {
			t.Error("Inserting element ", fmt.Sprint(i), " failed.")
		}
	}
	ds0.PersistChanges()
	ds1, err1 := Create("ds_test")
	if err1 != nil {
		t.Error("Managing existing data dir failed: " + err1.Error())
	}
	for i := 0; i < 1000; i++ {
		_, err := ds1.Get(fmt.Sprint(i))
		if err != nil {
			t.Error("Getting element ", fmt.Sprint(i), " failed.")
		}
	}
	os.RemoveAll("ds_test")
}

func TestDSDeleteVersionsAt(t *testing.T) {
	os.RemoveAll("ds_test")
	ds, err := Create("ds_test")
	if err != nil {
		t.Error("Managing not existing data dir failed: " + err.Error())
	}
	for i := 0; i < 1000; i++ {
		if err := ds.Put(fmt.Sprint(i), i); err != nil {
			t.Error("Inserting element ", fmt.Sprint(i), " failed.")
		}
	}
	timestamp := time.Now().UnixNano()
	ds.DeleteVersionsAt(timestamp)
	for i := 0; i < 1000; i++ {
		_, err := ds.Get(fmt.Sprint(i))
		if err == nil {
			t.Error("Getting deleted element ", fmt.Sprint(i), " succeeded.")
		}
	}
	os.RemoveAll("ds_test")
}
