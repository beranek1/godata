package godata

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestManageEmptyDir(t *testing.T) {
	os.RemoveAll("dm_test")
	_, err0 := Manage("dm_test")
	if err0 != nil {
		t.Error("Managing not existing data dir failed: " + err0.Error())
	}
	_, err1 := Manage("dm_test")
	if err1 != nil {
		t.Error("Managing existing data dir failed: " + err1.Error())
	}
	_, err2 := Manage("dm_test/sub")
	if err2 != nil {
		t.Error("Managing not existing data sub dir failed: " + err2.Error())
	}
	_, err3 := Manage("dm_test/sub")
	if err3 != nil {
		t.Error("Managing existing data sub dir failed: " + err3.Error())
	}
	os.RemoveAll("dm_test")
	_, err4 := Manage("dm_test/sub")
	if err4 != nil {
		t.Error("Managing not existing data sub dir in not existing dir failed: " + err4.Error())
	}
	os.RemoveAll("dm_test")
}

func TestInsertData(t *testing.T) {
	os.RemoveAll("dm_test")
	dm, err := Manage("dm_test")
	if err != nil {
		t.Error("Managing not existing data dir failed: " + err.Error())
	}
	for i := 0; i < 1000; i++ {
		if !dm.InsertData(fmt.Sprint(i), i) {
			t.Error("Inserting element ", fmt.Sprint(i), " failed.")
		}
	}
	os.RemoveAll("dm_test")
}

func TestGetData(t *testing.T) {
	os.RemoveAll("dm_test")
	dm, err := Manage("dm_test")
	if err != nil {
		t.Error("Managing not existing data dir failed: " + err.Error())
	}
	for i := 0; i < 1000; i++ {
		if !dm.InsertData(fmt.Sprint(i), i) {
			t.Error("Inserting element ", fmt.Sprint(i), " failed.")
		}
	}
	for i := 0; i < 1000; i++ {
		data := dm.GetData(fmt.Sprint(i))
		if data == nil {
			t.Error("Getting element ", fmt.Sprint(i), " failed.")
		}
	}
	os.RemoveAll("dm_test")
}

func TestManageExistingDir(t *testing.T) {
	os.RemoveAll("dm_test")
	dm0, err0 := Manage("dm_test")
	if err0 != nil {
		t.Error("Managing not existing data dir failed: " + err0.Error())
	}
	for i := 0; i < 1000; i++ {
		if !dm0.InsertData(fmt.Sprint(i), i) {
			t.Error("Inserting element ", fmt.Sprint(i), " failed.")
		}
	}
	dm1, err1 := Manage("dm_test")
	if err1 != nil {
		t.Error("Managing existing data dir failed: " + err1.Error())
	}
	for i := 0; i < 1000; i++ {
		data := dm1.GetData(fmt.Sprint(i))
		if data == nil {
			t.Error("Getting element ", fmt.Sprint(i), " failed.")
		}
	}
	os.RemoveAll("dm_test")
}

func TestDeleteVersionsAt(t *testing.T) {
	os.RemoveAll("dm_test")
	dm, err := Manage("dm_test")
	if err != nil {
		t.Error("Managing not existing data dir failed: " + err.Error())
	}
	for i := 0; i < 1000; i++ {
		if !dm.InsertData(fmt.Sprint(i), i) {
			t.Error("Inserting element ", fmt.Sprint(i), " failed.")
		}
	}
	timestamp := time.Now().UnixNano()
	dm.DeleteVersionsAt(timestamp)
	for i := 0; i < 1000; i++ {
		data := dm.GetData(fmt.Sprint(i))
		if data != nil {
			t.Error("Getting element ", fmt.Sprint(i), " wasn't deleted.")
		}
	}
	os.RemoveAll("dm_test")
}
