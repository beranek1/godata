package godata

import (
	"os"
	"testing"
)

func TestManage(t *testing.T) {
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

}

func TestGetData(t *testing.T) {

}

func TestDeleteVersionsAt(t *testing.T) {

}
