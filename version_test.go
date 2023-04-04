package godata

import (
	"testing"
)

func TestImport(t *testing.T) {
	version := CreateDataVersion(1, 0)
	version = version.InsertDataAt(5, 1)
	version = version.InsertDataAt(25, 2)
	if version.GetDataAt(2) != 25 {
		t.Error("Original has wrong data: ", version.GetDataAt(2))
	}
	exp, err := version.Export()
	if err != nil {
		t.Error("Error occured during export.")
	}
	iv, ierr := ImportDataVersion(exp)
	if ierr != nil {
		t.Error("Error occured during import.")
	}
	if iv.GetTimestamp() != 2 {
		t.Error("Import has wrong timestamp")
	}
	// if iv.GetDataAt(2) != 25 {
	// 	t.Error("Import has wrong data: ", iv.GetDataAt(2))
	// }
}

func TestCreateDataVersion(t *testing.T) {
	var version *DataVersionLinkedSortedList
	if version != nil {
		t.Error("New instance not empty.")
	}
	if version.GetData() != nil {
		t.Error("First element of empty data version is not end.")
	}
	if version.GetDataAt(0) != nil {
		t.Error("Element at timestamp 0 of empty data version is not end.")
	}
	version = version.InsertDataAt(5, 10)
	if version == nil {
		t.Error("Inserting first element failed.")
	}
	if version.GetData() == nil {
		t.Error("First inserted element not found.")
	}
	if version.GetDataAt(10) == nil {
		t.Error("First inserted element not found at timestamp 10.")
	}
	if version.GetDataAt(20) == nil {
		t.Error("First inserted element not found at timestamp 20.")
	}
	if version.GetDataAt(0) != nil {
		t.Error("First inserted element found at timestamp 0.")
	}
	if version.GetData() != 5 {
		t.Error("Data of first element does not equal inserted data.")
	}
	version = version.InsertDataAt(20, 40)
	if version.GetData() != 20 {
		t.Error("Data of first element does not equal latest inserted data.")
	}
	if version.GetDataAt(40) != 20 {
		t.Error("Data of element at timestamp 40 does not equal latest inserted data.")
	}
	if version.GetDataAt(50) != 20 {
		t.Error("Data of element at timestamp 50 does not equal latest inserted data.")
	}
	if version.GetDataAt(30) != 5 {
		t.Error("Data of element at timestamp 30 does not equal old inserted data.")
	}
	version = version.InsertDataAt(0, 5)
	if version.GetDataAt(5) != 0 {
		t.Error("Data of element at timestamp 5 does not equal latest inserted data.")
	}
	version = version.DeleteVersionsAt(5)
	if version.GetDataAt(20) != 5 {
		t.Error("Deletion of versions at timestamp 5 affected first newer version.")
	}
	if version.GetDataAt(50) != 20 {
		t.Error("Deletion of versions at timestamp 5 affected latest version.")
	}
	version = version.DeleteVersionsAt(10)
	if version.GetDataAt(20) == 5 {
		t.Error("Deletion of versions at timestamp 10 didn't affect first version.")
	}
	if version.GetDataAt(50) != 20 {
		t.Error("Deletion of versions at timestamp 10 affected latest version.")
	}
	version = version.DeleteVersionsAt(20)
	if version.GetDataAt(50) != 20 {
		t.Error("Deletion of versions at timestamp 20 affected latest version.")
	}
	version = version.DeleteVersionsAt(50)
	if version.GetDataAt(50) == 20 {
		t.Error("Deletion of versions at timestamp 50 didn't affect latest version.")
	}
	version = version.DeleteVersionsAt(5)
	if version != nil {
		t.Error("Data version not empty after deletion at timestamp 5.")
	}
}

func TestDuplicateDoubleDataVersion(t *testing.T) {
	var version *DataVersionLinkedSortedList
	version = version.InsertDataAt(5, 10)
	old_timestamp := version.GetTimestamp()
	old_end := version.GetEnd()
	if old_timestamp != old_end {
		t.Error("End of single version does not equal timestamp.")
	}
	version = version.InsertDataAt(5, 40)
	new_timestamp := version.GetTimestamp()
	new_end := version.GetEnd()
	if old_timestamp != new_timestamp {
		t.Error("Data was duplicated.")
	}
	if old_end == new_end {
		t.Error("End was not updated.")
	}
	if new_timestamp == new_end {
		t.Error("End is not timestamp of new duplicate.")
	}
	version = version.DeleteVersionsAt(10)
	if version.GetDataAt(20) == 5 {
		t.Error("Deletion of versions at timestamp 10 didn't affect first version.")
	}
	if version.GetDataAt(50) != 5 {
		t.Error("Deletion of versions at timestamp 10 affected latest version.")
	}
	version = version.DeleteVersionsAt(40)
	if version != nil {
		t.Error("Data version not empty after deletion at timestamp 10.")
	}
}

func TestDuplicateSplitDoubleDataVersion(t *testing.T) {
	var version *DataVersionLinkedSortedList
	version = version.InsertDataAt(5, 10)
	version = version.InsertDataAt(5, 40)
	new_end := version.GetEnd()
	version = version.InsertDataAt(10, 25)
	if version.GetTimestamp() != new_end {
		t.Error("Double version split resulted in wrong timestamp.")
	}
	if version.GetTimestamp() != version.GetEnd() {
		t.Error("End does not equal timestamp.")
	}
	if version.GetDataAt(25) != 10 {
		t.Error("New version in between has wrong value.")
	}
	if version.GetDataAt(20) != 5 {
		t.Error("First version has wrong value.")
	}
	if version.GetDataAt(50) != 5 {
		t.Error("Second partition of first version has wrong value.")
	}
	version = version.DeleteVersionsAt(10)
	if version.GetDataAt(20) == 5 {
		t.Error("Deletion of versions at timestamp 10 didn't affect first version.")
	}
	if version.GetDataAt(30) != 10 {
		t.Error("Deletion of versions at timestamp 10 affected new version in between.")
	}
	if version.GetDataAt(50) != 5 {
		t.Error("Deletion of versions at timestamp 10 affected second partition of first version.")
	}
	if version == nil {
		t.Error("Data version empty after deletion at timestamp 10.")
	}
	version = version.DeleteVersionsAt(25)
	if version == nil {
		t.Error("Data version empty after deletion at timestamp 25.")
	}
	version = version.DeleteVersionsAt(40)
	if version != nil {
		t.Error("Data version not empty after deletion at timestamp 40.")
	}
}

func TestDuplicateSplitTripleDataVersion(t *testing.T) {
	var version *DataVersionLinkedSortedList
	version = version.InsertDataAt(5, 10)
	version = version.InsertDataAt(5, 30)
	version = version.InsertDataAt(5, 40)
	version = version.InsertDataAt(10, 25)
	if version.GetTimestamp() != 30 {
		t.Error("Triple version split resulted in wrong timestamp.")
	}
	if version.GetEnd() != 40 {
		t.Error("End does not equal last timestamp.")
	}
	if version.GetDataAt(25) != 10 {
		t.Error("New version in between has wrong value.")
	}
	if version.GetDataAt(20) != 5 {
		t.Error("First version has wrong value.")
	}
	if version.GetDataAt(50) != 5 {
		t.Error("Second partition of first version has wrong value.")
	}
	version = version.DeleteVersionsAt(10)
	if version.GetDataAt(20) == 5 {
		t.Error("Deletion of versions at timestamp 10 didn't affect first version.")
	}
	if version.GetDataAt(27) != 10 {
		t.Error("Deletion of versions at timestamp 10 affected new version in between.")
	}
	if version.GetDataAt(50) != 5 {
		t.Error("Deletion of versions at timestamp 10 affected second partition of first version.")
	}
	if version == nil {
		t.Error("Data version empty after deletion at timestamp 10.")
	}
	version = version.DeleteVersionsAt(25)
	if version == nil {
		t.Error("Data version empty after deletion at timestamp 25.")
	}
	version = version.DeleteVersionsAt(40)
	if version != nil {
		t.Error("Data version not empty after deletion at timestamp 40.")
	}
}

func TestDuplicateSplitSpecialTripleDataVersion(t *testing.T) {
	var version *DataVersionLinkedSortedList
	version = version.InsertDataAt(5, 10)
	version = version.InsertDataAt(5, 20)
	version = version.InsertDataAt(5, 40)
	version = version.InsertDataAt(10, 25)
	if version.GetTimestamp() != 40 {
		t.Error("Triple version split resulted in wrong timestamp.")
	}
	if version.GetEnd() != 40 {
		t.Error("End does not equal last timestamp.")
	}
	if version.GetDataAt(25) != 10 {
		t.Error("New version in between has wrong value.")
	}
	if version.GetDataAt(20) != 5 {
		t.Error("First version has wrong value.")
	}
	if version.GetDataAt(50) != 5 {
		t.Error("Second partition of first version has wrong value.")
	}
	version = version.DeleteVersionsAt(10)
	if version.GetDataAt(15) == 5 {
		t.Error("Deletion of versions at timestamp 10 didn't affect first version.")
	}
	if version.GetDataAt(20) != 5 {
		t.Error("Deletion of versions at timestamp 10 affected second part of first version.")
	}
	if version.GetDataAt(27) != 10 {
		t.Error("Deletion of versions at timestamp 10 affected new version in between.")
	}
	if version.GetDataAt(50) != 5 {
		t.Error("Deletion of versions at timestamp 10 affected second partition of first version.")
	}
	if version == nil {
		t.Error("Data version empty after deletion at timestamp 10.")
	}
	version = version.DeleteVersionsAt(25)
	if version == nil {
		t.Error("Data version empty after deletion at timestamp 25.")
	}
	version = version.DeleteVersionsAt(40)
	if version != nil {
		t.Error("Data version not empty after deletion at timestamp 40.")
	}
}

func TestDuplicateSplitQuadDataVersion(t *testing.T) {
	var version *DataVersionLinkedSortedList
	version = version.InsertDataAt(5, 10)
	version = version.InsertDataAt(5, 20)
	version = version.InsertDataAt(5, 30)
	version = version.InsertDataAt(5, 40)
	version = version.InsertDataAt(10, 25)
	if version.GetTimestamp() != 30 {
		t.Error("Triple version split resulted in wrong timestamp.")
	}
	if version.GetEnd() != 40 {
		t.Error("End does not equal last timestamp.")
	}
	if version.GetDataAt(25) != 10 {
		t.Error("New version in between has wrong value.")
	}
	if version.GetDataAt(20) != 5 {
		t.Error("First version has wrong value.")
	}
	if version.GetDataAt(50) != 5 {
		t.Error("Second partition of first version has wrong value.")
	}
	if len(version.Map()) != 5 {
		t.Error("Map conversion invalid.")
	}
	if len(version.Array()) != 5 {
		t.Error("Array conversion invalid.")
	}
	version = version.DeleteVersionsAt(10)
	if version.GetDataAt(15) == 5 {
		t.Error("Deletion of versions at timestamp 10 didn't affect first version.")
	}
	if version.GetDataAt(20) != 5 {
		t.Error("Deletion of versions at timestamp 10 affected second part of first version.")
	}
	if version.GetDataAt(27) != 10 {
		t.Error("Deletion of versions at timestamp 10 affected new version in between.")
	}
	if version.GetDataAt(50) != 5 {
		t.Error("Deletion of versions at timestamp 10 affected second partition of first version.")
	}
	if version == nil {
		t.Error("Data version empty after deletion at timestamp 10.")
	}
	version = version.DeleteVersionsAt(25)
	if version == nil {
		t.Error("Data version empty after deletion at timestamp 25.")
	}
	version = version.DeleteVersionsAt(40)
	if version != nil {
		t.Error("Data version not empty after deletion at timestamp 40.")
	}
}

func TestVersionRange(t *testing.T) {
	var version *DataVersionLinkedSortedList
	i := 1
	c := 100
	for i <= c {
		version = version.InsertDataAt(i, int64(i))
		i++
	}
	res := version.Range(0, int64(c)).Map()
	if len(res) != c {
		t.Error("Full range does not contain all elements.", len(res))
	}
	res = version.Range(int64(c)/2, int64(c)).Map()
	if len(res) != c/2 {
		t.Error("Half range does not contain half elements.")
	}
	rres := version.Range(1, 0)
	if rres != nil {
		t.Error("Range accepts invalid range.")
	}
	res = version.Range(int64(c), int64(c)*2).Map()
	if len(res) != 0 {
		t.Error("Elements after range returned.")
	}
	res = version.Range(-int64(c), 0).Map()
	if len(res) != 0 {
		t.Error("Elements before range returned.")
	}
	// res = version.RangeInterval(0, int64(c), 5).Map()
	// if len(res) != c/5 {
	// 	t.Error("Full range with interval 5 does not contain fifth of all elements.")
	// }
	// rres := version.RangeInterval(1, 0, 5)
	// if rres != nil {
	// 	t.Error("Range accepts invalid range.")
	// }
	// res = version.RangeInterval(int64(c), int64(c)*2, 5).Map()
	// if len(res) != 0 {
	// 	t.Error("Elements after range returned.")
	// }
	// res = version.RangeInterval(-int64(c), 0, 5).Map()
	// if len(res) != 0 {
	// 	t.Error("Elements before range returned.")
	// }
	// rres = version.RangeInterval(0, int64(c), 0)
	// if rres != nil {
	// 	t.Error("Interval accept 0 as interval.")
	// }
	// rres = version.RangeInterval(0, int64(c), -1)
	// if rres != nil {
	// 	t.Error("Interval accepts negative interval.")
	// }
}

func TestVersionRangeExtra(t *testing.T) {
	var version *DataVersionLinkedSortedList
	version = version.InsertDataAt(5, 10)
	version = version.InsertDataAt(5, 15)
	version = version.InsertDataAt(5, 20)
	version = version.InsertDataAt(5, 30)
	version = version.InsertDataAt(5, 35)
	version = version.InsertDataAt(5, 40)
	version = version.InsertDataAt(10, 25)
	for i := 5; i <= 40; i = i + 5 {
		for j := i; j <= 40; j = j + 5 {
			if len(version.Range(int64(i), int64(j)).Array()) != (j/5)-(i/5) {
				t.Error("Range invalid.")
			}
		}
	}
}
