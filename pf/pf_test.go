package pf

import (
	"kqdb/pf"
	"testing"
)

func test_CreateDataFile(t *testing.T) {
	err := pf.CreateDataFile("test")
	t.Log(err)
}

func test_AddData(t *testing.T) {
	fileHandle := pf.FileHandle{Path: "sch", FileName: "test.myd"}
	numbers := []byte{1, 0, 1, 2, 3, 4, 5, 6, 7, 8}
	err := fileHandle.AddData(numbers)
	t.Log(err)
}

func Test_GetMetaInfo(t *testing.T) {
	fileHandle := pf.FileHandle{Path: "sch", FileName: "test.myd"}
	_, err := fileHandle.GetMetaInfo()
	t.Log(err)
}
