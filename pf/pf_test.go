package pf

import (
	"kqdb/pf"
	"testing"
)

func test_CreateDataFile(t *testing.T) {
	err := pf.CreateDataFile("test")
	t.Log(err)
}

func Test_AddData(t *testing.T) {
	fileHandle, _ := pf.OpenDataFile("sch", "test.myd")
	defer pf.CloseDataFile(fileHandle)
	bytes := []byte("hello wiki!2")
	err := fileHandle.AddData(bytes)
	t.Log(err)
}

func test_GetMetaInfo(t *testing.T) {
	fileHandle := pf.FileHandle{Path: "sch", FileName: "test.myd"}
	mi := fileHandle.GetMetaInfo()
	t.Log(mi)
}

func test_SaveMetaInfo(t *testing.T) {
	fileHandle, _ := pf.OpenDataFile("sch", "test.myd")
	defer pf.CloseDataFile(fileHandle)
	t.Logf("%p", fileHandle)
	mi := pf.MetaInfo{233, 33, 34}
	err := fileHandle.SaveMetaInfo(mi)
	t.Log(err)
}
