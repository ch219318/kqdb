package filem

import (
	"testing"
)

func test_CreateDataFile(t *testing.T) {
	err := CreateDataFile("test")
	t.Log(err)
}

func Test_AddData(t *testing.T) {
	fileHandle, _ := OpenDataFile("sch", "test.data")
	defer CloseDataFile(fileHandle)
	bytes := []byte("hello wiki!2")
	err := fileHandle.AddData(bytes)
	t.Log(err)
}

func test_GetMetaInfo(t *testing.T) {
	fileHandle := FileHandle{Path: "sch", FileName: "test.data"}
	mi := fileHandle.GetMetaInfo()
	t.Log(mi)
}

func test_SaveMetaInfo(t *testing.T) {
	fileHandle, _ := OpenDataFile("sch", "test.data")
	defer CloseDataFile(fileHandle)
	t.Logf("%p", fileHandle)
	mi := MetaInfo{233, 33, 34}
	err := fileHandle.SaveMetaInfo(mi)
	t.Log(err)
}
