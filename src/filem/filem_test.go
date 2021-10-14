package filem

import (
	"testing"
)

func test_CreateDataFile(t *testing.T) {
	err := CreateDataFile("test")
	t.Log(err)
}

func Test_AddData(t *testing.T) {
	fileHandle, _ := OpenFile("sch", "test")
	defer fileHandle.Close()
	bytes := []byte("hello wiki!2")
	err := fileHandle.AddData(bytes)
	t.Log(err)
}

func test_GetMetaInfo(t *testing.T) {
	fileHandle := FileHandler{Path: "sch", FileName: "test"}
	mi := fileHandle.GetMetaInfo()
	t.Log(mi)
}

func test_SaveMetaInfo(t *testing.T) {
	fileHandle, _ := OpenFile("sch", "test.data")
	defer fileHandle.Close()
	t.Logf("%p", fileHandle)
	mi := MetaInfo{233, 33, 34}
	err := fileHandle.SaveMetaInfo(mi)
	t.Log(err)
}
