package recordm

import (
	"kqdb/src/filem"
	"kqdb/src/global"
)

//纪录管理模块

func init() {
	global.InitLog()
}

func CreateFileHandle() {

}

func OpenFileHandle(schemaName string, talbeName string) *RmFileHandle {
	fileHandle := new(RmFileHandle)
	fmFileHandler := filem.GetFile(filem.FileTypeData, schemaName, talbeName)
	fileHandle.fmFileHandler = fmFileHandler
	return fileHandle
}

type RmFileHandle struct {
	fmFileHandler *filem.FileHandler
}

func (rfh *RmFileHandle) InsertRecord(tuple Tuple) {
	page := rfh.fmFileHandler.GetPage(1)

	tupleBytes := tuple.Marshal()
	page.AddTupleBytes(tupleBytes)

	rfh.fmFileHandler.MarkDirty(1)

	return
}

func (rfh *RmFileHandle) DelRecord(tupleNum int) {
	return
}

func (rfh *RmFileHandle) UpdateRecord(bytes []byte, tupleNum int) {
	return

}

func (rfh *RmFileHandle) GetRecord(tupleNum int) []byte {
	//获取当前readView

	//根据readView筛选多版本记录
	return nil
}
