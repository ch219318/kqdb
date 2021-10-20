package recordm

import (
	"kqdb/src/filem"
	"kqdb/src/global"
	"log"
	"path/filepath"
)

//纪录管理模块

func init() {
	global.InitLog()
}

func CreateFileHandle() {

}

func OpenFileHandle(schemaName string, tableName string) *RmFileHandle {
	fileHandle := new(RmFileHandle)
	fileP := filepath.Join(global.DataDir, schemaName, tableName+"."+filem.DataFileSuf)
	fmFileHandler := filem.GetFile(fileP)
	fileHandle.fmFileHandler = fmFileHandler
	return fileHandle
}

type RmFileHandle struct {
	fmFileHandler *filem.FileHandler
}

func (rfh *RmFileHandle) InsertRecord(tuple Tuple) {
	var resultPage *filem.Page
	tupleBytes := tuple.Marshal()

	//获取一个可用的page
	totalPage := rfh.fmFileHandler.TotalPage
	for i := 1; i < totalPage; i++ {
		page := rfh.fmFileHandler.GetPage(i)
		if (page.Upper - page.Lower - 1) > (4 + len(tupleBytes)) {
			resultPage = page
			break
		}
	}

	//分配新page
	if resultPage == nil {
		resultPage = rfh.fmFileHandler.AllocatePage()

	}

	if resultPage != nil {
		resultPage.AddTupleBytes(tupleBytes)
		rfh.fmFileHandler.MarkDirty(resultPage.PageNum)
	} else {
		log.Panic("获取page出错")
	}

}

func (rfh *RmFileHandle) DelRecord(tupleNum int) {
}

func (rfh *RmFileHandle) UpdateRecord(bytes []byte, tupleNum int) {

}

func (rfh *RmFileHandle) GetRecord(tupleNum int) []byte {
	//获取当前readView

	//根据readView筛选多版本记录
	return nil
}
