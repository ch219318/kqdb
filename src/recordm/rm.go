package recordm

import (
	"container/list"
	"kqdb/src/global"
)

//纪录管理模块

type rawTuple struct {
	len        int     //行的长度
	nullBitMap [2]byte //空值位图
	data       []byte  //实际数据，非定长类型数据=2字节长度+实际内容
}

//tuple
type Tuple struct {
	TupleNum int               //从0开始
	Table    Table             //表
	Content  map[string]string //列名：列值，列值为string
}

type Page struct {
	PageNum   int        //从0开始
	TupleList *list.List //元素不是tuple指针
}

func InsertRecord(tuple Tuple) (err error) {
	t := TableName(tuple.Table.Name)
	dirtyPageList := BufferPool[global.DefaultSchemaName][t].DirtyPageList
	for e := dirtyPageList.Front(); e != nil; e = e.Next() {
		dirtyPage := e.Value.(Page)
		dirtyPage.TupleList.PushBack(tuple)

		break
	}
	return
}

func DelRecord(nodeId int) (err error) {
	return
}

func UpdateRecord(bytes []byte, nodeId int) (err error) {
	return

}

func GetRecord(nodeId int) (bytes []byte, err error) {
	//获取当前readView

	//根据readView筛选多版本记录
	return
}
