package recordm

import (
	"container/list"
	"encoding/binary"
	"kqdb/src/filem"
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
	Content  map[string]string //列名：列值，列值为字节数组
}

func (page *Tuple) Marshal() []byte {
	return nil
}

func (page *Tuple) UnMarshal([]byte) {
}

type Page struct {
	PageNum   int        //从0开始
	TupleList *list.List //元素不是tuple指针
}

func (page *Page) Marshal() []byte {
	itemsBytes := make([]byte, 0)
	tuplesBytes := make([]byte, 0)

	tupleList := page.TupleList
	for e := tupleList.Front(); e != nil; e = e.Next() {
		tuple := e.Value.(Tuple)
		tupleBs := tuple.Marshal()
		tuplesBytes = append(tupleBs, tuplesBytes...)
		//item生成
		tupleOffset := filem.PageSize - len(tuplesBytes)
		tupleLen := len(tupleBs)
		flag := 1
		item := tupleOffset<<17 + flag<<15 + tupleLen
		itemBs := make([]byte, 4)
		binary.BigEndian.PutUint32(itemBs, uint32(item))
		//todo
		//a := bits.Add()
		itemsBytes = append(itemsBytes, itemBs...)
	}

	//page头
	headerBytes := make([]byte, 24)
	pageLower := uint16(24 + len(itemsBytes))
	pageUpper := uint16(filem.PageSize - 1 - len(tuplesBytes))
	binary.BigEndian.PutUint16(headerBytes[2:4], pageLower)
	binary.BigEndian.PutUint16(headerBytes[4:6], pageUpper)

	//空白区
	blankLen := filem.PageSize - len(headerBytes) - len(itemsBytes) - len(tuplesBytes)
	blankBytes := make([]byte, blankLen)

	pageBytes := append(append(append(headerBytes, itemsBytes...), blankBytes...), tuplesBytes...)
	return pageBytes
}

func (page *Page) UnMarshal([]byte) {
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
