package recordm

import (
	"container/list"
	"kqdb/src/systemm"
)

//纪录管理模块

type rawRow struct {
	len        int     //行的长度
	nullBitMap [2]byte //空值位图
	data       []byte  //实际数据，非定长类型数据=2字节长度+实际内容
}

type Row struct {
	RowNum  int               //从0开始
	Table   systemm.Table     //表
	Content map[string]string //列名：列值，列值为string
}

type Page struct {
	PageNum int        //从0开始
	RowList *list.List //元素不是row指针
}

func InsertRecord(bytes []byte) (nodeId int, err error) {

	return nodeId, err
}

func DelRecord(nodeId int) (err error) {
	return err
}

func UpdateRecord(bytes []byte, nodeId int) (err error) {
	return err

}

func GetRecord(nodeId int) (bytes []byte, err error) {
	//获取当前readView

	//根据readView筛选多版本记录
	return bytes, err
}
