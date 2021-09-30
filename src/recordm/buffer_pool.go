package recordm

import (
	"container/list"
)

type BufferTable struct {
	PageList      *list.List //元素不是page指针
	DirtyPageList *list.List
}

//创建buffer pool数据结构
type TableName string

var BufferPool = make(map[TableName]BufferTable)

//插入databuffer

//databuffer过期与替换

//扩展大小
