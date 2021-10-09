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

var BufferPool = initBufferPool()

func initBufferPool() map[string]map[TableName]*BufferTable {
	schemaPool := make(map[string]map[TableName]*BufferTable)

	for schemaName := range SchemaMap {
		tablePool := make(map[TableName]*BufferTable)
		tableMap := SchemaMap[schemaName]
		for tableName := range tableMap {
			t := TableName(tableName)
			tablePool[t] = new(BufferTable)
		}
		schemaPool[schemaName] = tablePool
	}

	return schemaPool
}

//插入databuffer

//databuffer过期与替换

//扩展大小
