package recordm

import (
	"container/list"
	"kqdb/src/filem"
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

			pageList := list.New()
			//加入一定数量page
			fileHandler := filem.FilesMap[schemaName][tableName][1]
			for i := 1; i < 10; i++ {
				bytes := fileHandler.GetPageData(i)
				page := new(Page)
				page.UnMarshal(bytes, i, schemaName, tableName)
				pageList.PushBack(*page)
			}

			bufferTable := BufferTable{pageList, list.New()}

			tablePool[t] = &bufferTable
		}
		schemaPool[schemaName] = tablePool
	}

	return schemaPool
}

//插入databuffer

//databuffer过期与替换

//扩展大小
