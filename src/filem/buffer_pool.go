package filem

import (
	"container/list"
)

type bufferTable struct {
	PageList      *list.List //元素是page指针
	DirtyPageList *list.List
}

//创建buffer pool数据结构
type tName string

var bufferPool = initBufferPool()

func initBufferPool() map[string]map[tName]*bufferTable {
	schemaPool := make(map[string]map[tName]*bufferTable)

	for schemaName := range filesMap {
		tablePool := make(map[tName]*bufferTable)
		tableMap := filesMap[schemaName]
		for tableName := range tableMap {
			t := tName(tableName)

			pageList := list.New()
			//加入一定数量page
			fileHandler := filesMap[schemaName][tableName][1]
			for i := 1; i < 10; i++ {
				page := fileHandler.getPageFromDisk(i)
				pageList.PushBack(page)
			}

			bufferTable := bufferTable{pageList, list.New()}

			tablePool[t] = &bufferTable
		}
		schemaPool[schemaName] = tablePool
	}

	return schemaPool
}

//插入databuffer

//databuffer过期与替换

//扩展大小
