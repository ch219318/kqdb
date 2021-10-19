package filem

import (
	"container/list"
	"strings"
)

type bufferTable struct {
	PageList      *list.List //元素是page指针
	DirtyPageList *list.List
}

//创建buffer pool数据结构,key为filePath
var bufferPool = initBufferPool()

func initBufferPool() map[string]*bufferTable {
	pool := make(map[string]*bufferTable)

	for fileP := range filesMap {
		if strings.HasSuffix(fileP, "."+DataFileSuf) {
			pageList := list.New()
			//加入一定数量page
			fileHandler := filesMap[fileP]
			for i := 1; i < 10; i++ {
				page := fileHandler.getPageFromDisk(i)
				pageList.PushBack(page)
			}

			bufferTable := bufferTable{pageList, list.New()}
			pool[fileP] = &bufferTable
		}
	}

	return pool
}

func addFileToPool(fileP string) {
	pageList := list.New()
	//加入一定数量page
	fileHandler := filesMap[fileP]
	for i := 1; i < 10; i++ {
		page := fileHandler.getPageFromDisk(i)
		pageList.PushBack(page)
	}
	bufferTable := bufferTable{pageList, list.New()}
	bufferPool[fileP] = &bufferTable
}

//插入databuffer

//databuffer过期与替换

//扩展大小
