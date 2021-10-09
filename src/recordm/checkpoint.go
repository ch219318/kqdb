package recordm

import (
	"log"
	"time"
)

//定时器
var ticker = initTicker()

func initTicker() *time.Ticker {
	ticker := time.NewTicker(time.Second * 3)
	go func() {
		for {
			a := <-ticker.C
			log.Println(a)
			flushDirtyList()
		}
	}()
	return ticker
}

//处理dirty链
func flushDirtyList() {
	for schemaName := range BufferPool {
		tablePool := BufferPool[schemaName]
		for tableName := range tablePool {
			bufferTable := tablePool[tableName]
			dirtyPageList := bufferTable.DirtyPageList
			if dirtyPageList != nil {
				for e := dirtyPageList.Front(); e != nil; e = e.Next() {
					dirtyPage := e.Value.(Page)
					//todo
					log.Print(dirtyPage)
				}
			}

		}
	}
}

func stop() {
	ticker.Stop() //停止定时器
}
