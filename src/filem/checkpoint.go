package filem

import (
	"container/list"
	"log"
	"time"
)

//定时器
var ticker = initTicker()

func initTicker() *time.Ticker {
	ticker := time.NewTicker(time.Second * 3)
	go func() {
		for {
			<-ticker.C
			flushDirtyList()
		}
	}()
	return ticker
}

//处理dirty链
func flushDirtyList() {
	for fileP := range bufferPool {
		bufferTable := bufferPool[fileP]
		dirtyPageList := bufferTable.DirtyPageList
		if dirtyPageList.Len() > 0 {

			log.Println("flushDirtyList:", fileP)

			file := GetFile(fileP).File
			for e := dirtyPageList.Front(); e != nil; e = e.Next() {
				dirtyPage := e.Value.(*Page)
				log.Print(*dirtyPage)

				//dirtyPage转bytes
				bytes := dirtyPage.Marshal()
				offset := dirtyPage.PageNum * PageSize
				n, err := file.WriteAt(bytes, int64(offset))
				if err != nil {
					log.Println(n)
					log.Println(err)
				}
			}

			//dirty链清零
			bufferTable.DirtyPageList = list.New()

		}
	}
}

func stop() {
	ticker.Stop() //停止定时器
}
