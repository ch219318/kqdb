package recordm

import (
	"container/list"
	"kqdb/src/filem"
	"kqdb/src/global"
	"log"
	"os"
	"path/filepath"
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
	for schemaName := range BufferPool {
		tablePool := BufferPool[schemaName]
		for tableName := range tablePool {
			bufferTable := tablePool[tableName]
			dirtyPageList := bufferTable.DirtyPageList
			if dirtyPageList.Len() > 0 {

				log.Println("flushDirtyList:" + schemaName + "." + string(tableName))
				fileName := string(tableName) + "." + filem.DataFileSuf
				tablePath := filepath.Join(global.DataDir, global.DefaultSchemaName, fileName)
				file, err := os.OpenFile(tablePath, os.O_RDWR, os.ModePerm)
				if err != nil {
					log.Fatal(err)
				}
				for e := dirtyPageList.Front(); e != nil; e = e.Next() {
					dirtyPage := e.Value.(Page)
					log.Print(dirtyPage)

					//dirtyPage转bytes
					bytes := dirtyPage.Marshal()
					offset := dirtyPage.PageNum * filem.PageSize
					n, err := file.WriteAt(bytes, int64(offset))
					if err != nil {
						log.Println(n)
						log.Println(err)
					}
				}
				file.Close()

				//dirty链清零
				bufferTable.DirtyPageList = list.New()

			}

		}
	}
}

func stop() {
	ticker.Stop() //停止定时器
}
