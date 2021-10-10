package recordm

import (
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
			if dirtyPageList.Len() > 0 {

				fileName := string(tableName) + "." + filem.DataFileSuf
				tablePath := filepath.Join(global.DataDir, global.DefaultSchemaName, fileName)
				file, err := os.Open(tablePath)
				if err != nil {
					log.Fatal(err)
				}
				for e := dirtyPageList.Front(); e != nil; e = e.Next() {
					dirtyPage := e.Value.(Page)
					log.Print(dirtyPage)

					//dirtyPage转bytes
					bytes := dirtyPage.Marshal()
					offset := dirtyPage.PageNum * filem.PageSize
					file.WriteAt(bytes, int64(offset))
				}

			}

		}
	}
}

func stop() {
	ticker.Stop() //停止定时器
}
