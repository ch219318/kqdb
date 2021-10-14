package global

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
)

const DefaultSchemaName = "example"

var IsolationLevel = "RR" //RC、RR、SE

var AutoCommit = true

var HomeDir = initHomeDir()
var BinDir = filepath.Join(HomeDir, "bin")
var DataDir = filepath.Join(HomeDir, "data")

func initHomeDir() string {
	path, err := os.Executable()
	if err != nil {
		panic(err)
	}
	dir := filepath.Dir(filepath.Dir(path))
	fmt.Println("homeDir:", dir)
	return dir
}

func InitLog() {
	log.SetPrefix("TRACE: ")
	log.SetFlags(log.Ldate | log.Lmicroseconds | log.Lshortfile)
}

func init() {
	InitLog()
}
