package systemm

import (
	"io/ioutil"
	"kqdb/src/filem"
	"kqdb/src/global"
	"log"
	"path/filepath"
	"strings"

	// "time"
	"encoding/json"
	"os"
)

//系统管理模块

var schemaMap = initSchemaMap()

//key为schema和table
func initSchemaMap() map[string]map[string]*Table {
	schemaMap := make(map[string]map[string]*Table)

	//获取所有schema
	dirNames := filem.ListDir(global.DataDir)

	for _, dirName := range dirNames {
		dirPath := filepath.Join(global.DataDir, dirName)
		fileNames := filem.ListFile(dirPath, filem.FrameFileSuf)

		tableMap := make(map[string]*Table)
		for _, fileName := range fileNames {
			filePath := filepath.Join(dirPath, fileName)
			table := genTableFromFile(filePath)
			tableName := strings.TrimSuffix(fileName, "."+filem.FrameFileSuf)
			tableMap[tableName] = table
		}
		schemaName := dirName
		schemaMap[schemaName] = tableMap
	}

	return schemaMap
}

//定义列结构体
type Table struct {
	SchemaName string
	Name       string   //表名
	Columns    []Column //列切片
}

//定义表结构体
type Column struct {
	Name       string   //列名
	DataType   DataType //列数据类型
	DataWidth  int      // 数据宽度
	IsNull     bool     //非空与否
	IsUnique   bool     // 是否唯一
	DefaultVal string   // 默认值
	Comment    string   //注释
}

//定义列数据类型枚举值
type DataType int

const (
	TypeInt DataType = 1 + iota
	TypeString
	TypeDate
)

//判断表是否存在
func TableIsExist(schemaName string, tableName string) bool {
	if _, ok := schemaMap[schemaName]; ok {
		if _, ok := schemaMap[schemaName][tableName]; ok {
			return true
		}
	}
	return false
}

func GetTable(schemaName string, tableName string) *Table {
	if _, ok := schemaMap[schemaName]; ok {
		if t, ok := schemaMap[schemaName][tableName]; ok {
			return t
		}
	}
	return nil
}

func CreateTable(table *Table) {
	schemaName := table.SchemaName
	tableName := table.Name
	isExist := TableIsExist(schemaName, tableName)
	if isExist {
		panic(global.NewSqlError(schemaName + "." + tableName + "表已存在"))
	}

	//保存表结构体到frm文件
	bytes, err := json.Marshal(table)
	if err != nil {
		log.Panic(err)
	}
	log.Println("json:" + string(bytes))

	tablePath := filepath.Join(global.DataDir, schemaName, table.Name+".frm")
	file, err := os.Create(tablePath)
	defer file.Close()
	if err != nil {
		log.Panic(err)
	}

	_, err1 := file.Write(bytes)
	if err1 != nil {
		log.Panic(err)
	}

	//添加至SchemaMap
	schemaMap[schemaName][tableName] = table

	//初始化表data文件
	fileP := filepath.Join(global.DataDir, schemaName, tableName+"."+filem.DataFileSuf)
	filem.CreateDataFile(fileP)

}

//根据frm文件生成表结构体
func genTableFromFile(filePath string) *Table {
	bytes, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Panic(err)
	}
	var table *Table = new(Table)
	err1 := json.Unmarshal(bytes, table)
	if err1 != nil {
		log.Panic(err1)
	}
	return table
}
