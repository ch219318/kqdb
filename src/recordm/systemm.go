package recordm

import (
	"errors"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
	"io/ioutil"
	"kqdb/src/filem"
	"kqdb/src/global"
	"log"
	"path/filepath"
	"strings"

	// "time"
	"encoding/json"
	"fmt"
	"os"
)

//系统管理模块

var SchemaMap = initSchemaMap()

//key为schema和table
func initSchemaMap() map[string]map[string]*Table {
	schemaMap := make(map[string]map[string]*Table)

	//获取所有schema
	dirNames, err := listDir(global.DataDir)
	if err != nil {
		log.Fatal(err)
	}
	for _, dirName := range dirNames {
		dirPath := filepath.Join(global.DataDir, dirName)
		fileNames, err := listFile(dirPath, filem.FrameFileSuf)
		if err != nil {
			log.Fatal(err)
		}
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

//获取指定目录下的所有文件，不进入下一级目录搜索，可以匹配后缀过滤。
func listFile(dirPth string, suffix string) (fileNames []string, err error) {
	fileNames = make([]string, 0, 10)
	dir, err := ioutil.ReadDir(dirPth)
	if err != nil {
		return nil, err
	}
	suffix = strings.ToUpper(suffix) //忽略后缀匹配的大小写
	for _, fi := range dir {
		if fi.IsDir() { // 忽略目录
			continue
		}
		if strings.HasSuffix(strings.ToUpper(fi.Name()), "."+suffix) { //匹配文件
			fileNames = append(fileNames, fi.Name())
		}
	}
	return
}

//获取指定目录下的所有目录
func listDir(dirPth string) (dirNames []string, err error) {
	dirNames = make([]string, 0, 10)
	dir, err := ioutil.ReadDir(dirPth)
	if err != nil {
		return nil, err
	}
	for _, fi := range dir {
		if fi.IsDir() {
			dirNames = append(dirNames, fi.Name())
		} else {
			continue
		}
	}
	return
}

//定义列结构体
type Table struct {
	Name    string   //表名
	Columns []Column //列切片
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

//定义语法错误结构体
type grammerError struct {
	msg string
}

func (ge grammerError) Error() string {
	return fmt.Sprintf("语法错误--%s", ge.msg)
}

//根据ddl语句生成表结构体
func GenTableByDdl(stmt *sqlparser.DDL) (*Table, error) {
	tableName := stmt.NewName.Name.String()

	isExist := TableIsExist(tableName)
	if isExist {
		return nil, errors.New(global.DefaultSchemaName + "." + tableName + "表已存在")
	}

	genTable := new(Table)
	genTable.Name = tableName

	astCols := stmt.TableSpec.Columns
	colNumber := len(astCols) //列数量
	columns := make([]Column, colNumber)
	for i := 0; i < colNumber; i++ {
		astCol := astCols[i]
		col, err := genColumn(astCol)
		if err != nil {
			return genTable, err
		}
		columns[i] = col
	}
	//log.Println(columns)
	genTable.Columns = columns

	//添加至SchemaMap和BufferPool
	SchemaMap[global.DefaultSchemaName][tableName] = genTable
	BufferPool[global.DefaultSchemaName][TableName(tableName)] = new(BufferTable)

	return genTable, nil
}

//判断表是否已存在
func TableIsExist(tableName string) bool {
	_, ok := SchemaMap[global.DefaultSchemaName][tableName]
	return ok
}

func GetTable(tableName string) *Table {
	t, ok := SchemaMap[global.DefaultSchemaName][tableName]
	if ok {
		return t
	}
	return nil
}

//根据ddl生成column
func genColumn(astColDef *sqlparser.ColumnDefinition) (Column, error) {
	col := new(Column)
	astColName := astColDef.Name
	astColType := astColDef.Type

	col.Name = astColName.String()

	switch astColType.SQLType() {
	case sqltypes.Int32:
		col.DataType = TypeInt
	case sqltypes.VarChar:
		col.DataType = TypeString
	default:
		return *col, grammerError{"不支持字段:" + col.Name + "的字段类型:" + astColType.SQLType().String()}
	}

	switch astColType.NotNull {
	case sqlparser.BoolVal(true):
		col.IsNull = false
	case sqlparser.BoolVal(false):
		col.IsNull = true
	default:
		return *col, grammerError{"字段" + col.Name + "格式有误"}
	}

	//todo
	col.DataWidth = 20
	col.IsUnique = false
	col.DefaultVal = "de"
	col.Comment = "co"

	return *col, nil
}

func GenFileForTable(table *Table) error {
	//保存表结构体到frm文件
	bytes, err := json.Marshal(table)
	if err != nil {
		return err
	}
	log.Println("json:" + string(bytes))

	tablePath := filepath.Join(global.DataDir, global.DefaultSchemaName, table.Name+".frm")
	file, err := os.Create(tablePath)
	defer file.Close()
	if err != nil {
		return err
	}

	_, err1 := file.Write(bytes)
	if err1 != nil {
		return err1
	}

	//初始化表data文件
	err2 := filem.CreateDataFile(table.Name)
	if err2 != nil {
		return err2
	}

	return nil
}

//根据frm文件生成表结构体
func genTableFromFile(filePath string) *Table {
	bytes, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Fatal(err)
	}
	var table *Table = new(Table)
	err1 := json.Unmarshal(bytes, table)
	if err1 != nil {
		log.Fatal(err1)
	}
	return table
}
