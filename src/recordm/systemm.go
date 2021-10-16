package recordm

import (
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
	"os"
)

//系统管理模块

var SchemaMap = initSchemaMap()

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
	Name      string   //表名
	Columns   []Column //列切片
	PageTotal int      //page数量
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

//根据ddl语句生成表结构体
func GenTableByDdl(stmt *sqlparser.DDL) *Table {
	tableName := stmt.NewName.Name.String()

	isExist := TableIsExist(tableName)
	if isExist {
		panic(global.NewSqlError(global.DefaultSchemaName + "." + tableName + "表已存在"))
	}

	genTable := new(Table)
	genTable.Name = tableName
	genTable.PageTotal = filem.DATA_FILE_INIT_SIZE / filem.PageSize

	astCols := stmt.TableSpec.Columns
	colNumber := len(astCols) //列数量
	columns := make([]Column, colNumber)
	for i := 0; i < colNumber; i++ {
		astCol := astCols[i]
		col := genColumn(astCol)
		columns[i] = col
	}
	//log.Println(columns)
	genTable.Columns = columns

	//添加至SchemaMap和BufferPool
	SchemaMap[global.DefaultSchemaName][tableName] = genTable
	BufferPool[global.DefaultSchemaName][TableName(tableName)] = new(BufferTable)

	return genTable
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
func genColumn(astColDef *sqlparser.ColumnDefinition) Column {
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
		panic(global.NewSqlError("不支持字段:" + col.Name + "的字段类型:" + astColType.SQLType().String()))
	}

	switch astColType.NotNull {
	case sqlparser.BoolVal(true):
		col.IsNull = false
	case sqlparser.BoolVal(false):
		col.IsNull = true
	default:
		panic(global.NewSqlError("字段" + col.Name + "格式有误"))
	}

	//todo
	col.DataWidth = 20
	col.IsUnique = false
	col.DefaultVal = "de"
	col.Comment = "co"

	return *col
}

func GenFileForTable(table *Table) {
	//保存表结构体到frm文件
	bytes, err := json.Marshal(table)
	if err != nil {
		log.Panic(err)
	}
	log.Println("json:" + string(bytes))

	tablePath := filepath.Join(global.DataDir, global.DefaultSchemaName, table.Name+".frm")
	file, err := os.Create(tablePath)
	defer file.Close()
	if err != nil {
		log.Panic(err)
	}

	_, err1 := file.Write(bytes)
	if err1 != nil {
		log.Panic(err)
	}

	//初始化表data文件
	filem.CreateDataFile(table.Name)

	//todo buffer_pool添加

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
