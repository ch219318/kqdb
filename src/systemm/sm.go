package systemm

import (
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
	"log"
	"path/filepath"
	// "time"
	"encoding/json"
	"fmt"
	"os"
)

//系统管理模块

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

	genTable := new(Table)
	genTable.Name = stmt.NewName.Name.String()

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
	return genTable, nil
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

//保存表结构体到frm文件
func SaveTableToFile(table *Table) error {
	bytes, err := json.Marshal(table)
	if err != nil {
		return err
	}
	log.Println("json:" + string(bytes))

	tablePath := filepath.Join(DataDir, "example", table.Name+".frm")
	file, err := os.Create(tablePath)
	defer file.Close()
	if err != nil {
		return err
	}

	_, err1 := file.Write(bytes)
	if err1 != nil {
		return err1
	}

	return nil
}

//根据frm文件生成表结构体
