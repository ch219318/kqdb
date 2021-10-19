package sqlm

import (
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
	"kqdb/src/global"
	"kqdb/src/querym"
	"kqdb/src/systemm"
	"log"
	"runtime/debug"
)

func init() {
	global.InitLog()
}

//执行sql
func HandSql(sql string) (result string) {
	defer func() {
		if pa := recover(); pa != nil {
			//全局处理SqlError
			if sqlError, ok := pa.(*global.SqlError); ok {
				debug.PrintStack()
				errMsg := sqlError.Error()
				log.Println(errMsg)
				result = errMsg
			} else {
				//再次抛出其他panic
				panic(pa)
			}
		}
	}()

	log.Printf("sql：%s\n", sql)

	result = "ok"

	stmt, err := sqlparser.ParseStrictDDL(sql)
	if err != nil {
		log.Println("sql格式错误:" + sql)
		return "sql格式错误:" + sql
	}

	switch stmt := stmt.(type) {
	case *sqlparser.DDL:
		result = handDdl(stmt)
	case *sqlparser.Select:
		result = handSelect(stmt)
	case *sqlparser.Insert:
		result = handInsert(stmt)
	default:
		result = "暂不支持当前类型sql:" + sql
	}

	log.Printf("result的值为%v\n", result)
	return
}

func handDdl(ddlStmt *sqlparser.DDL) string {
	switch ddlStmt.Action {
	case sqlparser.CreateStr:
		table := genTableByDdl(ddlStmt)
		systemm.CreateTable(table)
	case sqlparser.AlterStr:
	case sqlparser.DropStr:
	default:
		return "不支持的ddl类型:" + ddlStmt.Action
	}

	return "ok"
}

//根据ddl语句生成表结构体
func genTableByDdl(stmt *sqlparser.DDL) *systemm.Table {
	tableName := stmt.NewName.Name.String()

	genTable := new(systemm.Table)
	genTable.SchemaName = global.DefaultSchemaName
	genTable.Name = tableName

	astCols := stmt.TableSpec.Columns
	colNumber := len(astCols) //列数量
	columns := make([]systemm.Column, colNumber)
	for i := 0; i < colNumber; i++ {
		astCol := astCols[i]
		col := genColumn(astCol)
		columns[i] = col
	}
	//log.Println(columns)
	genTable.Columns = columns

	return genTable
}

//根据ddl生成column
func genColumn(astColDef *sqlparser.ColumnDefinition) systemm.Column {
	col := new(systemm.Column)
	astColName := astColDef.Name
	astColType := astColDef.Type

	col.Name = astColName.String()

	switch astColType.SQLType() {
	case sqltypes.Int32:
		col.DataType = systemm.TypeInt
	case sqltypes.VarChar:
		col.DataType = systemm.TypeString
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

func handSelect(selectStmt *sqlparser.Select) string {
	return querym.Select(selectStmt)
}

func handInsert(insertStmt *sqlparser.Insert) string {
	return querym.Insert(insertStmt)
}
