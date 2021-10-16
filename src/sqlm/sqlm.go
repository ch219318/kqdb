package sqlm

import (
	"encoding/json"
	"github.com/xwb1989/sqlparser"
	"kqdb/src/global"
	"kqdb/src/recordm"
	"log"
)

func init() {
	global.InitLog()
}

type logicalPlan struct {
	root relationAlgebraOp
}

type physicalPlan struct {
	root relationAlgebraOp
}

//执行sql
func HandSql(sql string) (result string) {
	defer func() {
		if pa := recover(); pa != nil {
			//只处理SqlError
			if sqlError, ok := pa.(*global.SqlError); ok {
				errMsg := sqlError.Error()
				log.Println(errMsg)
				result = errMsg
			} else {
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
		table, err := recordm.GenTableByDdl(ddlStmt)
		if err != nil {
			return err.Error()
		}
		err1 := recordm.GenFileForTable(table)
		if err1 != nil {
			return err1.Error()
		}
	case sqlparser.AlterStr:
	case sqlparser.DropStr:
	default:
		return "不支持的ddl类型:" + ddlStmt.Action
	}

	return "ok"
}

func handSelect(selectStmt *sqlparser.Select) string {
	//var tuples []recordm.Tuple

	//语义检查
	check(selectStmt)

	//生成逻辑计划
	logicalPlan := transToLocalPlan(selectStmt)

	//生成物理计划
	physicalPlan := physicalPlan{logicalPlan.root}

	//执行
	rootOp := physicalPlan.root
	var tuples []recordm.Tuple
	for e := rootOp.getNextTuple(); e != nil; e = rootOp.getNextTuple() {
		tuples = append(tuples, *e)
	}

	bytes, err := json.Marshal(tuples)
	if err != nil {
		return err.Error()
	}
	return string(bytes)
}

func check(statement sqlparser.Statement) {

}

func transToLocalPlan(selectStmt *sqlparser.Select) logicalPlan {
	tableName := ([]sqlparser.TableExpr)(selectStmt.From)[0].(*sqlparser.AliasedTableExpr).
		Expr.(sqlparser.TableName).Name.String()

	//构建select op
	columns := make([]recordm.Column, 0)
	for _, v := range selectStmt.SelectExprs {
		switch i := v.(type) {
		case *sqlparser.StarExpr:
			columns = recordm.SchemaMap[global.DefaultSchemaName][tableName].Columns
		case *sqlparser.AliasedExpr:
			log.Println(i)
		case sqlparser.Nextval:
		}

	}
	root := new(project)
	root.selectedCols = columns

	//构建tableScan op
	op1 := tableScan{global.DefaultSchemaName, tableName, 1, 0}

	//组装op链
	root.child = &op1

	plan := logicalPlan{root}
	return plan
}

func handInsert(insertStmt *sqlparser.Insert) string {
	tableName := insertStmt.Table.Name.String()
	log.Println(tableName)

	//判断表是否存在
	isExist := recordm.TableIsExist(tableName)
	if !isExist {
		return global.DefaultSchemaName + "." + tableName + "表不存在"
	}

	//获取表
	table := recordm.GetTable(tableName)
	columns := table.Columns

	switch node := insertStmt.Rows.(type) {
	case sqlparser.Values:
		for _, valTuple := range node {

			//构造tuple
			content := make(map[string]string)
			for i, expr := range valTuple {
				switch expr := expr.(type) {
				case *sqlparser.SQLVal:

					//sqlVal转string
					var colVal string
					switch expr.Type {
					case sqlparser.StrVal:
						colVal = string(expr.Val)
					case sqlparser.IntVal:
						colVal = string(expr.Val)
					default:
						return "不支持的类型"
					}
					//log.Println("colVal:" + colVal)

					column := columns[i]
					content[column.Name] = colVal
				}
			}
			tuple := recordm.Tuple{-1, global.DefaultSchemaName, tableName, content}
			log.Println(tuple)
			recordm.InsertRecord(tuple)

		}
	}

	return "ok"
}
