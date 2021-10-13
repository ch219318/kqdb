package sqlm

import (
	"encoding/json"
	"fmt"
	"github.com/xwb1989/sqlparser"
	"kqdb/src/global"
	"kqdb/src/recordm"
	"log"
)

type logicalPlan struct {
	root relationAlgebraOp
}

type physicalPlan struct {
	root relationAlgebraOp
}

//执行sql
func HandSql(sql string) (result string) {
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

	defer func() {
		if err := recover(); err != nil {
			result = "程序发生严重错误" + fmt.Sprintf("%v", err)
		}
	}()

	log.Printf("result的值为%v\n", result)
	return
}

func handDdl(stmt *sqlparser.DDL) string {
	switch stmt.Action {
	case sqlparser.CreateStr:
		table, err := recordm.GenTableByDdl(stmt)
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
		return "不支持的ddl类型:" + stmt.Action
	}

	return "ok"
}

func handSelect(stmt *sqlparser.Select) string {
	//var tuples []recordm.Tuple

	//语义检查
	check(stmt)

	//生成逻辑计划
	logicalPlan := transToLocalPlan(stmt)

	//生成物理计划
	physicalPlan := physicalPlan{logicalPlan.root}

	//执行
	op := physicalPlan.root
	var tuples []recordm.Tuple
	for e := op.getNextTuple(); e != nil; e = op.getNextTuple() {
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

func transToLocalPlan(stmt sqlparser.SQLNode) logicalPlan {
	//root := project{}
	//op1 := tableScan{}

	sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch n := node.(type) {
		case *sqlparser.AliasedExpr:
			_ = n.Expr
		}
		return true, nil
	}, stmt)

	plan := logicalPlan{}
	return plan
}

func handInsert(stmt *sqlparser.Insert) string {
	tableName := stmt.Table.Name.String()
	log.Println(tableName)

	//判断表是否存在
	isExist := recordm.TableIsExist(tableName)
	if !isExist {
		return global.DefaultSchemaName + "." + tableName + "表不存在"
	}

	//获取表
	table := recordm.GetTable(tableName)
	columns := table.Columns

	switch node := stmt.Rows.(type) {
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
