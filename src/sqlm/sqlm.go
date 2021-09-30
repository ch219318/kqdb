package sqlm

import (
	"encoding/json"
	"fmt"
	"github.com/xwb1989/sqlparser"
	"kqdb/src/recordm"
	"kqdb/src/systemm"
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
		table, err := systemm.GenTableByDdl(stmt)
		if err != nil {
			return err.Error()
		}
		err1 := systemm.SaveTableToFile(table)
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
	//var rows []recordm.Row

	//语义检查
	check(stmt)

	//生成逻辑计划
	logicalPlan := transToLocalPlan(stmt)

	//生成物理计划
	physicalPlan := physicalPlan{logicalPlan.root}

	//执行
	op := physicalPlan.root
	var rows []recordm.Row
	for e := op.getNextRow(); e != nil; e = op.getNextRow() {
		rows = append(rows, *e)
	}

	bytes, err := json.Marshal(rows)
	if err != nil {
		return err.Error()
	}
	return string(bytes)
}

func check(statement sqlparser.Statement) {

}

func transToLocalPlan(stmt sqlparser.SQLNode) logicalPlan {
	root := project{}
	op1 := tableScan{}

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
	return "result"
}
