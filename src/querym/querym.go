package querym

import (
	"encoding/json"
	"github.com/xwb1989/sqlparser"
	"kqdb/src/global"
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

func Select(selectStmt *sqlparser.Select) string {
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
	var projectOp *project
	var scanOp *tableScan
	var filterOp *filter

	sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case sqlparser.SelectExprs:
			//构建select op
			projectOp = new(project)
			//columns := make([]systemm.Column, 0)
			for _, v := range node {
				switch i := v.(type) {
				case *sqlparser.StarExpr:
					projectOp.colNames = nil
				case *sqlparser.AliasedExpr:
					log.Println(i)
				case sqlparser.Nextval:
				}
			}
		case sqlparser.TableExprs:
			tableName := node[0].(*sqlparser.AliasedTableExpr).
				Expr.(sqlparser.TableName).Name.String()
			schemaName := global.DefaultSchemaName
			//判断表是否存在
			isExist := systemm.TableIsExist(schemaName, tableName)
			if !isExist {
				panic(global.NewSqlError(schemaName + "." + tableName + "表不存在"))
			}
			//构建tableScan op
			scanOp = &tableScan{schemaName, tableName, 1, 0}
		case *sqlparser.Where:
			if node != nil {
				//构建filter op
				filterOp = new(filter)
				exprs := make([]sqlparser.Expr, 0)
				sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
					switch node := node.(type) {
					case sqlparser.Expr:
						exprs = append(exprs, node)
					}
					return true, nil
				}, node)
				reverse(exprs)
				filterOp.exprs = exprs
			}
		}
		return true, nil
	}, selectStmt)

	//组装op链
	if filterOp == nil {
		projectOp.child = scanOp
	} else {
		projectOp.child = filterOp
		filterOp.child = scanOp
	}

	plan := logicalPlan{projectOp}
	return plan
}

func reverse(s []sqlparser.Expr) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func Insert(insertStmt *sqlparser.Insert) string {
	schemaName := global.DefaultSchemaName
	tableName := insertStmt.Table.Name.String()

	//判断表是否存在
	isExist := systemm.TableIsExist(schemaName, tableName)
	if !isExist {
		return schemaName + "." + tableName + "表不存在"
	}

	//获取表
	table := systemm.GetTable(schemaName, tableName)
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
			tuple := recordm.Tuple{-1, schemaName, tableName, content}
			log.Println(tuple)
			rmFileHandle := recordm.OpenFileHandle(schemaName, tableName)
			rmFileHandle.InsertRecord(tuple)

		}
	}

	return "ok"
}
