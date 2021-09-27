package sqlm

import (
	"fmt"
	"github.com/xwb1989/sqlparser"
	"log"
)

//执行sql
func HandSql(sql string) (result string) {
	log.Printf("sql：%s\n", sql)

	result = "ok"

	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		log.Println("sql格式错误:" + sql)
		return "sql格式错误:" + sql
	}

	sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch n := node.(type) {
		case *sqlparser.Select:
			_ = n
			result = "abc|123|fg"
		case *sqlparser.Insert:
		case *sqlparser.DDL:
		}
		return true, nil
	}, stmt)

	defer func() {
		if err := recover(); err != nil {
			result = "程序发生严重错误" + fmt.Sprintf("%v", err)
		}
	}()

	log.Printf("result的值为%v\n", result)
	return result
}

func handDdl(stmt *sqlparser.DDL) string {
	//table, err := sm.GenTableByDdl(input)
	//if err != nil {
	//	return err.Error()
	//}
	//err1 := sm.SaveTableToFile(table)
	//if err1 != nil {
	//	return err1.Error()
	//}
	return "op ok"
}

func handSelect(stmt *sqlparser.Select) string {
	//var rows []recordm.Row
	return ""
}

func handInsert(stmt *sqlparser.Insert) string {
	return "result"
}
