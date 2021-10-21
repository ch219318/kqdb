package querym

import (
	"github.com/xwb1989/sqlparser"
	"kqdb/src/filem"
	"kqdb/src/global"
	"kqdb/src/recordm"
	"log"
	"path/filepath"
	"strconv"
)

type relationAlgebraOp interface {
	getNextTuple() *recordm.Tuple
}

type indexScan struct {
}

type tableScan struct {
	schemaName      string
	tableName       string
	pageCursor      int //当前page
	pageTupleCursor int //当前page中第几个tuple
}

func (ts *tableScan) getNextTuple() *recordm.Tuple {
	//从cursor处遍历page
	fileP := filepath.Join(global.DataDir, ts.schemaName, ts.tableName+"."+filem.DataFileSuf)
	fileHandler := filem.GetFile(fileP)
	for ts.pageCursor < fileHandler.TotalPage {
		page := fileHandler.GetPage(ts.pageCursor)
		items := page.Items
		totalTuple := len(items)

		//从cursor处遍历tuple
		for ts.pageTupleCursor < totalTuple {
			//从page中获取tuple
			tupleBytes := page.GetTupleBytes(ts.pageTupleCursor)
			if tupleBytes != nil {
				result := new(recordm.Tuple)
				result.UnMarshal(tupleBytes, ts.pageTupleCursor, ts.schemaName, ts.tableName)
				ts.pageTupleCursor++ //不能删除
				return result
			}
			ts.pageTupleCursor++
		}

		ts.pageCursor++
		ts.pageTupleCursor = 0
	}

	return nil
}

type project struct {
	child    relationAlgebraOp
	colNames []string
}

func (p *project) getNextTuple() *recordm.Tuple {
	tuple := p.child.getNextTuple()
	if tuple == nil {
		return nil
	} else {
		//星号列
		if p.colNames == nil {
			return tuple
		} else {
			m := make(map[string]string)
			for _, colName := range p.colNames {
				if colVal, ok := tuple.Content[colName]; ok {
					m[colName] = colVal
				} else {
					panic(global.NewSqlError("不存在列：" + colName))
				}
			}
			tuple.Content = m
			return tuple
		}

	}
}

type filter struct {
	child relationAlgebraOp
	exprs []sqlparser.Expr
}

func (f *filter) getNextTuple() *recordm.Tuple {
	for tuple := f.child.getNextTuple(); tuple != nil; tuple = f.child.getNextTuple() {
		//判断tuple是否符合
		calcResultMap := make(map[sqlparser.Expr]string)
		for _, e := range f.exprs {
			switch expr := e.(type) {
			case *sqlparser.SQLVal:
				calcResultMap[expr] = string(expr.Val)
			case *sqlparser.ColName:
				name := expr.Name.String()
				if colVal, ok := tuple.Content[name]; ok {
					calcResultMap[expr] = colVal
				} else {
					panic(global.NewSqlError("where中列不存在：" + name))
				}
			case *sqlparser.AndExpr:
				calcAnd(expr, calcResultMap)
			case *sqlparser.OrExpr:
				calcOr(expr, calcResultMap)
			case *sqlparser.BinaryExpr:
				calcBin(expr, calcResultMap)
			case *sqlparser.ComparisonExpr:
				calcCom(expr, calcResultMap)
			default:
				panic(global.NewSqlError("不支持的表达式"))
			}
		}
		result := calcResultMap[f.exprs[len(f.exprs)-1]]
		resultBool, err := strconv.ParseBool(result)
		if err != nil {
			log.Panicln("解析result字符串出错")
		}
		if resultBool {
			return tuple
		}
	}

	return nil
}

func calcAnd(expr *sqlparser.AndExpr, calcResultMap map[sqlparser.Expr]string) {
	leftStr, ok := calcResultMap[expr.Left]
	if !ok {
		log.Panicln("leftStr", "不存在")
	}
	rightStr, ok := calcResultMap[expr.Right]
	if !ok {
		log.Panicln("rightStr", "不存在")
	}
	leftBool, err := strconv.ParseBool(leftStr)
	if err != nil {
		log.Panicln("转化leftBool出错")
	}
	rightBool, err := strconv.ParseBool(rightStr)
	if err != nil {
		log.Panicln("转化rightBool出错")
	}
	andResult := strconv.FormatBool(leftBool && rightBool)
	calcResultMap[expr] = andResult
}

func calcOr(expr *sqlparser.OrExpr, calcResultMap map[sqlparser.Expr]string) {
	leftStr, ok := calcResultMap[expr.Left]
	if !ok {
		log.Panicln("leftStr", "不存在")
	}
	rightStr, ok := calcResultMap[expr.Right]
	if !ok {
		log.Panicln("rightStr", "不存在")
	}
	leftBool, err := strconv.ParseBool(leftStr)
	if err != nil {
		log.Panicln("转化leftBool出错")
	}
	rightBool, err := strconv.ParseBool(rightStr)
	if err != nil {
		log.Panicln("转化rightBool出错")
	}
	orResult := strconv.FormatBool(leftBool || rightBool)
	calcResultMap[expr] = orResult
}

func calcCom(expr *sqlparser.ComparisonExpr, calcResultMap map[sqlparser.Expr]string) {
	leftStr, ok := calcResultMap[expr.Left]
	if !ok {
		log.Panicln("leftStr", "不存在")
	}
	rightStr, ok := calcResultMap[expr.Right]
	if !ok {
		log.Panicln("rightStr", "不存在")
	}
	leftInt, _ := strconv.ParseInt(leftStr, 10, 32)
	rightInt, _ := strconv.ParseInt(rightStr, 10, 32)

	switch expr.Operator {
	case sqlparser.EqualStr:
		comResult := strconv.FormatBool(leftStr == rightStr)
		calcResultMap[expr] = comResult
	case sqlparser.LessThanStr:
		comResult := strconv.FormatBool(leftInt < rightInt)
		calcResultMap[expr] = comResult
	case sqlparser.GreaterThanStr:
		comResult := strconv.FormatBool(leftInt > rightInt)
		calcResultMap[expr] = comResult
	}
}

func calcBin(expr *sqlparser.BinaryExpr, calcResultMap map[sqlparser.Expr]string) {
	leftStr, ok := calcResultMap[expr.Left]
	if !ok {
		log.Panicln("leftStr", "不存在")
	}
	rightStr, ok := calcResultMap[expr.Right]
	if !ok {
		log.Panicln("rightStr", "不存在")
	}
	leftInt, err := strconv.ParseInt(leftStr, 10, 32)
	if err != nil {
		log.Panicln("转化leftInt出错")
	}
	rightInt, err := strconv.ParseInt(rightStr, 10, 32)
	if err != nil {
		log.Panicln("转化rightInt出错")
	}

	switch expr.Operator {
	case sqlparser.PlusStr:
		binResult := strconv.FormatInt(leftInt+rightInt, 10)
		calcResultMap[expr] = binResult
	case sqlparser.MinusStr:
		binResult := strconv.FormatInt(leftInt-rightInt, 10)
		calcResultMap[expr] = binResult
	case sqlparser.MultStr:
		binResult := strconv.FormatInt(leftInt*rightInt, 10)
		calcResultMap[expr] = binResult
	case sqlparser.DivStr:
		binResult := strconv.FormatInt(leftInt/rightInt, 10)
		calcResultMap[expr] = binResult
	}
}

type join struct {
	left           relationAlgebraOp
	right          relationAlgebraOp
	leftTuple      *recordm.Tuple  //指示当前左表行
	allRightTupleS []recordm.Tuple //所有右表数据
	cursor         int             //上面数据集的游标,初始值为0
}

func (j *join) getNextTuple() *recordm.Tuple {

	//加载右表所有数据，优化方向：加载小表
	if len(j.allRightTupleS) == 0 {
		for {
			tuple := j.right.getNextTuple()
			if tuple == nil {
				break
			} else {
				j.allRightTupleS = append(j.allRightTupleS, *tuple)
			}
		}
	}

	//初始化当前左表指示
	if j.leftTuple == nil {
		j.leftTuple = j.left.getNextTuple()
	}

	//生成最终tuple
	rightTuple := j.allRightTupleS[j.cursor]

	//构建临时表
	//leftColumns := systemm.recordm.schemaMap[j.leftTuple.SchemaName][j.leftTuple.TableName].Columns
	//rightColumns := systemm.recordm.schemaMap[rightTuple.SchemaName][rightTuple.TableName].Columns
	//tempTableCols := append(leftColumns, rightColumns...)
	tempTableName := j.leftTuple.TableName + rightTuple.TableName
	//tempTable := recordm.Table{tempTableName, tempTableCols}

	//todo 列名可能重复
	resultContent := mergeMaps(j.leftTuple.Content, rightTuple.Content)
	resultTuple := recordm.Tuple{0, "tempSchema", tempTableName, resultContent}

	//收尾：如果当前cursor为末尾，则获取下一个左表数据，cursor重置为0；否则，cursor加1
	if len(j.allRightTupleS) == j.cursor+1 { //cursor处于最末尾
		j.leftTuple = j.left.getNextTuple()
		j.cursor = 0
	} else {
		j.cursor = j.cursor + 1
	}

	return &resultTuple

}

func mergeMaps(maps ...map[string]string) map[string]string {
	result := make(map[string]string)
	for _, m := range maps {
		for k, v := range m {
			result[k] = v
		}
	}
	return result
}

type aggregation struct {
}

type values struct {
}

type sort struct {
}

type topN struct {
}

type output struct {
}
