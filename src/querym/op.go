package querym

import (
	"kqdb/src/filem"
	"kqdb/src/global"
	"kqdb/src/recordm"
	"kqdb/src/systemm"
	"path/filepath"
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
	child        relationAlgebraOp
	selectedCols []systemm.Column
}

func (p project) getNextTuple() *recordm.Tuple {
	tuple := p.child.getNextTuple()
	if tuple == nil {
		return nil
	} else {
		m := make(map[string]string)
		for _, col := range p.selectedCols {
			m[col.Name] = tuple.Content[col.Name]
		}
		tuple.Content = m
		return tuple
	}
}

type filter struct {
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
