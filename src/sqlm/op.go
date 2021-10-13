package sqlm

import (
	"kqdb/src/global"
	"kqdb/src/recordm"
)

type relationAlgebraOp interface {
	getNextTuple() *recordm.Tuple
}

type indexScan struct {
}

type tableScan struct {
	tableName       string
	pageCursor      int //当前page
	pageTupleCursor int //当前page中第几个tuple
}

func (ts *tableScan) getNextTuple() *recordm.Tuple {
	tuple := ts.getTuple()

	//如果没有获取到tuple，则pageCursor+1，pageTupleCursor置0，重新获取
	if tuple == nil {
		ts.pageCursor = ts.pageCursor + 1
		ts.pageTupleCursor = 0
		tuple = ts.getTuple()
	}

	return tuple
}

//获取当前cursor中的tuple
func (ts *tableScan) getTuple() *recordm.Tuple {
	//从buffer_pool中获取page
	tName := recordm.TableName(ts.tableName)
	var page recordm.Page
	pageList := recordm.BufferPool[global.DefaultSchemaName][tName].PageList
	for e := pageList.Front(); e != nil; e = e.Next() {
		p := e.Value.(recordm.Page)
		if p.PageNum == ts.pageCursor {
			page = p
			break
		}
	}
	//如果page链上没有，从脏链上获取
	if (page == recordm.Page{}) {
		dirtyPageList := recordm.BufferPool[global.DefaultSchemaName][tName].DirtyPageList
		for e := dirtyPageList.Front(); e != nil; e = e.Next() {
			p := e.Value.(recordm.Page)
			if p.PageNum == ts.pageCursor {
				page = p
				break
			}
		}
	}

	//如果buffer_pool中page不存在，从文件中获取page，并放入buffer_pool
	if (page == recordm.Page{}) {
		p := GetPage(tName, ts.pageCursor)
		if p != nil {
			page = *p
			//放入buffer_pool
			pageList.PushFront(*p)
		} else {
			return nil
		}
	}

	//从page中获取tuple
	tupleList := page.TupleList
	for e := tupleList.Front(); e != nil; e = e.Next() {
		tuple := e.Value.(recordm.Tuple)
		if ts.pageTupleCursor == tuple.TupleNum {
			return &tuple
		}
	}

	return nil
}

func GetPage(name recordm.TableName, pageNum int) *recordm.Page {
	return nil
}

type project struct {
	child        relationAlgebraOp
	selectedCols []recordm.Column
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
	//leftColumns := recordm.SchemaMap[j.leftTuple.SchemaName][j.leftTuple.TableName].Columns
	//rightColumns := recordm.SchemaMap[rightTuple.SchemaName][rightTuple.TableName].Columns
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
