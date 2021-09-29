package sqlm

import (
	"kqdb/src/recordm"
	"kqdb/src/systemm"
)

type relationAlgebraOp interface {
	getNextRow() *recordm.Row
}

type indexScan struct {
}

type tableScan struct {
	tableName     string
	pageCursor    int //当前page
	pageRowCursor int //当前page中第几个row
}

func (ts tableScan) getNextRow() *recordm.Row {
	return nil
}

type project struct {
	child        relationAlgebraOp
	selectedCols []systemm.Column
}

func (p project) getNextRow() *recordm.Row {
	row := p.child.getNextRow()
	if row == nil {
		return nil
	} else {
		m := make(map[string]string)
		for _, col := range p.selectedCols {
			m[col.Name] = row.Content[col.Name]
		}
		row.Content = m
		return row
	}
}

type filter struct {
}

type join struct {
	left         relationAlgebraOp
	right        relationAlgebraOp
	leftRow      *recordm.Row  //指示当前左表行
	allRightRowS []recordm.Row //所有右表数据
	cursor       int           //上面数据集的游标,初始值为0
}

func (j *join) getNextRow() *recordm.Row {

	//加载右表所有数据，优化方向：加载小表
	if len(j.allRightRowS) == 0 {
		for {
			row := j.right.getNextRow()
			if row == nil {
				break
			} else {
				j.allRightRowS = append(j.allRightRowS, *row)
			}
		}
	}

	//初始化当前左表指示
	if j.leftRow == nil {
		j.leftRow = j.left.getNextRow()
	}

	//生成最终row
	rightRow := j.allRightRowS[j.cursor]

	//构建临时表
	tempTableCols := append(j.leftRow.Table.Columns, rightRow.Table.Columns...)
	tempTableName := j.leftRow.Table.Name + rightRow.Table.Name
	tempTable := systemm.Table{tempTableName, tempTableCols}

	//todo 列名可能重复
	resultContent := mergeMaps(j.leftRow.Content, rightRow.Content)
	resultRow := recordm.Row{tempTable, resultContent}

	//收尾：如果当前cursor为末尾，则获取下一个左表数据，cursor重置为0；否则，cursor加1
	if len(j.allRightRowS) == j.cursor+1 { //cursor处于最末尾
		j.leftRow = j.left.getNextRow()
		j.cursor = 0
	} else {
		j.cursor = j.cursor + 1
	}

	return &resultRow

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
