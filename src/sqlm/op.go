package sqlm

import "kqdb/src/recordm"

type relationAlgebraOp interface {
	getNextRow() recordm.Row
}

type indexScan struct {
}

type tableScan struct {
}

type project struct {
	child relationAlgebraOp
}

func (p *project) getNextRow() recordm.Row {
	row := p.child.getNextRow()
	if row == (recordm.Row{}) {
		return recordm.Row{}
	} else {
		result := row
		return result
	}
}

type filter struct {
}

type join struct {
	left         relationAlgebraOp
	right        relationAlgebraOp
	leftRow      recordm.Row   //指示当前左表行
	allRightRowS []recordm.Row //所有右表数据
	cursor       int           //上面数据集的游标,初始值为0
}

func (j *join) getNextRow() recordm.Row {

	//加载右表所有数据，优化方向：加载小表
	if len(j.allRightRowS) == 0 {
		for {
			row := j.right.getNextRow()
			if row == (recordm.Row{}) {
				break
			} else {
				j.allRightRowS = append(j.allRightRowS, row)
			}
		}
	}

	//初始化当前左表指示
	if j.leftRow == (recordm.Row{}) {
		j.leftRow = j.left.getNextRow()
	}

	//生成最终row
	resultRow := recordm.Row{}
	rightRow := j.allRightRowS[j.cursor]
	//todo

	//收尾：如果当前cursor为末尾，则获取下一个左表数据，cursor重置为0；否则，cursor加1
	if len(j.allRightRowS) == j.cursor+1 { //cursor处于最末尾
		j.leftRow = j.left.getNextRow()
		j.cursor = 0
	} else {
		j.cursor = j.cursor + 1
	}

	return resultRow

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
