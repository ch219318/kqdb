package transaction

import "global"

//事务对象
type Trx struct {
	id             int
	startTime      int
	endTime        int
	isolationLevel string
	firstReadView  ReadView //第一个视图
}

func StartTrx() {
	global.IsolationLevel = "RR"
}

func EndTrx() {

}
