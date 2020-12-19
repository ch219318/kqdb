package transaction

import "global"

//事务对象
type Trx struct {
	id             int64
	startTime      int32
	endTime        int32
	isolationLevel string
	firstReadView  ReadView //第一个视图
}

func StartTrx() {
	global.IsolationLevel = "RR"
}

func EndTrx() {

}
