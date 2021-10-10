package transaction

//key为事务id，value为是否提交
type ReadView map[int]bool

func getCurrentReadView() ReadView {
	currentRV := make(map[int]bool)
	return currentRV
}
