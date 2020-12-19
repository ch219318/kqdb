package rm

func InsertRecord(bytes []byte) (nodeId int, err error) {

	return nodeId, err
}

func DelRecord(nodeId int) (err error) {
	return err
}

func UpdateRecord(bytes []byte, nodeId int) (err error) {
	return err

}

func GetRecord(nodeId int) (bytes []byte, err error) {
	//获取当前readView

	//根据readView筛选多版本记录
	return bytes, err
}
