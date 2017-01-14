//使用b+树结构，order m，
//如果值为字符串，先使用hash得出对应的数字类型key，
package ix

//根据列名创建索引
func CreateIndex(colname string) (err error) {

}

//单个查询使用索引
//根据列名，值，获取数据文件seqid。
func GetSeqId(colname string, colval int) int {

}

//范围range查询使用索引
func GetSeqIds() {

}

//删除索引中关键字，可以是逻辑删除
func DelKey(key int) (err error) {

}

//插入索引关键字
func InsertKey(Key int) (err error) {

}
