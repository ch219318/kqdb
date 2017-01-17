//使用b+树结构，order 6，
//如果值为字符串，先使用hash得出对应的数字类型key
package ix

import (
	"errors"
	"os"
)

const (
	SIZE_B                    int64 = 1
	SIZE_K                          = 1024 * SIZE_B
	SIZE_M                          = 1024 * SIZE_K
	OVERFLOW_PAGE_SIZE              = 1 * SIZE_K //溢出页大小
	ROOTNODE_INFO_HEADER_SIZE       = 1 * SIZE_M //b树索引根节点消息文件头大小
	//每一个根节点信息大小,第一个字节1表示使用中,0表示未使用，2-16表示字段名字，17-20表示根节点位置
	ROOTNODE_INFO_SIZE   = 1 * SIZE_K
	INDEX_FILE_EXT_NAME  = "myi"      //索引文件扩展名
	INDEX_FILE_BASE_PATH = "../data/" //索引文件位置
)

//b＋树非叶子节点
type NonLeafNode struct {
	key1   int //0为空
	key2   int
	key3   int
	key4   int
	key5   int
	point1 int //相对于文件头偏移量，单位字节
	point2 int
	point3 int
	point4 int
	point5 int
	point6 int
}

//b＋树叶子节点
type LeafNode struct {
	nodetype int //-1表示为叶子节点
	key1     int
	key2     int
	key3     int
	key4     int
	key5     int
	value1   int //负数为溢出页地址，正数为seqid,0为空或者被删除
	value2   int
	value3   int
	value4   int
	value5   int
}

//创建索引文件
func CreateIndexFile(tablename string, schemaname string) (err error) {
	fullname := INDEX_FILE_BASE_PATH + schemaname + "/" + tablename + "." + INDEX_FILE_EXT_NAME
	file, err1 := os.Create(fullname)
	defer file.Close()
	if err1 == nil {
		buf := make([]byte, 1024)
		for i := 0; i < 1024; i++ {
			file.Write(buf)
		}
	} else {
		return err1
	}
	return
}

//根据列名创建索引
func CreateIndex(colname string, tablename string, schemaname string) (err error) {
	colnameBytes := []byte(colname)
	if len(colnameBytes) > 15 {
		return errors.New(colname + "字段名过长")
	}
	fullname := INDEX_FILE_BASE_PATH + schemaname + "/" + tablename + "." + INDEX_FILE_EXT_NAME
	_, err1 := os.Stat(fullname)
	if err1 != nil {
		CreateIndexFile(tablename, schemaname)
	}
	indexfile, err2 := os.OpenFile(fullname, os.O_RDWR|os.O_APPEND, os.ModePerm)
	if err2 == nil {

		//写入根节点

		firstbyte := make([]byte, 1)
		for i := 0; i < 1024; i++ { //最多有1024个索引
			n, err3 := indexfile.ReadAt(firstbyte, i*1024)
			if (n == 1) && (err3 == nil) && (firstbyte[0] == 0) {
				//写入根节点元信息
				rootnodemeta := make([]byte, ROOTNODE_INFO_SIZE)
				rootnodemeta[0] = 1
				for m := range colnameBytes {
					rootnodemeta[m+1] = colnameBytes[m]
				}

				indexfile.WriteAt(rootnodemeta, i*1024)
				return err
			}
		}
		return errors.New("文件索引头使用完毕") //如果1024个索引都被占用，返回错误
	}
}

//单个查询使用索引
//根据列名，值，获取数据文件seqid。
func GetSeqId(colname string, colval int) (i int) {
	return i
}

//范围range查询使用索引
func GetSeqIds() {

}

//删除索引中关键字，可以是逻辑删除
func DelKey(key int) (err error) {
	return err
}

//插入索引关键字
func InsertKey(Key int) (err error) {
	return err
}
