package recordm

import (
	"container/list"
	"encoding/binary"
	"kqdb/src/filem"
	"kqdb/src/global"
	"kqdb/src/utils"
	"log"
	"strconv"
)

//纪录管理模块

func init() {
	global.InitLog()
}

type rawTuple struct {
	len        int     //行的长度
	nullBitMap [2]byte //空值位图
	data       []byte  //实际数据，非定长类型数据=2字节长度+实际内容
}

//tuple
type Tuple struct {
	TupleNum   int               //从0开始
	SchemaName string            //schema名
	TableName  string            //表名
	Content    map[string]string //列名：列值，列值为string
}

func (tuple *Tuple) Marshal() []byte {
	nullBitMap := uint16(0)
	data := make([]byte, 0)

	columns := SchemaMap[tuple.SchemaName][tuple.TableName].Columns
	for i, v := range columns {
		colVal, ok := tuple.Content[v.Name]

		if ok {
			//如果map中存在当前列，序列化列值后加入data
			var colValBytes []byte
			switch v.DataType {
			case TypeInt:
				colValInt, err := strconv.ParseInt(colVal, 10, 32)
				if err != nil {
					log.Panic(err)
				}
				colValBytes = utils.Int32ToBytes(int32(colValInt))
			case TypeString:
				colValBytes = []byte(colVal)
				lenBytes := make([]byte, 2)
				binary.BigEndian.PutUint16(lenBytes, uint16(len(colValBytes)))
				colValBytes = append(lenBytes, colValBytes...)
			default:
				log.Panic("不支持的类型")
			}
			data = append(data, colValBytes...)
		} else {
			//如果map中不存在当前列，相应空值位图位置设置为1
			nullBitMap |= uint16(1) << uint(i)
		}

	}

	nullBitMapBytes := make([]byte, 2, 2)
	binary.BigEndian.PutUint16(nullBitMapBytes, nullBitMap)
	result := append(nullBitMapBytes, data...)
	return result
}

func (tuple *Tuple) UnMarshal(bytes []byte, tupleNum int, schemaName string, tableName string) {
	tuple.TupleNum = tupleNum
	tuple.SchemaName = schemaName
	tuple.TableName = tableName

	//反序列化
	nullBitMapBytes := bytes[0:2]
	data := bytes[2:]
	nullBitMap := binary.BigEndian.Uint16(nullBitMapBytes)

	content := make(map[string]string)

	columns := SchemaMap[tuple.SchemaName][tuple.TableName].Columns
	for i, v := range columns {
		//如果当前列非空
		isNotNull := (nullBitMap & uint16(1<<uint(i))) == 0
		if isNotNull {
			//从data字节数组中反序列化列值
			var colVal string
			switch v.DataType {
			case TypeInt:
				colValBytes := data[0:4]
				colVal = strconv.FormatInt(int64(utils.BytesToInt32(colValBytes)), 10)
				//去掉data中已使用部分
				data = data[4:]
			case TypeString:
				lenBytes := data[0:2]
				len := binary.BigEndian.Uint16(lenBytes)
				colValBytes := data[2 : 2+len]
				colVal = string(colValBytes)
				//去掉data中已使用部分
				data = data[2+len:]
			default:
				log.Panic("不支持的类型")
			}
			//把列值加入content
			content[v.Name] = colVal
		}

		tuple.Content = content
	}
}

type Page struct {
	PageNum    int        //从0开始
	SchemaName string     //schema名
	TableName  string     //表名
	TupleList  *list.List //元素不是tuple指针
}

//page序列化
func (page *Page) Marshal() []byte {
	itemsBytes := make([]byte, 0)
	tuplesBytes := make([]byte, 0)

	tupleList := page.TupleList
	for e := tupleList.Front(); e != nil; e = e.Next() {
		//tuple序列化
		tuple := e.Value.(Tuple)
		tupleBs := tuple.Marshal()
		tuplesBytes = append(tupleBs, tuplesBytes...)

		//item生成
		tupleOffset := filem.PageSize - len(tuplesBytes)
		tupleLen := len(tupleBs)
		flag := 1
		item := tupleOffset<<17 + flag<<15 + tupleLen
		itemBs := make([]byte, 4)
		binary.BigEndian.PutUint32(itemBs, uint32(item))
		//todo
		//a := bits.Add()
		itemsBytes = append(itemsBytes, itemBs...)
	}

	//page头
	headerBytes := make([]byte, 24)
	pageLower := uint16(24 + len(itemsBytes))
	pageUpper := uint16(filem.PageSize - 1 - len(tuplesBytes))
	binary.BigEndian.PutUint16(headerBytes[2:4], pageLower)
	binary.BigEndian.PutUint16(headerBytes[4:6], pageUpper)

	//空白区
	blankLen := filem.PageSize - len(headerBytes) - len(itemsBytes) - len(tuplesBytes)
	blankBytes := make([]byte, blankLen)

	pageBytes := append(append(append(headerBytes, itemsBytes...), blankBytes...), tuplesBytes...)
	return pageBytes
}

//page反序列化
func (page *Page) UnMarshal(bytes []byte, pageNum int, schemaName string, tableName string) {
	if len(bytes) != filem.PageSize {
		log.Panic("page size大小出错：" + string(len(bytes)))
	}

	page.PageNum = pageNum
	page.SchemaName = schemaName
	page.TableName = tableName

	tupleList := list.New()
	page.TupleList = tupleList

	//page头
	headerBytes := bytes[0:24]
	pageLower := binary.BigEndian.Uint16(headerBytes[2:4])
	//pageUpper := binary.BigEndian.Uint16(headerBytes[4:6])

	//item区
	itemsBytes := bytes[24:pageLower]
	itemsNum := len(itemsBytes) / 4
	for i := 0; i < itemsNum; i++ {
		itemBs := itemsBytes[i*4 : (i+1)*4]
		item := binary.BigEndian.Uint32(itemBs)
		tupleOffset := item >> 17
		tupleLen := item & 0x7FFF
		flag := item & 0x18000 >> 15
		if flag == 1 {
			tupleBs := bytes[tupleOffset : tupleOffset+tupleLen]
			tuple := new(Tuple)
			tuple.UnMarshal(tupleBs, i, schemaName, tableName)
			tupleList.PushBack(*tuple)
		}
	}

}

func InsertRecord(tuple Tuple) (err error) {
	t := TableName(tuple.TableName)
	dirtyPageList := BufferPool[global.DefaultSchemaName][t].DirtyPageList
	pageList := BufferPool[global.DefaultSchemaName][t].PageList

	//如果dirty链上有page
	if dirtyPageList.Len() > 0 {
		for e := dirtyPageList.Front(); e != nil; e = e.Next() {
			dirtyPage := e.Value.(Page)
			//设置tupleNum
			tuple.TupleNum = dirtyPage.TupleList.Len()
			dirtyPage.TupleList.PushBack(tuple)
			break
		}
	} else {
		var page Page
		for e := pageList.Front(); e != nil; e = e.Next() {
			page = e.Value.(Page)
			//设置tupleNum
			tuple.TupleNum = page.TupleList.Len()
			page.TupleList.PushBack(tuple)
			//从page链中删除当前page
			pageList.Remove(e)
			break
		}
		//加入dirty链
		dirtyPageList.PushBack(page)
	}

	return
}

func DelRecord(nodeId int) (err error) {
	return
}

func UpdateRecord(bytes []byte, nodeId int) (err error) {
	return

}

func GetRecord(nodeId int) (bytes []byte, err error) {
	//获取当前readView

	//根据readView筛选多版本记录
	return
}
