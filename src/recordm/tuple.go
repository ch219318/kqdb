package recordm

import (
	"encoding/binary"
	"kqdb/src/systemm"
	"kqdb/src/utils"
	"log"
	"strconv"
)

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

	columns := systemm.GetTable(tuple.SchemaName, tuple.TableName).Columns
	for i, v := range columns {
		colVal, ok := tuple.Content[v.Name]

		if ok {
			//如果map中存在当前列，序列化列值后加入data
			var colValBytes []byte
			switch v.DataType {
			case systemm.TypeInt:
				colValInt, err := strconv.ParseInt(colVal, 10, 32)
				if err != nil {
					log.Panic(err)
				}
				colValBytes = utils.Int32ToBytes(int32(colValInt))
			case systemm.TypeString:
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

	columns := systemm.GetTable(tuple.SchemaName, tuple.TableName).Columns
	for i, v := range columns {
		//如果当前列非空
		isNotNull := (nullBitMap & uint16(1<<uint(i))) == 0
		if isNotNull {
			//从data字节数组中反序列化列值
			var colVal string
			switch v.DataType {
			case systemm.TypeInt:
				colValBytes := data[0:4]
				colVal = strconv.FormatInt(int64(utils.BytesToInt32(colValBytes)), 10)
				//去掉data中已使用部分
				data = data[4:]
			case systemm.TypeString:
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
