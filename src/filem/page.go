package filem

import (
	"encoding/binary"
	"log"
)

type Page struct {
	PageNum    int    //从0开始
	SchemaName string //schema名
	TableName  string //表名
	Lower      int
	Upper      int
	Items      []*Item
	Content    []byte //page内容,从upper位置到文件尾
}

type Item struct {
	Flag   int
	Offset int
	Len    int
}

//page序列化，返回，包括page头+
func (page *Page) Marshal() []byte {
	//item区
	itemsBytes := make([]byte, 0)
	for _, item := range page.Items {
		//item生成
		tupleOffset := item.Offset
		tupleLen := item.Len
		flag := item.Flag
		item := tupleOffset<<17 + flag<<15 + tupleLen
		itemBs := make([]byte, 4)
		binary.BigEndian.PutUint32(itemBs, uint32(item))
		//todo 优化
		//a := bits.Add()
		itemsBytes = append(itemsBytes, itemBs...)
	}

	//page头
	headerBytes := make([]byte, 24)
	pageLower := uint16(page.Lower)
	pageUpper := uint16(page.Upper)
	binary.BigEndian.PutUint16(headerBytes[2:4], pageLower)
	binary.BigEndian.PutUint16(headerBytes[4:6], pageUpper)

	//空白区
	blankLen := PageSize - len(headerBytes) - len(itemsBytes) - len(page.Content)
	blankBytes := make([]byte, blankLen)

	pageBytes := append(append(append(headerBytes, itemsBytes...), blankBytes...), page.Content...)
	return pageBytes
}

//page反序列化
func (page *Page) UnMarshal(bytes []byte, pageNum int, schemaName string, tableName string) {
	if len(bytes) != PageSize {
		log.Panic("page size大小出错：" + string(len(bytes)))
	}

	page.PageNum = pageNum
	page.SchemaName = schemaName
	page.TableName = tableName

	//page头
	headerBytes := bytes[0:24]
	pageLower := binary.BigEndian.Uint16(headerBytes[2:4])
	pageUpper := binary.BigEndian.Uint16(headerBytes[4:6])

	page.Lower = int(pageLower)
	page.Upper = int(pageUpper)

	//item区
	itemsBytes := bytes[24:pageLower]
	itemsNum := len(itemsBytes) / 4
	items := make([]*Item, 0)
	for i := 0; i < itemsNum; i++ {
		itemBs := itemsBytes[i*4 : (i+1)*4]
		itemInt := binary.BigEndian.Uint32(itemBs)
		tupleOffset := itemInt >> 17
		tupleLen := itemInt & 0x7FFF
		flag := itemInt & 0x18000 >> 15

		item := new(Item)
		item.Flag = int(flag)
		item.Offset = int(tupleOffset)
		item.Len = int(tupleLen)
		items = append(items, item)
	}
	page.Items = items

	//content区
	page.Content = bytes[pageUpper+1:]

}

func (page *Page) AddTupleBytes(tupleBytes []byte) (tupleNum int) {
	tupleLen := len(tupleBytes)

	page.Lower = page.Lower + 4
	page.Upper = page.Upper - tupleLen

	page.Content = append(tupleBytes, page.Content...)

	item := new(Item)
	item.Flag = 1
	item.Offset = PageSize - len(page.Content)
	item.Len = tupleLen
	page.Items = append(page.Items, item)

	return len(page.Items) - 1
}

func (page *Page) GetTupleBytes(tupleNum int) (tupleBytes []byte) {
	item := page.Items[tupleNum]
	if item.Flag == 1 {
		contentOffset := item.Offset - (PageSize - len(page.Content))
		tupleBytes := page.Content[contentOffset : contentOffset+item.Len]
		return tupleBytes
	} else {
		return
	}
}
