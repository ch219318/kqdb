package recordm

import (
	"container/list"
)

type buffer_table struct {
	page_list       *list.List
	dirty_page_list *list.List
}

//创建buffer pool数据结构
type table_name string

var Buffer_pool = make(map[table_name]buffer_table)

//插入databuffer

//databuffer过期与替换

//扩展大小
