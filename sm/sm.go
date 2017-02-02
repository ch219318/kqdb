package sm

import (
	"log"
	"strings"
	"time"
)

//定义列结构体
type table struct {
	name    string   //表名
	columns []column //列切片
}

//定义表结构体
type column struct {
	name       string   //列名
	dataType   DataType //列数据类型
	dataWidth  int      // 数据宽度
	isNull     bool     //非空与否
	isUnique   bool     // 是否唯一
	defaultVal string   // 默认值
	comment    string   //注释
}

//定义列数据类型枚举值
type DataType int

const (
	TypeInt DataType = 1 + iota
	TypeString
	TypeDate
)

//根据ddl语句生成表结构体
func GenTableByDdl(sql string) (table, error) {
	//sql语言校验
	genTable := new(table)
	genTable.name = ""
	return genTable, nil
}

//保存结构体到frm文件

//根据frm文件生成表结构体

//把sql语句转换为一个一个单词
func SqlToWords(sql string) (words []string, err error) {
	// runes := ([]rune)sql
	chars := strings.Split(sql, "")
	specialChars := "(),;"
	word := ""
	for _, char := range chars {
		if char == " " {
			words = append(words, word)
			word = ""
		} else if strings.Contains(specialChars, char) {
			words = append(words, word, char)
			// words = append(words, char)
			word = ""
		} else {
			word = word + char
		}
	}
	return
}
