package sm

import (
	"log"
	"strings"
	// "time"
	"fmt"
	"strconv"
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

//定义语法错误结构体
type grammerError struct {
	msg string
}

func (ge grammerError) Error() string {
	return fmt.Sprintf("语法错误--%s", ge.msg)
}

//根据ddl语句生成表结构体
func GenTableByDdl(sql string) (*table, error) {
	//todo sql语言校验
	words := SqlToWords(sql)
	genTable := new(table)
	genTable.name = words[2]
	indexs1 := index(words, "(")
	indexs2 := index(words, ",")
	indexs3 := index(words, ")")
	log.Println(indexs1)
	log.Println(indexs2)
	log.Println(indexs3)
	slice := make([]int, 0)
	slice = append(slice, indexs1[0])
	slice = append(slice, indexs2...)
	slice = append(slice, indexs3[len(indexs3)-1])
	log.Println(slice)
	colNumber := len(slice) - 1 //列数量
	columns := make([]column, colNumber)
	for i := 0; i < colNumber; i++ {
		startIndex := slice[i]
		endIndex := slice[i+1]
		colWords := words[startIndex+1 : endIndex]
		log.Println(colWords)
		col, err := genColumn(colWords)
		if err != nil {
			return genTable, err
		}
		columns[i] = col
	}
	log.Println(columns)
	genTable.columns = columns
	return genTable, nil
}

//保存结构体到frm文件

//根据frm文件生成表结构体

//把sql语句转换为一个一个单词
func SqlToWords(sql string) (words []string) {
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
	words = append(words, word)
	log.Printf("%v,%d\n", words, len(words))
	return
}

func index(words []string, word string) (indexs []int) {
	return index0(words, word, false)
}

func index0(words []string, word string, isIgnoreCase bool) (indexs []int) {
	if isIgnoreCase {
		for n, w := range words {
			if strings.ToLower(w) == strings.ToLower(word) {
				indexs = append(indexs, n)
			}
		}
	} else {
		for n, w := range words {
			if w == word {
				indexs = append(indexs, n)
			}
		}
	}
	return
}

//根据ddl词切片生成column
func genColumn(words []string) (column, error) {
	col := new(column)
	col.name = words[0]
	switch strings.ToLower(words[1]) {
	case "number":
		col.dataType = TypeInt
	case "varchar2":
		col.dataType = TypeString
	default:
		return *col, grammerError{"不支持字段:" + col.name + "的字段类型:" + words[1]}
	}
	width, err := strconv.ParseInt(words[3], 0, 64)
	if err != nil {
		return *col, err
	}
	col.dataWidth = int(width)
	indexs := index0(words, "NULL", true)
	switch len(indexs) {
	case 0:
		col.isNull = true
	case 1:
		col.isNull = false
	default:
		return *col, grammerError{"字段" + col.name + "格式有误"}
	}
	return *col, nil
}
