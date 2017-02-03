package sm

import (
	"kqdb/sm"
	"strings"
	"testing"
)

func test_test(t *testing.T) {
	numbers := []int{0, 1, 2, 3, 4, 5, 6, 7, 8}
	num1 := numbers[0:5]
	num1[2] = 33333
	t.Log(num1)
	t.Log(numbers)
	t.Log(strings.ToLower("(),"))
}

func test_SqlToWords(t *testing.T) {
	sql := "CREATE TABLE region(" +
		"ID number(2) NOT NULL PRIMARY KEY," +
		"postcode number(6) default '0' NOT NULL," +
		"areaname varchar2(30) default ' ' NOT NULL);"
	sm.SqlToWords(sql)
}

func Test_GenTableByDdl(t *testing.T) {
	sql := "CREATE TABLE region(" +
		"ID number(2) NOT nULL PRIMARY KEY," +
		"postcode number(6) default '0' NOT NULL," +
		"areaname varchar2(30) default ' ' NOT NULL);"
	_, err := sm.GenTableByDdl(sql)
	t.Log(err)
}
