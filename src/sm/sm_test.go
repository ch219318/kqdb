package sm

import (
	"testing"
)

func test_test(t *testing.T) {

}

func test_SqlToWords(t *testing.T) {
	sql := "CREATE TABLE region(" +
		"ID number(2) NOT NULL PRIMARY KEY," +
		"postcode number(6) default '0' NOT NULL," +
		"areaname varchar2(30) default ' ' NOT NULL);"
	SqlToWords(sql)
}

func Test_GenTableByDdl(t *testing.T) {
	sql := "CREATE TABLE region(" +
		"ID number(2) NOT nULL PRIMARY KEY," +
		"postcode number(6) default '0' NOT NULL," +
		"areaname varchar2(30) default '' NOT NULL comment 'aadd');"
	table, err := GenTableByDdl(sql)
	t.Log(table)
	t.Log(err)
	SaveTableToFile(table)
}

func test_SaveTableToFile(t *testing.T) {
}
