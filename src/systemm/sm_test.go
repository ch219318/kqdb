package systemm

import (
	"testing"
)

func test_test(t *testing.T) {

}

func test_SqlToWords(t *testing.T) {
	_ = "CREATE TABLE region(" +
		"ID number(2) NOT NULL PRIMARY KEY," +
		"postcode number(6) default '0' NOT NULL," +
		"areaname varchar2(30) default ' ' NOT NULL);"
}

func Test_GenTableByDdl(t *testing.T) {
	_ = "CREATE TABLE region(" +
		"ID number(2) NOT nULL PRIMARY KEY," +
		"postcode number(6) default '0' NOT NULL," +
		"areaname varchar2(30) default '' NOT NULL comment 'aadd');"
}

func test_SaveTableToFile(t *testing.T) {
}
