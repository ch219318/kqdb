package main

import (
	"fmt"
	"github.com/xwb1989/sqlparser"
	"log"
	"strconv"
)

func main() {

	/* 创建切片 */
	numbers := []int{0, 1, 2, 3, 4, 5, 6, 7, 8}
	printSlice(numbers)

	numbers1 := make([]int, 0, 5)

	printSlice(numbers1)

	log.Println("====")

	sql := "select * from Persons2 where  LastName = 'Bill1' and  ID_P1 >2-3*4+1 or (cc <0 and bb>0+2-1)"
	stmt, _ := sqlparser.ParseStrictDDL(sql)
	var where sqlparser.Expr
	var select1 *sqlparser.Select
	sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.Select:
			log.Println("select")
			where = node.Where.Expr
			select1 = node

		}
		return true, nil
	}, stmt)

	sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case sqlparser.SelectExprs:
			log.Println("SelectExprs", node)
		case sqlparser.TableExprs:
			log.Println("TableExprs", node)
		case *sqlparser.Where:
			log.Println("where", node)

		}
		return true, nil
	}, select1)

	sqlparser.Walk(visit1, where)

	leftInt, err := strconv.ParseInt("12", 10, 32)
	fmt.Println(err)
	fmt.Println(leftInt)

}

func visit1(node sqlparser.SQLNode) (kontinue bool, err error) {
	switch node := node.(type) {
	case *sqlparser.AndExpr:
		log.Println("AndExpr", node)
		_ = node
	case *sqlparser.OrExpr:
		log.Println("OrExpr", node)
	case *sqlparser.ComparisonExpr:
		log.Println("ComparisonExpr", node)
	case *sqlparser.BinaryExpr:
		log.Println("BinaryExpr", node)
	case *sqlparser.UnaryExpr:
		log.Println("UnaryExpr", node)
	}
	return true, nil
}

func printSlice(x []int) {
	fmt.Printf("len=%d cap=%d slice=%v\n", len(x), cap(x), x)
}
