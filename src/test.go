package main

import (
	"fmt"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
	"strconv"
)

func main() {

	/* 创建切片 */
	numbers := []int{0, 1, 2, 3, 4, 5, 6, 7, 8}
	printSlice(numbers)

	numbers1 := make([]int, 0, 5)
	printSlice(numbers1)
	printSlice(numbers[2:2])

	sqltypes.NewVarChar()

	fmt.Println("====")

}

func printSlice(x []int) {
	fmt.Printf("len=%d cap=%d slice=%v\n", len(x), cap(x), x)
}
