package main

import (
	"fmt"
	"kqdb/src/filem"
	"log"
	"reflect"
)

func main() {

	/* 创建切片 */
	numbers := []int{0, 1, 2, 3, 4, 5, 6, 7, 8}
	printSlice(numbers)

	numbers1 := make([]int, 0, 5)

	printSlice(numbers1)

	log.Println("====")

	map1 := make(map[string]filem.Page)
	map1["aa"] = *new(filem.Page)
	b := map1["bb"]
	log.Println(b)
	log.Println(reflect.TypeOf(b))

}

func printSlice(x []int) {
	fmt.Printf("len=%d cap=%d slice=%v\n", len(x), cap(x), x)
}
