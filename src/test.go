package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func main() {

	/* 创建切片 */
	numbers := []int{0, 1, 2, 3, 4, 5, 6, 7, 8}
	printSlice(numbers)

	/* 打印原始切片 */
	fmt.Println("numbers ==", numbers)

	/* 打印子切片从索引1(包含) 到索引4(不包含)*/
	fmt.Println("numbers[1:4] ==", numbers[1:4])

	/* 默认下限为 0*/
	fmt.Println("numbers[:3] ==", numbers[:3])

	/* 默认上限为 len(s)*/
	fmt.Println("numbers[4:] ==", numbers[0])

	numbers1 := make([]int, 0, 5)
	printSlice(numbers1)

	/* 打印子切片从索引  0(包含) 到索引 2(不包含) */
	number2 := numbers[:2]
	printSlice(number2)

	/* 打印子切片从索引 2(包含) 到索引 5(不包含) */
	number3 := numbers[2:5]
	printSlice(number3)

	str, _ := os.Getwd()
	fmt.Println("wd:%s", str)

	path, err := os.Executable()
	if err != nil {
		panic(err)
	}
	dir := filepath.Dir(path)
	dir1 := filepath.Join(dir, "data11", "dd.cc")
	fmt.Println(path)
	fmt.Println(dir)
	fmt.Println(dir1)

	fmt.Println("====")

	fmt.Println(strings.Trim("frmaa.frm", "frm"))
	fmt.Println(strings.TrimRight("frmaa.frm", "frm"))
	fmt.Println(strings.TrimSuffix("frmaa.frm", "frm"))

	ticker := time.NewTicker(time.Second * 3)
	i := 0
	go func() {
		for { //循环
			a := <-ticker.C
			fmt.Println(a)
			i++
			fmt.Println("i = ", i)

			if i == 5 {
				ticker.Stop() //停止定时器
			}
		}
	}() //别忘了()

	//死循环，特地不让main goroutine结束
	for {
		fmt.Print("")
	}
}

func printSlice(x []int) {
	fmt.Printf("len=%d cap=%d slice=%v\n", len(x), cap(x), x)
}
