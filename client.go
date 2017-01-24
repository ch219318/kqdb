package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
)

func main() {
	conn, err := net.Dial("tcp", ":33455")
	defer conn.Close()
	log.Printf("连接为：%v\n", conn)
	if err != nil {
		log.Printf("异常信息为：%s\n", err.Error())
		return
	}
	for {
		fmt.Printf(">>")
		// var s string
		// fmt.Scanln(&s)如果输入有空格会被分成多段
		slice, _, _ := bufio.NewReader(os.Stdin).ReadLine()
		if string(slice) == "quit" {
			break
		}
		log.Printf("您所输入的字符为%s\n", string(slice))
		sendMsg(slice, conn)
	}
}

func sendMsg(bytes []byte, conn net.Conn) error {
	// status, err := bufio.NewReader(conn).ReadString('\n')
	stauts, err := conn.Write(bytes)
	log.Printf("写入字节数:%v\n", stauts)
	slice := make([]byte, 1024)
	n, _ := conn.Read(slice)
	log.Printf("接收字符串为：%s\n", string(slice[:n]))
	return err
}
