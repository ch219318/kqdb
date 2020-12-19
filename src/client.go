package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
)

var host = flag.String("h", "127.0.0.1", "help message for flagname")
var serverPort = flag.String("p", "33455", "help message for flagname")

func main() {
	flag.Parse()
	log.Printf("host=%s,serverPort=%s\n", *host, *serverPort)
	address := *host + ":" + *serverPort
	conn, err := net.Dial("tcp", address)
	if err != nil {
		log.Println(err.Error())
		return
	}
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
	fmt.Printf("<<%s\n", string(slice[:n]))
	return err
}
