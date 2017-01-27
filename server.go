package main

import (
	"flag"
	"log"
	"net"
	// "strconv"
)

var port = flag.String("p", "33455", "help message for flagname")

func main() {
	flag.Parse()
	var address string = ":" + *port
	ln, err := net.Listen("tcp", address)
	if err != nil {
		// handle error
	}
	log.Printf("kqdb服务端启动成功，监听端口为：%s\n", *port)
	for {
		conn, err := ln.Accept()
		if err != nil {
			// handle error
			continue
		}
		go handleConnection(conn)
	}
}

//处理连接
func handleConnection(conn net.Conn) {
	log.Printf("开始处理连接：%v\n", conn)
	for {
		slice := make([]byte, 1024)
		n, err := conn.Read(slice)
		if err != nil {
			log.Printf("异常信息为：%s\n", err.Error())
			break
		}
		s := string(slice[:n])
		log.Printf("接收字节数：%v,字符串：%s\n", n, s)
		result := handSql(s)
		conn.Write([]byte(result))
	}
	log.Printf("连接：%v关闭\n", conn)
}

//执行sql
func handSql(s string) (result string) {
	return "result"
}

// func createTable(tableName string, cols []column) {

// }
