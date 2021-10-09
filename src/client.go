package main

import (
	"bufio"
	"flag"
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
		log.Fatal(err.Error())
	}
	defer conn.Close()
	log.Printf("连接为：%v\n", conn)

	for {
		log.Printf(">>")
		// var s string
		// fmt.Scanln(&s)如果输入有空格会被分成多段
		str, _ := bufio.NewReader(os.Stdin).ReadString('\n')
		if str == "quit\n" {
			break
		}
		sendRequest(str, conn)
		handResponse(conn)
	}
	log.Printf("Bye!")
}

//发送请求
func sendRequest(msg string, conn net.Conn) {
	writer := bufio.NewWriter(conn)

	_, wErr := writer.WriteString(msg)
	if wErr != nil {
		log.Fatal(wErr.Error())
	}
	writer.Flush()
	//log.Printf("写入字节数:%v\n", num)

	return
}

//处理响应
func handResponse(conn net.Conn) {
	reader := bufio.NewReader(conn)

	response, rErr := reader.ReadString('\n')
	if rErr != nil {
		log.Fatal(rErr.Error())
	}

	log.Printf("<< %s\n", response)
	return
}
