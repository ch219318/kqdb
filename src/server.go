package main

import (
	"bufio"
	"flag"
	"io"
	"kqdb/src/global"
	"kqdb/src/sqlm"
	"log"
	"net"
)

func init() {
	global.InitLog()
}

var port = flag.String("p", "33455", "help message for flagname")

type Config struct {
	Port     string
	BasePath string
}

func main() {
	initCfg()

	var address = ":" + *port
	ln, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("服务端启动成功，监听端口为：%s\n", *port)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		log.Printf("Accepted connection to %v from %v", conn.LocalAddr(), conn.RemoteAddr())
		go handleConn(conn)
	}
}

func initCfg() {
	//toml, err := toml.LoadFile("db.toml")
	//config := new(Config)
	//t.Fetch(prefix)
	flag.Parse()
}

//处理连接
func handleConn(conn net.Conn) {
	log.Printf("开始处理连接：%v\n", conn)
	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		//接受请求
		request, rErr := reader.ReadString('\n')
		if rErr == io.EOF {
			log.Printf("异常信息为：EOF\n")
			break
		}
		if rErr != nil {
			log.Printf("异常信息为：%s\n", rErr.Error())
			break
		}
		log.Printf("接收的字符串：%s\n", request)

		//处理请求
		sqlStr := request[:len(request)-1]
		result := sqlm.HandSql(sqlStr)

		//返回响应
		_, wErr := writer.WriteString(result + "\n")
		if wErr != nil {
			log.Println(wErr.Error())
			break
		}
		writer.Flush()
	}

	log.Printf("连接结束：%v\n", conn)
}
