package main

import (
	"flag"
	"kqdb/src/sm"
	"log"
	"net"
	// "strconv"
	"fmt"
)

import (
	"github.com/xwb1989/sqlparser"
)

var port = flag.String("p", "33455", "help message for flagname")

type Config struct {
	Port     string
	BasePath string
}

func main() {
	init1()
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

func init1() {
	//toml, err := toml.LoadFile("db.toml")
	//config := new(Config)
	//t.Fetch(prefix)
	flag.Parse()
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
func handSql(sql string) (result string) {

	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		// Do something with the err
	}

	// Otherwise do something with stmt
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		_ = stmt
	case *sqlparser.Insert:
	}

	defer func() {
		if err := recover(); err != nil {
			result = "程序发生严重错误" + fmt.Sprintf("%v", err)
		}
	}()
	result = handDdlSql(sql)
	return result
}

func handDdlSql(input string) string {
	table, err := sm.GenTableByDdl(input)
	if err != nil {
		return err.Error()
	}
	err1 := sm.SaveTableToFile(table)
	if err1 != nil {
		return err1.Error()
	}
	return "op ok"
}

func handDmlSql(input string) string {
	return "result"
}
