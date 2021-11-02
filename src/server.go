package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"kqdb/src/filem"
	"kqdb/src/global"
	"kqdb/src/sqlm"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
)

func init() {
	global.InitLog()
}

var port = flag.String("p", "33455", "监听端口")

type Config struct {
	Port     string
	BasePath string
}

func main() {
	initCfg()

	var address = ":" + *port
	ln, err := net.Listen("tcp", address)
	if err != nil {
		log.Panic(err)
	}
	log.Printf("服务端启动成功，监听端口为：%s\n", *port)

	//监听关闭信号
	ch := make(chan os.Signal)
	signal.Notify(ch, os.Interrupt, os.Kill, syscall.SIGUSR1, syscall.SIGUSR2)
	go func() {
		<-ch
		log.Println("获取到关闭命令。现在关闭中...")
		if err = ln.Close(); err != nil {
			log.Println(err)
		}
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println(err)
			break
		} else {
			log.Printf("Accepted connection to %v from %v", conn.LocalAddr(), conn.RemoteAddr())
			go handleConn(conn)
		}
	}

	//执行清理程序
	log.Println("退出前清理开始...")
	filem.CloseFilesMap()
	log.Println("退出前清理完成")

	log.Println("服务端成功退出")
}

func initCfg() {
	//toml, err := toml.LoadFile("db.toml")
	//config := new(Config)
	//t.Fetch(prefix)
	flag.Parse()
}

//处理连接
func handleConn(conn net.Conn) {
	connId := fmt.Sprintf("%v", conn)
	log.Println("开始处理连接：", connId)

	defer conn.Close()
	defer func() {
		if pnc := recover(); pnc != nil {
			log.Println("程序发生严重错误:" + fmt.Sprintf("%v", pnc))
			debug.PrintStack()
		}
	}()

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

	log.Println("连接结束：", connId)
}
