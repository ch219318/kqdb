package global

import "fmt"

//定义sql错误结构体
type SqlError struct {
	Msg string
}

func (error *SqlError) Error() string {
	return fmt.Sprintf("sql错误：%s", error.Msg)
}
