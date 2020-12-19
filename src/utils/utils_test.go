package utils

import (
	"kqdb/utils"
	"testing"
)

func Test_BytesToInt(t *testing.T) {
	bytes := [4]byte{1, 2, 3, 4}
	_, err := utils.BytesToInt(bytes)
	t.Log(err)
}
