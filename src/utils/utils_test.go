package utils

import (
	"testing"
)

func Test_BytesToInt(t *testing.T) {
	bytes := [4]byte{1, 2, 3, 4}
	_, err := BytesToInt(bytes)
	t.Log(err)
}
