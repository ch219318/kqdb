package sm

import (
	// "kqdb/sm"
	"testing"
)

type teststruc struct {
	tt string
	aa int
}

func Test_test(t *testing.T) {
	// var i int = 32
	words := make([]string, 2)
	words = append(words, "hello")
	t.Logf("%v", words)
	t.Log(words)
}
