package ix

import (
	"kqdb/ix"
	"testing"
)

func Test_CreateIndexFile(t *testing.T) {
	err := ix.CreateIndexFile("test1", "sch")
	t.Log(err)
}
