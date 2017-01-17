package ix

import (
	"kqdb/ix"
	"testing"
)

func test_CreateIndexFile(t *testing.T) {
	err := ix.CreateIndexFile("test1", "sch")
	t.Log(err)
}

func Test_CreateIndex(t *testing.T) {
	err := ix.CreateIndex("colname2", "table1", "sch")
	t.Log(err)
}
