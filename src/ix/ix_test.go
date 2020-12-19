package ix

import (
	"testing"
)

func test_CreateIndexFile(t *testing.T) {
	err := CreateIndexFile("test1", "sch")
	t.Log(err)
}

func Test_CreateIndex(t *testing.T) {
	err := CreateIndex("colname2", "table1", "sch")
	t.Log(err)
}
