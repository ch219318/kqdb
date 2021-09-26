package recordm

import (
	"os"
	"testing"
)

func Test_CreateDataFile(t *testing.T) {
	file1, _ := os.Open("界面要求.docx")
	file2, _ := os.Open("界面要求.docx")
	t.Log(file1)
	t.Log(file2)
}
