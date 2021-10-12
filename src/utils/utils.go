package utils

import (
	"encoding/binary"
)

func IntToBytes(i int) (bytes [4]byte, err error) {
	if i > 0 {
		bytes1 := make([]byte, 4)
		binary.BigEndian.PutUint32(bytes1, uint32(i))
	}
	return bytes, err
}

func BytesToInt(Bytes [4]byte) (i int, err error) {
	i = -2232
	println(uint32(i)) //unit32强转int，负数int＝＝》补码＝＝》unit
	return i, err
}

func testEq(a, b []byte) bool {
	// If one is nil, the other must also be nil.
	if (a == nil) != (b == nil) {
		return false
	}

	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
