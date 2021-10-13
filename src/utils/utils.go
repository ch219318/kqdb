package utils

import (
	"encoding/binary"
)

func Int32ToBytes(i int32) []byte {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, uint32(i))
	return bytes
}

func BytesToInt32(bytes []byte) int32 {
	i := binary.BigEndian.Uint32(bytes)
	return int32(i)
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
