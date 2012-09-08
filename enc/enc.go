// enc includes common encoding utils
package enc

import (
	"unicode/utf8"
)

// encode int as string - convert character code
func EncChar(i int) string {
	return string(EncByte(i)[0])
}

// encode int as byte representation - convert character code
func EncByte(i int) []byte {
	b := make([]byte, 1)
	utf8.EncodeRune(b, rune(i))
	return b
}
