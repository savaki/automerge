package encoding

import (
	"testing"
	"unicode/utf8"

	"github.com/tj/assert"
)

func TestEquals(t *testing.T) {
	assert.EqualValues(t, 4, lengthUint32)
}

func TestEncodeVarInt(t *testing.T) {
	assert.Equal(t, 3, utf8.RuneLen('你'))
}
