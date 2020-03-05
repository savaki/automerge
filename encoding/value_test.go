package encoding

import (
	"testing"

	"github.com/tj/assert"
)

func TestReadValue(t *testing.T) {
	t.Run("int64", func(t *testing.T) {
		want := int64(123)
		value := Int64Value(want)
		data, err := value.Append(nil)
		assert.Nil(t, err)

		got, err := ReadValue(value.RawType, data)
		assert.Nil(t, err)
		assert.Equal(t, want, got.Int)
	})

	t.Run("rune", func(t *testing.T) {
		want := 'a'
		value := RuneValue(want)
		data, err := value.Append(nil)
		assert.Nil(t, err)

		got, err := ReadValue(value.RawType, data)
		assert.Nil(t, err)
		assert.EqualValues(t, want, got.Int)
	})

	t.Run("copy rune", func(t *testing.T) {
		data := make([]byte, 10)
		want := 'a'
		value := RuneValue(want)
		value.Copy(data)

		got, err := ReadValue(value.RawType, data)
		assert.Nil(t, err)
		assert.EqualValues(t, want, got.Int)
	})

	t.Run("string", func(t *testing.T) {
		want := "abc"
		value := StringValue(want)
		data, err := value.Append(nil)
		assert.Nil(t, err)

		got, err := ReadValue(value.RawType, data)
		assert.Nil(t, err)
		assert.Equal(t, want, string(got.Bytes))
	})
}

func TestPropertyValue(t *testing.T) {
	k, v := int64(123), "abc"
	buffer := PropertyValue(k, []byte(v))
	gk, gv, err := DecodePropertyValue(buffer.Bytes)
	assert.Nil(t, err)
	assert.Equal(t, k, gk)
	assert.Equal(t, v, string(gv))
}

func TestRuneValue(t *testing.T) {
	value := RuneValue('好')
	assert.Equal(t, '好', rune(value.Int))
}
