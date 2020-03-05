package encoding

import (
	"io"
	"testing"

	"github.com/tj/assert"
)

func TestDictionaryRLE_InsertAt(t *testing.T) {
	d := NewDictionaryRLE(nil, nil)

	a, b := []byte("hello"), []byte("world")

	err := d.InsertAt(0, a)
	assert.Nil(t, err)

	err = d.InsertAt(1, b)
	assert.Nil(t, err)

	v, err := d.Lookup(a)
	assert.Nil(t, err)
	assert.EqualValues(t, 0, v)

	v, err = d.Lookup(b)
	assert.Nil(t, err)
	assert.EqualValues(t, 1, v)

	got := readAllDictionary(t, d)
	assert.Len(t, got, 2)
	assert.Equal(t, string(a), string(got[0]))
	assert.Equal(t, string(b), string(got[1]))
}

func TestDictionaryRLE_SplitAt(t *testing.T) {
	t.Run("middle", func(t *testing.T) {
		d := NewDictionaryRLE(nil, nil)
		a, b, c := []byte("a"), []byte("b"), []byte("c")
		err := d.InsertAt(0, a)
		assert.Nil(t, err)
		err = d.InsertAt(1, b)
		assert.Nil(t, err)
		err = d.InsertAt(2, c)
		assert.Nil(t, err)

		left, right, err := d.SplitAt(2)
		assert.Nil(t, err)

		got := readAllDictionary(t, left)
		assert.Len(t, got, 2)
		assert.Equal(t, a, got[0])
		assert.Equal(t, b, got[1])

		got = readAllDictionary(t, right)
		assert.Len(t, got, 1)
		assert.Equal(t, c, got[0])
	})
}

func readAllDictionary(t *testing.T, d *DictionaryRLE) [][]byte {
	var got [][]byte
	var token DictionaryRLEToken
	var err error
	for {
		token, err = d.Next(token)
		if err == io.EOF {
			break
		}
		assert.Nil(t, err)

		got = append(got, token.Value)
	}
	return got
}
