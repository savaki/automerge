// Copyright 2020 Matt Ho
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package encoding

import (
	"encoding/binary"
	"io"
	"testing"

	"github.com/tj/assert"
)

func TestRLE_InsertAt(t *testing.T) {
	var err error

	t.Run("insert head - first time", func(t *testing.T) {
		r := NewRLE(nil)

		err = r.InsertAt(0, 1)
		assert.Nil(t, err)

		values := readAllRLE(t, r.buffer)
		assert.EqualValues(t, []int64{1}, values)
	})

	t.Run("insert tail", func(t *testing.T) {
		r := NewRLE(nil)

		err = r.InsertAt(0, 1)
		assert.Nil(t, err)

		err = r.InsertAt(1, 2)
		assert.Nil(t, err)

		values := readAllRLE(t, r.buffer)
		assert.EqualValues(t, []int64{1, 2}, values)
	})

	t.Run("insert head - subsequent", func(t *testing.T) {
		r := NewRLE(nil)

		err = r.InsertAt(0, 1)
		assert.Nil(t, err)

		err = r.InsertAt(0, 2)
		assert.Nil(t, err)

		values := readAllRLE(t, r.buffer)
		assert.EqualValues(t, []int64{2, 1}, values)
	})

	t.Run("increment repeat - start of run", func(t *testing.T) {
		r := NewRLE(nil)

		err = r.InsertAt(0, 1)
		assert.Nil(t, err)
		want := len(r.buffer)

		err = r.InsertAt(0, 1)
		assert.Nil(t, err)
		got := len(r.buffer)
		assert.Equal(t, want, got) // buffer should not have increased as we just incremented repeat by 1

		values := readAllRLE(t, r.buffer)
		assert.EqualValues(t, []int64{1, 1}, values)
	})

	t.Run("increment repeat - end of run", func(t *testing.T) {
		r := NewRLE(nil)

		err = r.InsertAt(0, 1)
		assert.Nil(t, err)
		want := len(r.buffer)

		err = r.InsertAt(1, 1)
		assert.Nil(t, err)
		got := len(r.buffer)
		assert.Equal(t, want, got) // buffer should not have increased as we just incremented repeat by 1

		values := readAllRLE(t, r.buffer)
		assert.EqualValues(t, []int64{1, 1}, values)
	})

	t.Run("insert value in middle of sequence", func(t *testing.T) {
		r := NewRLE(nil)
		_ = r.InsertAt(0, 3)
		_ = r.InsertAt(0, 2)
		_ = r.InsertAt(0, 1)

		// When
		err := r.InsertAt(2, 4)
		assert.Nil(t, err)

		// Then
		values := readAllRLE(t, r.buffer)
		assert.EqualValues(t, []int64{1, 2, 4, 3}, values)
	})

	t.Run("increment repeat - over var int length boundary", func(t *testing.T) {
		r := NewRLE(nil)
		n := 256

		err = r.InsertAt(0, 1)
		assert.Nil(t, err)
		want := len(r.buffer)

		for i := 0; i < n; i++ {
			err = r.InsertAt(0, 1)
			assert.Nil(t, err)
		}

		got := len(r.buffer)
		assert.NotEqual(t, want, got)

		values := readAllRLE(t, r.buffer)
		assert.Len(t, values, n+1)
		for _, v := range values {
			assert.EqualValues(t, 1, v)
		}
	})

	t.Run("increment repeat - middle of run", func(t *testing.T) {
		r := NewRLE(nil)

		err = r.InsertAt(0, 1)
		assert.Nil(t, err)
		err = r.InsertAt(0, 1)
		assert.Nil(t, err)
		err = r.InsertAt(0, 1)
		assert.Nil(t, err)
		want := len(r.buffer)

		// When - insert in middle of run
		err = r.InsertAt(1, 1)
		assert.Nil(t, err)

		got := len(r.buffer)
		assert.Equal(t, want, got)

		values := readAllRLE(t, r.buffer)
		assert.EqualValues(t, []int64{1, 1, 1, 1}, values)
	})

	t.Run("break repeat - new value in middle of run", func(t *testing.T) {
		r := NewRLE(nil)

		err = r.InsertAt(0, 1)
		assert.Nil(t, err)
		err = r.InsertAt(0, 1)
		assert.Nil(t, err)
		err = r.InsertAt(0, 1)
		assert.Nil(t, err)

		// When - insert in middle of run
		err = r.InsertAt(1, 2)
		assert.Nil(t, err)

		values := readAllRLE(t, r.buffer)
		assert.EqualValues(t, []int64{1, 2, 1, 1}, values)
	})
}

func TestRLE_Next(t *testing.T) {
	r := NewRLE(nil)
	_ = r.InsertAt(0, 3)
	_ = r.InsertAt(0, 3)
	_ = r.InsertAt(0, 2)
	_ = r.InsertAt(0, 1)

	var got []int64
	var token RLEToken
	var err error
	for {
		token, err = r.Next(token)
		if err == io.EOF {
			break
		}
		assert.Nil(t, err)

		got = append(got, token.Value)
	}

	assert.EqualValues(t, []int64{1, 2, 3, 3}, got)
}

func TestRLE_DeleteAt(t *testing.T) {
	t.Run("delete single element", func(t *testing.T) {
		r := NewRLE(nil)
		_ = r.InsertAt(0, 1)

		// When
		err := r.DeleteAt(0)
		assert.Nil(t, err)
		assert.Len(t, r.buffer, 0)
	})

	t.Run("decrement repeat count", func(t *testing.T) {
		r := NewRLE(nil)
		_ = r.InsertAt(0, 1)
		_ = r.InsertAt(0, 1)

		// When
		err := r.DeleteAt(0)
		assert.Nil(t, err)

		// Then
		values := readAllRLE(t, r.buffer)
		assert.EqualValues(t, []int64{1}, values)
	})

	t.Run("delete across var int size boundary", func(t *testing.T) {
		r := NewRLE(nil)
		n := 256

		for i := 0; i < n; i++ {
			err := r.InsertAt(0, 1)
			assert.Nil(t, err)
		}

		for i := 0; i < n; i++ {
			err := r.DeleteAt(0)
			assert.Nil(t, err)
		}

		assert.Len(t, r.buffer, 0)
	})

	t.Run("delete middle element", func(t *testing.T) {
		r := NewRLE(nil)
		_ = r.InsertAt(0, 3)
		_ = r.InsertAt(0, 2)
		_ = r.InsertAt(0, 1)

		// When
		err := r.DeleteAt(1)
		assert.Nil(t, err)

		// Then
		values := readAllRLE(t, r.buffer)
		assert.EqualValues(t, []int64{1, 3}, values)
	})

	t.Run("index below 0", func(t *testing.T) {
		r := NewRLE(nil)
		err := r.DeleteAt(-1)
		assert.Equal(t, io.ErrUnexpectedEOF, err)
	})

	t.Run("index out of bounds", func(t *testing.T) {
		r := NewRLE(nil)
		err := r.DeleteAt(2)
		assert.Equal(t, io.ErrUnexpectedEOF, err)
	})
}

func TestRLE_SplitAt(t *testing.T) {
	makeRLE := func() *RLE {
		base := NewRLE(nil)
		_ = base.InsertAt(0, 2)
		_ = base.InsertAt(0, 2)
		_ = base.InsertAt(0, 2)
		_ = base.InsertAt(0, 1)
		_ = base.InsertAt(0, 1)
		_ = base.InsertAt(0, 1)
		return base
	}

	t.Run("split on boundary", func(t *testing.T) {
		base := makeRLE()
		left, right, err := base.SplitAt(3)
		assert.Nil(t, err)

		l := readAllRLE(t, left.buffer)
		assert.Equal(t, []int64{1, 1, 1}, l)

		r := readAllRLE(t, right.buffer)
		assert.Equal(t, []int64{2, 2, 2}, r)
	})

	t.Run("split in middle", func(t *testing.T) {
		base := makeRLE()
		left, right, err := base.SplitAt(2)
		assert.Nil(t, err)

		l := readAllRLE(t, left.buffer)
		assert.Equal(t, []int64{1, 1}, l)

		r := readAllRLE(t, right.buffer)
		assert.Equal(t, []int64{1, 2, 2, 2}, r)
	})

	t.Run("split head", func(t *testing.T) {
		base := makeRLE()
		left, right, err := base.SplitAt(0)
		assert.Nil(t, err)

		l := readAllRLE(t, left.buffer)
		assert.Equal(t, []int64(nil), l)

		r := readAllRLE(t, right.buffer)
		assert.Equal(t, []int64{1, 1, 1, 2, 2, 2}, r)
	})

	t.Run("split tail", func(t *testing.T) {
		base := makeRLE()
		left, right, err := base.SplitAt(6)
		assert.Nil(t, err)

		l := readAllRLE(t, left.buffer)
		assert.Equal(t, []int64{1, 1, 1, 2, 2, 2}, l)

		r := readAllRLE(t, right.buffer)
		assert.Equal(t, []int64(nil), r)
	})
}

func BenchmarkRLE_Next(t *testing.B) {
	const n = 1e3

	r := NewRLE(nil)
	for i := int64(0); i < n; i++ {
		_ = r.InsertAt(i, i)
	}

	for i := 0; i < t.N; i++ {
		var token RLEToken
		var err error
		for {
			token, err = r.Next(token)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("got %v; want nil", err)
			}
		}
	}
}

func readAllRLE(t *testing.T, buffer []byte) []int64 {
	var pos int
	var values []int64
	if len(buffer) > 0 {
		for {
			repeat, length := binary.Varint(buffer[pos:])
			pos += length

			value, length := binary.Varint(buffer[pos:])
			pos += length

			for i := int64(0); i < repeat; i++ {
				values = append(values, value)
			}

			if pos == len(buffer) {
				break
			}
		}
	}
	return values
}
