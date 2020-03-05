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
	"io"
	"testing"

	"github.com/tj/assert"
)

func TestPlain_InsertAt(t *testing.T) {
	t.Run("insert empty", func(t *testing.T) {
		p := NewPlain(RawTypeByteArray, nil)

		want := "abc"
		err := p.InsertAt(0, StringValue(want))
		assert.Nil(t, err)

		got := readAllValues(t, p)
		assert.Len(t, got, 1)
		assert.Equal(t, want, string(got[0].Bytes))
	})

	t.Run("append", func(t *testing.T) {
		p := NewPlain(RawTypeByteArray, nil)
		a := "abc"
		b := "def"

		err := p.InsertAt(0, StringValue(a))
		assert.Nil(t, err)

		err = p.InsertAt(1, StringValue(b))
		assert.Nil(t, err)

		got := readAllValues(t, p)
		assert.Len(t, got, 2)
		assert.Equal(t, a, string(got[0].Bytes))
		assert.Equal(t, b, string(got[1].Bytes))
	})

	t.Run("prepend", func(t *testing.T) {
		p := NewPlain(RawTypeByteArray, nil)
		a := "abc"
		b := "def"

		err := p.InsertAt(0, StringValue(a))
		assert.Nil(t, err)

		err = p.InsertAt(0, StringValue(b))
		assert.Nil(t, err)

		got := readAllValues(t, p)
		assert.Len(t, got, 2)
		assert.Equal(t, b, string(got[0].Bytes))
		assert.Equal(t, a, string(got[1].Bytes))
	})
}

func TestPlain_InsertAtVarInt(t *testing.T) {
	t.Run("insert empty", func(t *testing.T) {
		p := NewPlain(RawTypeVarInt, nil)

		want := '你'
		err := p.InsertAt(0, RuneValue('你'))
		assert.Nil(t, err)

		got := readAllValues(t, p)
		assert.Len(t, got, 1)
		assert.Equal(t, want, rune(got[0].Int))
	})

	t.Run("append - single byte", func(t *testing.T) {
		p := NewPlain(RawTypeVarInt, nil)
		a := 'a'
		b := 'b'

		err := p.InsertAt(0, RuneValue(a))
		assert.Nil(t, err)

		err = p.InsertAt(1, RuneValue(b))
		assert.Nil(t, err)

		got := readAllValues(t, p)
		assert.Len(t, got, 2)
		assert.Equal(t, a, rune(got[0].Int))
		assert.Equal(t, b, rune(got[1].Int))
	})

	t.Run("append - multi byte", func(t *testing.T) {
		p := NewPlain(RawTypeVarInt, nil)
		a := '你'
		b := '好'

		err := p.InsertAt(0, RuneValue(a))
		assert.Nil(t, err)

		err = p.InsertAt(1, RuneValue(b))
		assert.Nil(t, err)

		got := readAllValues(t, p)
		assert.Len(t, got, 2)
		assert.Equal(t, a, rune(got[0].Int))
		assert.Equal(t, b, rune(got[1].Int))
	})

	t.Run("prepend - single byte", func(t *testing.T) {
		p := NewPlain(RawTypeVarInt, nil)
		a := 'a'
		b := 'b'

		err := p.InsertAt(0, RuneValue(a))
		assert.Nil(t, err)

		err = p.InsertAt(0, RuneValue(b))
		assert.Nil(t, err)

		got := readAllValues(t, p)
		assert.Len(t, got, 2)
		assert.Equal(t, b, rune(got[0].Int))
		assert.Equal(t, a, rune(got[1].Int))
	})

	t.Run("prepend - multi byte", func(t *testing.T) {
		p := NewPlain(RawTypeVarInt, nil)
		a := '你'
		b := '好'

		err := p.InsertAt(0, RuneValue(a))
		assert.Nil(t, err)

		err = p.InsertAt(0, RuneValue(b))
		assert.Nil(t, err)

		got := readAllValues(t, p)
		assert.Len(t, got, 2)
		assert.Equal(t, b, rune(got[0].Int))
		assert.Equal(t, a, rune(got[1].Int))
	})
}

var got Value

func BenchmarkPlain_Next(t *testing.B) {
	const n = 1e3
	const v = 123

	p := NewPlain(RawTypeVarInt, nil)
	for i := 0; i < n; i++ {
		err := p.InsertAt(0, Int64Value(v))
		assert.Nil(t, err)
	}

	for i := 0; i < t.N; i++ {
		var err error
		var token PlainToken

		for {
			token, err = p.Next(token)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("got %v; want nil", err)
			}

			got = token.Value
		}
	}
}

func TestPlain_SplitAt(t *testing.T) {
	makeItem := func() *Plain {
		base := NewPlain(RawTypeVarInt, nil)
		_ = base.InsertAt(0, Int64Value(3))
		_ = base.InsertAt(0, Int64Value(2))
		_ = base.InsertAt(0, Int64Value(1))
		return base
	}

	t.Run("split in middle", func(t *testing.T) {
		base := makeItem()
		left, right, err := base.SplitAt(1)
		assert.Nil(t, err)

		l := readAllValues(t, left)
		assert.Len(t, l, 1)
		assert.EqualValues(t, 1, l[0].Int)

		r := readAllValues(t, right)
		assert.Len(t, r, 2)
		assert.EqualValues(t, 2, r[0].Int)
		assert.EqualValues(t, 3, r[1].Int)
	})

	t.Run("split head", func(t *testing.T) {
		base := makeItem()
		left, right, err := base.SplitAt(0)
		assert.Nil(t, err)

		l := readAllValues(t, left)
		assert.Len(t, l, 0)

		r := readAllValues(t, right)
		assert.Len(t, r, 3)
		assert.EqualValues(t, 1, r[0].Int)
		assert.EqualValues(t, 2, r[1].Int)
		assert.EqualValues(t, 3, r[2].Int)
	})

	t.Run("split tail", func(t *testing.T) {
		base := makeItem()
		left, right, err := base.SplitAt(3)
		assert.Nil(t, err)

		l := readAllValues(t, left)
		assert.Len(t, l, 3)
		assert.EqualValues(t, 1, l[0].Int)
		assert.EqualValues(t, 2, l[1].Int)
		assert.EqualValues(t, 3, l[2].Int)

		r := readAllValues(t, right)
		assert.Len(t, r, 0)
	})
}

func readAllValues(t *testing.T, p *Plain) []Value {
	var err error
	var got []Value
	var token PlainToken
	for {
		token, err = p.Next(token)
		if err == io.EOF {
			break
		}
		assert.Nil(t, err)
		assert.Equal(t, p.rawType, token.Value.RawType)

		got = append(got, token.Value)
	}
	return got
}
