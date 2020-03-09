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
)

var got Value

func BenchmarkPlain_Next(t *testing.B) {
	const n = 1e3
	const v = 123

	p := NewPlain(RawTypeVarInt, nil)
	for i := 0; i < n; i++ {
		err := p.InsertAt(0, Int64Value(v))
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

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

func TestPlain_InsertAt(t *testing.T) {
	t.Run("insert empty", func(t *testing.T) {
		p := NewPlain(RawTypeByteArray, nil)

		want := "abc"
		err := p.InsertAt(0, StringValue(want))
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		got := readAllValues(t, p)
		if got, want := got, 1; len(got) != want {
			t.Fatalf("got %v; want %v", len(got), want)
		}
		if want, got := want, string(got[0].Bytes); got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("append", func(t *testing.T) {
		p := NewPlain(RawTypeByteArray, nil)
		a := "abc"
		b := "def"

		err := p.InsertAt(0, StringValue(a))
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		err = p.InsertAt(1, StringValue(b))
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		got := readAllValues(t, p)
		if got, want := got, 2; len(got) != want {
			t.Fatalf("got %v; want %v", len(got), want)
		}
		if want, got := a, string(got[0].Bytes); got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
		if want, got := b, string(got[1].Bytes); got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("prepend", func(t *testing.T) {
		p := NewPlain(RawTypeByteArray, nil)
		a := "abc"
		b := "def"

		err := p.InsertAt(0, StringValue(a))
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		err = p.InsertAt(0, StringValue(b))
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		got := readAllValues(t, p)
		if got, want := got, 2; len(got) != want {
			t.Fatalf("got %v; want %v", len(got), want)
		}
		if want, got := b, string(got[0].Bytes); got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
		if want, got := a, string(got[1].Bytes); got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
	})
}

func TestPlain_InsertAtVarInt(t *testing.T) {
	t.Run("insert empty", func(t *testing.T) {
		p := NewPlain(RawTypeVarInt, nil)

		want := '你'
		err := p.InsertAt(0, RuneValue('你'))
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		got := readAllValues(t, p)
		if got, want := got, 1; len(got) != want {
			t.Fatalf("got %v; want %v", len(got), want)
		}
		if want, got := want, rune(got[0].Int); got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("append - single byte", func(t *testing.T) {
		p := NewPlain(RawTypeVarInt, nil)
		a := 'a'
		b := 'b'

		err := p.InsertAt(0, RuneValue(a))
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		err = p.InsertAt(1, RuneValue(b))
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		got := readAllValues(t, p)
		if got, want := got, 2; len(got) != want {
			t.Fatalf("got %v; want %v", len(got), want)
		}
		if want, got := a, rune(got[0].Int); got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
		if want, got := b, rune(got[1].Int); got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("append - multi byte", func(t *testing.T) {
		p := NewPlain(RawTypeVarInt, nil)
		a := '你'
		b := '好'

		err := p.InsertAt(0, RuneValue(a))
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		err = p.InsertAt(1, RuneValue(b))
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		got := readAllValues(t, p)
		if got, want := got, 2; len(got) != want {
			t.Fatalf("got %v; want %v", len(got), want)
		}
		if want, got := a, rune(got[0].Int); got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
		if want, got := b, rune(got[1].Int); got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("prepend - single byte", func(t *testing.T) {
		p := NewPlain(RawTypeVarInt, nil)
		a := 'a'
		b := 'b'

		err := p.InsertAt(0, RuneValue(a))
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		err = p.InsertAt(0, RuneValue(b))
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		got := readAllValues(t, p)
		if got, want := got, 2; len(got) != want {
			t.Fatalf("got %v; want %v", len(got), want)
		}
		if want, got := b, rune(got[0].Int); got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
		if want, got := a, rune(got[1].Int); got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("prepend - multi byte", func(t *testing.T) {
		p := NewPlain(RawTypeVarInt, nil)
		a := '你'
		b := '好'

		err := p.InsertAt(0, RuneValue(a))
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		err = p.InsertAt(0, RuneValue(b))
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		got := readAllValues(t, p)
		if got, want := got, 2; len(got) != want {
			t.Fatalf("got %v; want %v", len(got), want)
		}
		if want, got := b, rune(got[0].Int); got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
		if want, got := a, rune(got[1].Int); got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
	})
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
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		l := readAllValues(t, left)
		if got, want := l, 1; len(got) != want {
			t.Fatalf("got %v; want %v", len(got), want)
		}
		if want, got := int64(1), l[0].Int; want != got {
			t.Fatalf("got %v, want %v", got, want)
		}

		r := readAllValues(t, right)
		if got, want := r, 2; len(got) != want {
			t.Fatalf("got %v; want %v", len(got), want)
		}
		if want, got := int64(2), r[0].Int; want != got {
			t.Fatalf("got %v, want %v", got, want)
		}
		if want, got := int64(3), r[1].Int; want != got {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("split head", func(t *testing.T) {
		base := makeItem()
		left, right, err := base.SplitAt(0)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		l := readAllValues(t, left)
		if got, want := l, 0; len(got) != want {
			t.Fatalf("got %v; want %v", len(got), want)
		}

		r := readAllValues(t, right)
		if got, want := r, 3; len(got) != want {
			t.Fatalf("got %v; want %v", len(got), want)
		}
		if want, got := int64(1), r[0].Int; want != got {
			t.Fatalf("got %v, want %v", got, want)
		}
		if want, got := int64(2), r[1].Int; want != got {
			t.Fatalf("got %v, want %v", got, want)
		}
		if want, got := int64(3), r[2].Int; want != got {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("split tail", func(t *testing.T) {
		base := makeItem()
		left, right, err := base.SplitAt(3)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		l := readAllValues(t, left)
		if got, want := l, 3; len(got) != want {
			t.Fatalf("got %v; want %v", len(got), want)
		}
		if want, got := int64(1), l[0].Int; want != got {
			t.Fatalf("got %v, want %v", got, want)
		}
		if want, got := int64(2), l[1].Int; want != got {
			t.Fatalf("got %v, want %v", got, want)
		}
		if want, got := int64(3), l[2].Int; want != got {
			t.Fatalf("got %v, want %v", got, want)
		}

		r := readAllValues(t, right)
		if got, want := r, 0; len(got) != want {
			t.Fatalf("got %v; want %v", len(got), want)
		}
	})
}

func TestPlain_SizeRowCount(t *testing.T) {
	makeItem := func() *Plain {
		base := NewPlain(RawTypeVarInt, nil)
		_ = base.InsertAt(0, Int64Value('你'))
		_ = base.InsertAt(0, Int64Value(3))
		_ = base.InsertAt(0, Int64Value(2))
		_ = base.InsertAt(0, Int64Value(1))
		return base
	}

	t.Run("size", func(t *testing.T) {
		base := makeItem()
		if got, want := base.Size(), 6; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
	})

	t.Run("row count", func(t *testing.T) {
		base := makeItem()
		if got, want := base.RowCount(), 4; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
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
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		if want, got := p.rawType, token.Value.RawType; got != want {
			t.Fatalf("got %v, want %v", got, want)
		}

		got = append(got, token.Value)
	}
	return got
}
