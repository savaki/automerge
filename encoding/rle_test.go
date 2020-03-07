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
	"errors"
	"io"
	"reflect"
	"testing"
)

func TestRLE_InsertAt(t *testing.T) {
	var err error

	t.Run("insert head - first time", func(t *testing.T) {
		r := NewRLE(nil)

		err = r.InsertAt(0, 1)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		values := readAllRLE(r.buffer)
		if want, got := []int64{1}, values; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("insert tail", func(t *testing.T) {
		r := NewRLE(nil)

		err = r.InsertAt(0, 1)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		err = r.InsertAt(1, 2)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		values := readAllRLE(r.buffer)
		if got, want := values, []int64{1, 2}; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v; want %v", len(got), want)
		}
	})

	t.Run("insert head - subsequent", func(t *testing.T) {
		r := NewRLE(nil)

		err = r.InsertAt(0, 1)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		err = r.InsertAt(0, 2)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		values := readAllRLE(r.buffer)
		if got, want := values, []int64{2, 1}; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v; want %v", len(got), want)
		}
	})

	t.Run("increment repeat - start of run", func(t *testing.T) {
		r := NewRLE(nil)

		err = r.InsertAt(0, 1)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		want := len(r.buffer)

		err = r.InsertAt(0, 1)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		got := len(r.buffer)
		if want, got := want, got; got != want {
			t.Fatalf("got %v, want %v", got, want)
		} // buffer should not have increased as we just incremented repeat by 1

		values := readAllRLE(r.buffer)
		if got, want := values, []int64{1, 1}; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v; want %v", len(got), want)
		}
	})

	t.Run("increment repeat - end of run", func(t *testing.T) {
		r := NewRLE(nil)

		err = r.InsertAt(0, 1)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		want := len(r.buffer)

		err = r.InsertAt(1, 1)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		got := len(r.buffer)
		if want, got := want, got; got != want {
			t.Fatalf("got %v, want %v", got, want)
		} // buffer should not have increased as we just incremented repeat by 1

		values := readAllRLE(r.buffer)
		if got, want := values, []int64{1, 1}; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v; want %v", len(got), want)
		}
	})

	t.Run("insert value in middle of sequence", func(t *testing.T) {
		r := NewRLE(nil)
		_ = r.InsertAt(0, 3)
		_ = r.InsertAt(0, 2)
		_ = r.InsertAt(0, 1)

		// When
		err := r.InsertAt(2, 4)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		// Then
		values := readAllRLE(r.buffer)
		if got, want := values, []int64{1, 2, 4, 3}; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v; want %v", len(got), want)
		}
	})

	t.Run("increment repeat - over var int length boundary", func(t *testing.T) {
		r := NewRLE(nil)
		n := 256

		err = r.InsertAt(0, 1)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		want := len(r.buffer)

		for i := 0; i < n; i++ {
			err = r.InsertAt(0, 1)
			if err != nil {
				t.Fatalf("got %v; want nil", err)
			}

		}

		got := len(r.buffer)
		if got == want {
			t.Fatalf("got %v; want not %v", got, want)
		}

		values := readAllRLE(r.buffer)
		if got, want := values, n+1; len(got) != want {
			t.Fatalf("got %v; want %v", len(got), want)
		}
		for _, v := range values {
			if want, got := int64(1), v; want != got {
				t.Fatalf("got %v, want %v", got, want)
			}
		}
	})

	t.Run("increment repeat - middle of run", func(t *testing.T) {
		r := NewRLE(nil)

		err = r.InsertAt(0, 1)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		err = r.InsertAt(0, 1)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		err = r.InsertAt(0, 1)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		want := len(r.buffer)

		// When - insert in middle of run
		err = r.InsertAt(1, 1)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		got := len(r.buffer)
		if want, got := want, got; got != want {
			t.Fatalf("got %v, want %v", got, want)
		}

		values := readAllRLE(r.buffer)
		if got, want := values, []int64{1, 1, 1, 1}; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v; want %v", len(got), want)
		}
	})

	t.Run("break repeat - new value in middle of run", func(t *testing.T) {
		r := NewRLE(nil)

		err = r.InsertAt(0, 1)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		err = r.InsertAt(0, 1)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		err = r.InsertAt(0, 1)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		// When - insert in middle of run
		err = r.InsertAt(1, 2)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		values := readAllRLE(r.buffer)
		if got, want := values, []int64{1, 2, 1, 1}; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v; want %v", len(got), want)
		}
	})

	t.Run("insert at end", func(t *testing.T) {
		r := NewRLE([]byte{122, 2})
		if got, want := MustInt64(r.Int64()), 61; len(got) != want {
			t.Fatalf("got %v; want %v", len(got), want)
		}

		err := r.InsertAt(60, 61)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		got := MustInt64(r.Int64())
		if got, want := got, 62; len(got) != want {
			t.Fatalf("got %v; want %v", len(got), want)
		}
		if got, want := got[len(got)-4:], []int64{1, 1, 61, 1}; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v; want %v", len(got), want)
		}
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
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		got = append(got, token.Value)
	}

	if got, want := got, []int64{1, 2, 3, 3}; !reflect.DeepEqual(want, got) {
		t.Fatalf("got %v; want %v", len(got), want)
	}
}

func TestRLE_DeleteAt(t *testing.T) {
	t.Run("delete single element", func(t *testing.T) {
		r := NewRLE(nil)
		_ = r.InsertAt(0, 1)

		// When
		err := r.DeleteAt(0)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		if got, want := r.buffer, 0; len(got) != want {
			t.Fatalf("got %v; want %v", len(got), want)
		}
	})

	t.Run("decrement repeat count", func(t *testing.T) {
		r := NewRLE(nil)
		_ = r.InsertAt(0, 1)
		_ = r.InsertAt(0, 1)

		// When
		err := r.DeleteAt(0)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		// Then
		values := readAllRLE(r.buffer)
		if want, got := []int64{1}, values; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("delete across var int size boundary", func(t *testing.T) {
		r := NewRLE(nil)
		n := 256

		for i := 0; i < n; i++ {
			err := r.InsertAt(0, 1)
			if err != nil {
				t.Fatalf("got %v; want nil", err)
			}

		}

		for i := 0; i < n; i++ {
			err := r.DeleteAt(0)
			if err != nil {
				t.Fatalf("got %v; want nil", err)
			}

		}

		if got, want := r.buffer, 0; len(got) != want {
			t.Fatalf("got %v; want %v", len(got), want)
		}
	})

	t.Run("delete middle element", func(t *testing.T) {
		r := NewRLE(nil)
		_ = r.InsertAt(0, 3)
		_ = r.InsertAt(0, 2)
		_ = r.InsertAt(0, 1)

		// When
		err := r.DeleteAt(1)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		// Then
		values := readAllRLE(r.buffer)
		if got, want := values, []int64{1, 3}; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v; want %v", len(got), want)
		}
	})

	t.Run("index below 0", func(t *testing.T) {
		r := NewRLE(nil)
		err := r.DeleteAt(-1)
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("got false; want true")
		}
	})

	t.Run("index out of bounds", func(t *testing.T) {
		r := NewRLE(nil)
		err := r.DeleteAt(2)
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("got false; want true")
		}
	})

	t.Run("delete in middle of long run", func(t *testing.T) {
		r := NewRLE([]byte{122, 2})
		if got, want := MustInt64(r.Int64()), 61; len(got) != want {
			t.Fatalf("got %v; want %v", len(got), want)
		}

		err := r.InsertAt(60, 61)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		got := MustInt64(r.Int64())
		if got, want := got, 62; len(got) != want {
			t.Fatalf("got %v; want %v", len(got), want)
		}
		if got, want := got[len(got)-4:], []int64{1, 1, 61, 1}; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v; want %v", len(got), want)
		}
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
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		l := readAllRLE(left.buffer)
		if got, want := l, []int64{1, 1, 1}; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v; want %v", len(got), want)
		}

		r := readAllRLE(right.buffer)
		if got, want := r, []int64{2, 2, 2}; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v; want %v", len(got), want)
		}
	})

	t.Run("split in middle", func(t *testing.T) {
		base := makeRLE()
		left, right, err := base.SplitAt(2)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		l := readAllRLE(left.buffer)
		if got, want := l, []int64{1, 1}; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v; want %v", len(got), want)
		}

		r := readAllRLE(right.buffer)
		if got, want := r, []int64{1, 2, 2, 2}; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v; want %v", len(got), want)
		}
	})

	t.Run("split head", func(t *testing.T) {
		base := makeRLE()
		left, right, err := base.SplitAt(0)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		l := readAllRLE(left.buffer)
		if want, got := []int64(nil), l; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v, want %v", got, want)
		}

		r := readAllRLE(right.buffer)
		if got, want := r, []int64{1, 1, 1, 2, 2, 2}; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v; want %v", len(got), want)
		}
	})

	t.Run("split tail", func(t *testing.T) {
		base := makeRLE()
		left, right, err := base.SplitAt(6)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		l := readAllRLE(left.buffer)
		if got, want := l, []int64{1, 1, 1, 2, 2, 2}; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v; want %v", len(got), want)
		}

		r := readAllRLE(right.buffer)
		if want, got := []int64(nil), r; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v, want %v", got, want)
		}
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

func readAllRLE(buffer []byte) []int64 {
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
