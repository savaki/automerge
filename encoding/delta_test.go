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
	"reflect"
	"testing"
)

func TestDelta_InsertAt(t *testing.T) {
	t.Run("insert empty", func(t *testing.T) {
		d := NewDelta(nil)
		err := d.InsertAt(0, 1)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		// Then
		got := readAllDeltaRLE(t, d)
		if want, got := []int64{1}, got; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("insert head", func(t *testing.T) {
		d := NewDelta(nil)

		err := d.InsertAt(0, 2)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		err = d.InsertAt(0, 1)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		// Then
		got := readAllDeltaRLE(t, d)
		if got, want := got, []int64{1, 2}; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v; want %v", len(got), want)
		}
	})

	t.Run("insert tail - different values", func(t *testing.T) {
		d := NewDelta(nil)

		err := d.InsertAt(0, 1)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		err = d.InsertAt(1, 5)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		err = d.InsertAt(2, 13)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		// Then
		got := readAllDeltaRLE(t, d)
		if got, want := got, []int64{1, 5, 13}; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v; want %v", len(got), want)
		}
	})

	t.Run("insert tail - same values", func(t *testing.T) {
		d := NewDelta(nil)

		err := d.InsertAt(0, 1)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		err = d.InsertAt(1, 1)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		err = d.InsertAt(2, 1)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		// Then
		got := readAllDeltaRLE(t, d)
		if got, want := got, []int64{1, 1, 1}; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v; want %v", len(got), want)
		}
	})

	t.Run("insert middle", func(t *testing.T) {
		d := NewDelta(nil)
		_ = d.InsertAt(0, 1)
		_ = d.InsertAt(1, 3)

		// When
		err := d.InsertAt(1, 2)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		// Then
		got := readAllDeltaRLE(t, d)
		if got, want := got, []int64{1, 2, 3}; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v; want %v", len(got), want)
		}
	})

	t.Run("failed to insert at end", func(t *testing.T) {
		d := NewDelta([]byte{122, 2})
		if got, want := d.MustValues(), 61; len(got) != want {
			t.Fatalf("got %v; want %v", len(got), want)
		}

		err := d.InsertAt(60, 61)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		got := MustInt64(d.Int64())
		if got, want := got, 62; len(got) != want {
			t.Fatalf("got %v; want %v", len(got), want)
		}
		if got, want := got[len(got)-4:], []int64{59, 60, 61, 61}; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v; want %v", len(got), want)
		}
	})
}

func TestDelta_SplitAt(t *testing.T) {
	makeItem := func() *Delta {
		base := NewDelta(nil)
		_ = base.InsertAt(0, 1)
		_ = base.InsertAt(1, 2)
		_ = base.InsertAt(2, 3)
		_ = base.InsertAt(3, 5)
		_ = base.InsertAt(4, 7)
		_ = base.InsertAt(5, 9)
		return base
	}

	t.Run("verify sequence", func(t *testing.T) {
		base := makeItem()
		got := readAllDeltaRLE(t, base)
		if got, want := got, []int64{1, 2, 3, 5, 7, 9}; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v; want %v", len(got), want)
		}
	})

	t.Run("split on boundary", func(t *testing.T) {
		base := makeItem()
		left, right, err := base.SplitAt(3)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		l := readAllDeltaRLE(t, left)
		if got, want := l, []int64{1, 2, 3}; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v; want %v", len(got), want)
		}

		r := readAllDeltaRLE(t, right)
		if got, want := r, []int64{5, 7, 9}; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v; want %v", len(got), want)
		}
	})

	t.Run("split in middle", func(t *testing.T) {
		base := makeItem()
		left, right, err := base.SplitAt(2)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		l := readAllDeltaRLE(t, left)
		if got, want := l, []int64{1, 2}; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v; want %v", len(got), want)
		}

		r := readAllDeltaRLE(t, right)
		if got, want := r, []int64{3, 5, 7, 9}; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v; want %v", len(got), want)
		}
	})

	t.Run("split head", func(t *testing.T) {
		base := makeItem()
		left, right, err := base.SplitAt(0)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		l := readAllDeltaRLE(t, left)
		if want, got := []int64(nil), l; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v, want %v", got, want)
		}

		r := readAllDeltaRLE(t, right)
		if got, want := r, []int64{1, 2, 3, 5, 7, 9}; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v; want %v", len(got), want)
		}
	})

	t.Run("split tail", func(t *testing.T) {
		base := makeItem()
		left, right, err := base.SplitAt(6)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		l := readAllDeltaRLE(t, left)
		if got, want := l, []int64{1, 2, 3, 5, 7, 9}; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v; want %v", len(got), want)
		}

		r := readAllDeltaRLE(t, right)
		if want, got := []int64(nil), r; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("split then continue", func(t *testing.T) {
		base := makeItem()
		left, _, err := base.SplitAt(3)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		err = left.InsertAt(3, 4)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		l := readAllDeltaRLE(t, left)
		if got, want := l, []int64{1, 2, 3, 4}; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v; want %v", len(got), want)
		}
	})
}

func readAllDeltaRLE(t *testing.T, d *Delta) []int64 {
	var err error
	var got []int64
	var token DeltaToken
	for {
		token, err = d.Next(token)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		got = append(got, token.Value)
	}
	return got
}
