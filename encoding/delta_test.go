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
	"github.com/tj/assert"
	"io"
	"testing"
)

func TestDelta_InsertAt(t *testing.T) {
	t.Run("insert empty", func(t *testing.T) {
		d := NewDelta(nil)
		err := d.InsertAt(0, 1)
		assert.Nil(t, err)

		// Then
		got := readAllDeltaRLE(t, d)
		assert.Equal(t, []int64{1}, got)
	})

	t.Run("insert head", func(t *testing.T) {
		d := NewDelta(nil)

		err := d.InsertAt(0, 2)
		assert.Nil(t, err)

		err = d.InsertAt(0, 1)
		assert.Nil(t, err)

		// Then
		got := readAllDeltaRLE(t, d)
		assert.Equal(t, []int64{1, 2}, got)
	})

	t.Run("insert tail - different values", func(t *testing.T) {
		d := NewDelta(nil)

		err := d.InsertAt(0, 1)
		assert.Nil(t, err)

		err = d.InsertAt(1, 5)
		assert.Nil(t, err)

		err = d.InsertAt(2, 13)
		assert.Nil(t, err)

		// Then
		got := readAllDeltaRLE(t, d)
		assert.Equal(t, []int64{1, 5, 13}, got)
	})

	t.Run("insert tail - same values", func(t *testing.T) {
		d := NewDelta(nil)

		err := d.InsertAt(0, 1)
		assert.Nil(t, err)

		err = d.InsertAt(1, 1)
		assert.Nil(t, err)

		err = d.InsertAt(2, 1)
		assert.Nil(t, err)

		// Then
		got := readAllDeltaRLE(t, d)
		assert.Equal(t, []int64{1, 1, 1}, got)
	})

	t.Run("insert middle", func(t *testing.T) {
		d := NewDelta(nil)
		_ = d.InsertAt(0, 1)
		_ = d.InsertAt(1, 3)

		// When
		err := d.InsertAt(1, 2)
		assert.Nil(t, err)

		// Then
		got := readAllDeltaRLE(t, d)
		assert.Equal(t, []int64{1, 2, 3}, got)
	})

	t.Run("failed to insert at end", func(t *testing.T) {
		d := NewDelta([]byte{122, 2})
		assert.Len(t, d.MustValues(), 61)

		err := d.InsertAt(60, 61)
		assert.Nil(t, err)

		got := MustInt64(d.Int64())
		assert.Len(t, got, 62)
		assert.Equal(t, []int64{59, 60, 61, 61}, got[len(got)-4:])
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
		assert.Equal(t, []int64{1, 2, 3, 5, 7, 9}, got)
	})

	t.Run("split on boundary", func(t *testing.T) {
		base := makeItem()
		left, right, err := base.SplitAt(3)
		assert.Nil(t, err)

		l := readAllDeltaRLE(t, left)
		assert.Equal(t, []int64{1, 2, 3}, l)

		r := readAllDeltaRLE(t, right)
		assert.Equal(t, []int64{5, 7, 9}, r)
	})

	t.Run("split in middle", func(t *testing.T) {
		base := makeItem()
		left, right, err := base.SplitAt(2)
		assert.Nil(t, err)

		l := readAllDeltaRLE(t, left)
		assert.Equal(t, []int64{1, 2}, l)

		r := readAllDeltaRLE(t, right)
		assert.Equal(t, []int64{3, 5, 7, 9}, r)
	})

	t.Run("split head", func(t *testing.T) {
		base := makeItem()
		left, right, err := base.SplitAt(0)
		assert.Nil(t, err)

		l := readAllDeltaRLE(t, left)
		assert.Equal(t, []int64(nil), l)

		r := readAllDeltaRLE(t, right)
		assert.Equal(t, []int64{1, 2, 3, 5, 7, 9}, r)
	})

	t.Run("split tail", func(t *testing.T) {
		base := makeItem()
		left, right, err := base.SplitAt(6)
		assert.Nil(t, err)

		l := readAllDeltaRLE(t, left)
		assert.Equal(t, []int64{1, 2, 3, 5, 7, 9}, l)

		r := readAllDeltaRLE(t, right)
		assert.Equal(t, []int64(nil), r)
	})

	t.Run("split then continue", func(t *testing.T) {
		base := makeItem()
		left, _, err := base.SplitAt(3)
		assert.Nil(t, err)

		err = left.InsertAt(3, 4)
		assert.Nil(t, err)

		l := readAllDeltaRLE(t, left)
		assert.Equal(t, []int64{1, 2, 3, 4}, l)
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
		assert.Nil(t, err)

		got = append(got, token.Value)
	}
	return got
}
