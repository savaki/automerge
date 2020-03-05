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

	t.Run("insert tail", func(t *testing.T) {
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
