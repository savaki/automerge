package encoding

import (
	"io"
)

type Delta struct {
	rle     *RLE
	numRows int64
}

type DeltaToken struct {
	rle   RLEToken
	Value int64
}

func NewDelta(buffer []byte) *Delta {
	return &Delta{rle: NewRLE(buffer)}
}

func (d *Delta) Get(index int64) (int64, error) {
	var i int64
	var token DeltaToken
	var err error
	for {
		token, err = d.Next(token)
		if err != nil {
			return 0, nil
		}

		if index == i {
			return token.Value, nil
		}

		i++
	}
}

func (d *Delta) InsertAt(index, value int64) error {
	switch {
	case index < 0 || index > d.numRows:
		return io.ErrUnexpectedEOF

	case d.numRows == 0: // empty
		d.numRows++
		return d.rle.InsertAt(0, value)

	case index == 0: // head
		d.numRows++
		v, err := d.rle.Get(index)
		if err != nil {
			return err
		}
		if err := d.rle.DeleteAt(index); err != nil {
			return err
		}
		if err := d.rle.InsertAt(index, value); err != nil {
			return err
		}
		return d.rle.InsertAt(index+1, v-value)
	}

	var i int64
	var lastValue int64
	var token DeltaToken
	var err error
	for ; ; i++ {
		token, err = d.Next(token)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if i == index {
			if err := d.rle.DeleteAt(i); err != nil {
				return nil
			}
			if err := d.rle.InsertAt(i, value-lastValue); err != nil {
				return nil
			}
			if err := d.rle.InsertAt(i+1, token.Value-value); err != nil {
				return nil
			}
			d.numRows++
			return nil
		}

		lastValue = token.Value
	}

	if i == index {
		d.numRows++
		return d.rle.InsertAt(i, value-lastValue)
	}

	return io.ErrUnexpectedEOF
}

func (d *Delta) Next(token DeltaToken) (DeltaToken, error) {
	rleToken, err := d.rle.Next(token.rle)
	if err != nil {
		return DeltaToken{}, err
	}

	return DeltaToken{
		rle:   rleToken,
		Value: token.Value + rleToken.Value,
	}, nil
}

func (d *Delta) Size() int {
	return len(d.rle.buffer)
}

func (d *Delta) SplitAt(index int64) (left, right *Delta, err error) {
	v, err := d.Get(index)
	if err != nil {
		return nil, nil, err
	}

	l, r, err := d.rle.SplitAt(index)
	if err != nil {
		return nil, nil, err
	}

	left = &Delta{
		rle:     l,
		numRows: index,
	}
	right = &Delta{
		rle:     r,
		numRows: d.numRows - index,
	}
	if right.numRows > 0 {
		if err := right.rle.DeleteAt(0); err != nil {
			return nil, nil, err
		}
		right.rle.writeAtWithShift(0, 1, v)
	}

	return
}
