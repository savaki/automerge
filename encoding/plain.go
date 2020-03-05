package encoding

import (
	"io"
)

type Plain struct {
	buffer  []byte
	rawType RawType
}

type PlainToken struct {
	pos   int
	Index int
	Value Value
}

func NewPlain(rawType RawType, buffer []byte) *Plain {
	return &Plain{
		buffer:  buffer,
		rawType: rawType,
	}
}

func (p *Plain) InsertAt(index int64, value Value) error {
	var i int64
	var pos int
	for pos < len(p.buffer) {
		if i == index {
			break
		}

		got, err := ReadValue(p.rawType, p.buffer[pos:])
		if err != nil {
			return err
		}

		i++
		pos += got.Length()
	}

	if i == index {
		p.buffer = shift(p.buffer, pos, value.Length())
		value.Copy(p.buffer[pos:])
		return nil
	}

	return io.ErrUnexpectedEOF
}

func (p *Plain) Next(token PlainToken) (PlainToken, error) {
	if token.pos >= len(p.buffer) {
		return PlainToken{}, io.EOF
	}

	got, err := ReadValue(p.rawType, p.buffer[token.pos:])
	if err != nil {
		return PlainToken{}, err
	}

	index := token.Index + 1
	if token.pos == 0 {
		index = 0
	}

	return PlainToken{
		pos:   token.pos + got.Length(),
		Index: index,
		Value: got,
	}, err
}

func (p *Plain) SplitAt(index int64) (left, right *Plain, err error) {
	var i int64
	var pos int
	for pos < len(p.buffer) {
		if i == index {
			rb := make([]byte, 0, cap(p.buffer))
			rb = append(rb, p.buffer[pos:]...)
			lb := p.buffer[0:pos]

			return NewPlain(p.rawType, lb), NewPlain(p.rawType, rb), nil
		}

		got, err := ReadValue(p.rawType, p.buffer[pos:])
		if err != nil {
			return nil, nil, err
		}

		i++
		pos += got.Length()
	}

	if i == index {
		return p, NewPlain(p.rawType, nil), nil
	}

	return nil, nil, io.ErrUnexpectedEOF
}

func (p *Plain) Size() int {
	return len(p.buffer)
}