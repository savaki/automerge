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

func (p *Plain) RowCount() int {
	var err error
	var got []Value
	var token PlainToken
	for {
		token, err = p.Next(token)
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}

		got = append(got, token.Value)
	}
	return len(got)
}
