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
	"fmt"
	"io"
)

type RLE struct {
	buffer []byte
}

type RLEToken struct {
	Pos    int
	Repeat int
	Index  int
	Value  int64
}

type rleBlock struct {
	Repeat       int64
	RepeatLength int
	Value        int64
	Length       int
}

type rleBuffer struct {
	Repeat       [8]byte
	RepeatLength int
	Value        [8]byte
	ValueLength  int
}

func (r rleBuffer) Length() int {
	return r.RepeatLength + r.ValueLength
}

func (r rleBuffer) Copy(buffer []byte) int {
	copy(buffer, r.Repeat[0:r.RepeatLength])
	copy(buffer[r.RepeatLength:], r.Value[0:r.ValueLength])
	return r.RepeatLength + r.ValueLength
}

func NewRLE(buffer []byte) *RLE {
	return &RLE{buffer: buffer}
}

func (r *RLE) readAt(pos int) (rleBlock, error) {
	var (
		repeat, repeatLength = binary.Varint(r.buffer[pos:])
		value, valueLength   = binary.Varint(r.buffer[pos+repeatLength:])
	)

	return rleBlock{
		Repeat:       repeat,
		RepeatLength: repeatLength,
		Value:        value,
		Length:       repeatLength + valueLength,
	}, nil
}

func (r *RLE) writeAtWithShift(pos int, repeat, value int64) int {
	buf := rleEncode(repeat, value)

	// shift bytes over to make room
	r.buffer = shift(r.buffer, pos, buf.RepeatLength+buf.ValueLength)

	copy(r.buffer[pos:], buf.Repeat[:buf.RepeatLength])
	copy(r.buffer[pos+buf.RepeatLength:], buf.Value[:buf.ValueLength])
	return buf.RepeatLength + buf.ValueLength
}

func (r *RLE) writeAt(pos int, repeat, value int64) int {
	buf := rleEncode(repeat, value)

	copy(r.buffer[pos:], buf.Repeat[:buf.RepeatLength])
	copy(r.buffer[pos+buf.RepeatLength:], buf.Value[:buf.ValueLength])
	return buf.RepeatLength + buf.ValueLength
}

func (r *RLE) DeleteAt(index int64) error {
	if index < 0 {
		return fmt.Errorf("rle delete failed: %w", io.ErrUnexpectedEOF)
	}

	var i int64
	var pos int
	for pos < len(r.buffer) {
		block, err := r.readAt(pos)
		if err != nil {
			return fmt.Errorf("unable to delete @%v: %w", index, err)
		}

		if index >= i && index < i+block.Repeat {
			// run length 1
			if block.Repeat == 1 {
				r.buffer = unshift(r.buffer, pos, block.Length)
				return nil
			}

			// shortened run length has same footprint
			repeat, repeatLength := putVarInt(block.Repeat - 1)
			if repeatLength == block.RepeatLength {
				copy(r.buffer[pos:], repeat[0:repeatLength])
				return nil
			}

			// shrink value
			r.buffer = unshift(r.buffer, pos+repeatLength, block.RepeatLength-repeatLength)
			copy(r.buffer[pos:], repeat[0:repeatLength])

			return nil
		}

		i += block.Repeat
		pos += block.Length
	}
	return fmt.Errorf("unable to delete @%v: %w", index, io.ErrUnexpectedEOF)
}

func (r *RLE) Get(index int64) (int64, error) {
	var i int64
	var pos int
	for pos < len(r.buffer) {
		block, err := r.readAt(pos)
		if err != nil {
			return 0, fmt.Errorf("unable to insert value at index, %v: %w", index, err)
		}

		if index >= i || index <= i+block.Repeat {
			return block.Value, nil
		}

		i += block.Repeat
		pos += block.Length
	}
	return 0, io.ErrUnexpectedEOF
}

func (r *RLE) InsertAt(index, v int64) error {
	var i int64
	var pos int
	for pos < len(r.buffer) {
		block, err := r.readAt(pos)
		if err != nil {
			return fmt.Errorf("unable to insert value, %v, at index, %v: %w", v, index, err)
		}

		switch {
		case v == block.Value && index >= i && index <= i+block.Repeat:
			repeat, repeatLength := putVarInt(block.Repeat + 1)
			if delta := repeatLength - block.RepeatLength; delta == 0 {
				// repeat length unchanged, just write over repeat
				copy(r.buffer[pos:], repeat[:repeatLength])
			} else {
				// repeat encoding length increased, shift contents and write both
				r.buffer = shift(r.buffer, pos, delta)
				r.writeAt(pos, block.Repeat+1, block.Value)
			}
			return nil

		case index == i:
			r.writeAtWithShift(pos, 1, v)
			return nil

		case index < i+block.Repeat:
			var (
				beforeN = index - i
				before  = rleEncode(beforeN, block.Value)
				buf     = rleEncode(1, v)
				after   = rleEncode(block.Repeat-beforeN, block.Value)
				delta   = before.Length() + buf.Length() + after.Length() - block.Length
			)

			// make space
			r.buffer = shift(r.buffer, pos, delta)

			n := before.Copy(r.buffer[pos:])
			pos += n

			n = buf.Copy(r.buffer[pos:])
			pos += n

			n = after.Copy(r.buffer[pos:])

			return nil
		}

		i += block.Repeat
		pos += block.Length
	}

	// new record
	r.writeAtWithShift(pos, 1, v)

	return nil
}

func (r *RLE) Int64() ([]int64, error) {
	var pos int
	var values []int64
	if len(r.buffer) > 0 {
		for {
			repeat, length := binary.Varint(r.buffer[pos:])
			pos += length

			value, length := binary.Varint(r.buffer[pos:])
			pos += length

			for i := int64(0); i < repeat; i++ {
				values = append(values, value)
			}

			if pos == len(r.buffer) {
				break
			}
		}
	}
	return values, nil
}

func (r *RLE) Next(token RLEToken) (RLEToken, error) {
	if token.Repeat > 0 {
		return RLEToken{
			Pos:    token.Pos,
			Index:  token.Index + 1,
			Repeat: token.Repeat - 1,
			Value:  token.Value,
		}, nil
	}
	if token.Pos >= len(r.buffer) {
		return RLEToken{}, io.EOF
	}

	pos := token.Pos
	repeat, length := binary.Varint(r.buffer[pos:])

	pos += length
	if pos >= len(r.buffer) {
		return RLEToken{}, io.ErrUnexpectedEOF
	}

	value, length := binary.Varint(r.buffer[pos:])

	index := token.Index + 1
	if token.Pos == 0 {
		index = 0
	}

	pos += length
	return RLEToken{
		Pos:    pos,
		Index:  index,
		Repeat: int(repeat) - 1,
		Value:  value,
	}, nil
}

func (r *RLE) RowCount() int {
	return len(readAllRLE2(r.buffer))
}

func (r *RLE) Size() int {
	return len(r.buffer)
}

func (r *RLE) SplitAt(index int64) (left, right *RLE, err error) {
	if index < 0 {
		return nil, nil, fmt.Errorf("unable to split on negative index")
	}

	var i int64
	var pos int
	for pos < len(r.buffer) {
		var (
			repeat, repeatLength = binary.Varint(r.buffer[pos:])
			value, valueLength   = binary.Varint(r.buffer[pos+repeatLength:])
		)

		switch {
		case index == i: // on run boundary
			rb := make([]byte, 0, len(r.buffer))
			rb = append(rb, r.buffer[pos:]...)
			lb := make([]byte, 0, len(r.buffer))
			lb = append(lb, r.buffer[0:pos]...)
			return NewRLE(lb), NewRLE(rb), nil

		case index > i && index < i+repeat: // in the middle
			rb := make([]byte, 0, cap(r.buffer))
			right := NewRLE(rb)
			right.writeAtWithShift(0, repeat-(index-i), value)
			right.buffer = append(right.buffer, r.buffer[pos+repeatLength+valueLength:]...)

			lb := make([]byte, 0, cap(r.buffer))
			lb = append(lb, r.buffer[0:pos]...)
			left := NewRLE(lb)
			left.writeAtWithShift(pos, index-i, value)

			return left, right, nil
		}

		i += repeat
		pos += repeatLength + valueLength
	}

	if i == index {
		return r, NewRLE(nil), nil
	}

	return nil, nil, io.ErrUnexpectedEOF
}

// Translate returns actual index within the page for the requested element accounting
// for deletes.
//
// For example, if the page consists of the operations: ins, ins, ins, del, ins
// and we ask to translate index 2, what we should get is index 4 since we have a
// del that would cancel out an ins.
func (r *RLE) Translate(index int64, isDelete func(opType int64) bool) (int64, error) {
	var (
		deletes     int64
		actualIndex int64
		token       RLEToken
		err         error
	)

	for i := int64(0); ; i++ {
		token, err = r.Next(token)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return 0, fmt.Errorf("unable to translate index, %v: %w", index, err)
		}

		if isDelete(token.Value) {
			deletes += 2
		}

		if index == i-deletes {
			actualIndex = i
		}
	}

	return actualIndex, nil
}

func rleEncode(repeat, value int64) rleBuffer {
	rb, rn := putVarInt(repeat)
	vb, vn := putVarInt(value)

	return rleBuffer{
		Repeat:       rb,
		RepeatLength: rn,
		Value:        vb,
		ValueLength:  vn,
	}
}

func readAllRLE2(buffer []byte) []int64 {
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
