package encoding

import (
	"encoding/binary"
	"fmt"
	"io"
)

type RLE struct {
	buffer []byte
}

type RLEToken struct {
	pos    int
	repeat int
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
		return io.ErrUnexpectedEOF
	}

	var i int64
	var pos int
	for pos < len(r.buffer) {
		block, err := r.readAt(pos)
		if err != nil {
			return fmt.Errorf("unable to delete index, %v: %w", index, err)
		}

		if i >= index && i < index+block.Repeat {
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
	return io.ErrUnexpectedEOF
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

func (r *RLE) Next(token RLEToken) (RLEToken, error) {
	if token.repeat > 0 {
		return RLEToken{
			pos:    token.pos,
			Index:  token.Index + 1,
			repeat: token.repeat - 1,
			Value:  token.Value,
		}, nil
	}
	if token.pos >= len(r.buffer) {
		return RLEToken{}, io.EOF
	}

	pos := token.pos
	repeat, length := binary.Varint(r.buffer[pos:])

	pos += length
	if pos >= len(r.buffer) {
		return RLEToken{}, io.ErrUnexpectedEOF
	}

	value, length := binary.Varint(r.buffer[pos:])

	index := token.Index + 1
	if token.pos == 0 {
		index = 0
	}

	pos += length
	return RLEToken{
		pos:    pos,
		Index:  index,
		repeat: int(repeat) - 1,
		Value:  value,
	}, nil
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
			lb := r.buffer[0:pos]
			return NewRLE(lb), NewRLE(rb), nil

		case index > i && index < i+repeat: // in the middle
			right := NewRLE(make([]byte, 0, len(r.buffer)))
			right.writeAtWithShift(0, repeat-(index-i), value)
			right.buffer = append(right.buffer, r.buffer[pos+repeatLength+valueLength:]...)

			left := NewRLE(r.buffer[0:pos])
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
