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
	"bytes"
	"io"
)

type keyNotFound struct {
	lastIndex int64
}

func (keyNotFound) Error() string {
	return "key not found"
}

type Dictionary struct {
	buffer []byte
}

func NewDictionary(buffer []byte) *Dictionary {
	return &Dictionary{buffer: buffer}
}

func (d *Dictionary) findKey(key []byte) (int64, error) {
	var (
		l     = int64(len(d.buffer))
		pos   int64
		index int64
	)

	for pos < l {
		length, err := readUint32(d.buffer[pos:])
		if err != nil {
			return 0, err
		}

		from, to := pos+lengthUint32, pos+lengthUint32+int64(length)
		if to > l {
			return 0, io.ErrUnexpectedEOF
		}

		if bytes.Equal(d.buffer[from:to], key) {
			return index, nil
		}

		index++
		pos = to
	}

	return 0, keyNotFound{lastIndex: index}
}

func (d *Dictionary) AppendTo(w io.Writer) error {
	_, err := w.Write(d.buffer)
	return err
}

func (d *Dictionary) Size() int {
	return len(d.buffer)
}

func (d *Dictionary) Lookup(key []byte) (int64, error) {
	index, err := d.findKey(key)
	if err != nil {
		if knf, ok := err.(keyNotFound); ok {
			buf := putUint32(uint32(len(key)))
			d.buffer = append(d.buffer, buf[:]...)
			d.buffer = append(d.buffer, key...)

			return knf.lastIndex, nil
		}
		return 0, err
	}

	return index, nil
}

func (d *Dictionary) LookupString(key string) (int64, error) {
	return d.Lookup([]byte(key))
}

func (d *Dictionary) Get(index int) ([]byte, error) {
	if index < 0 {
		return nil, io.ErrUnexpectedEOF
	}

	var (
		l   = int64(len(d.buffer))
		pos int64
	)

	for pos < l {
		length, err := readUint32(d.buffer[pos:])
		if err != nil {
			return nil, err
		}

		from, to := pos+lengthUint32, pos+lengthUint32+int64(length)
		if to > l {
			return nil, io.ErrUnexpectedEOF
		}

		if index == 0 {
			return d.buffer[from:to], nil
		}

		index--
		pos = to
	}

	return nil, io.ErrUnexpectedEOF
}
