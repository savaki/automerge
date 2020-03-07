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
	"fmt"
	"io"
)

type DictionaryRLE struct {
	dict *Plain
	data *RLE
}

type DictionaryRLEToken struct {
	dict  map[int64][]byte
	data  RLEToken
	Value []byte
}

func NewDictionaryRLE(dict, data []byte) *DictionaryRLE {
	return &DictionaryRLE{
		dict: NewPlain(RawTypeByteArray, dict),
		data: NewRLE(data),
	}
}

func (d *DictionaryRLE) findOrInsert(value []byte, insert bool) (int64, error) {
	var i int64
	var token PlainToken
	var err error
	for {
		token, err = d.dict.Next(token)
		if err != nil {
			if err == io.EOF && insert {
				break
			}
			return 0, err
		}

		if bytes.Equal(value, token.Value.Bytes) {
			return int64(token.Index), nil
		}

		i++
	}

	if err := d.dict.InsertAt(i, ByteSliceValue(value)); err != nil {
		return 0, err
	}

	return int64(i), nil
}

func (d *DictionaryRLE) Get(index int64) ([]byte, error) {
	v, err := d.data.Get(index)
	if err != nil {
		return nil, err
	}

	var token PlainToken
	for {
		token, err = d.dict.Next(token)
		if err != nil {
			return nil, err
		}

		if token.Index == int(v) {
			return token.Value.Bytes, nil
		}
	}
}

func (d *DictionaryRLE) InsertAt(index int64, value []byte) error {
	v, err := d.findOrInsert(value, true)
	if err != nil {
		if err != io.EOF {
			return err
		}
	}

	return d.data.InsertAt(index, v)
}

func (d *DictionaryRLE) Lookup(value []byte) (int64, error) {
	return d.findOrInsert(value, false)
}

func (d *DictionaryRLE) Next(token DictionaryRLEToken) (DictionaryRLEToken, error) {
	if token.dict == nil {
		token.dict = map[int64][]byte{}

		var plainToken PlainToken
		var err error
		for {
			plainToken, err = d.dict.Next(plainToken)
			if err != nil {
				if err == io.EOF {
					break
				}
				return DictionaryRLEToken{}, err
			}
			token.dict[int64(plainToken.Index)] = plainToken.Value.Bytes
		}
	}

	rleToken, err := d.data.Next(token.data)
	if err != nil {
		return DictionaryRLEToken{}, err
	}

	data, ok := token.dict[rleToken.Value]
	if !ok {
		return DictionaryRLEToken{}, fmt.Errorf("unable to find token for index, %v", rleToken.Value)
	}

	return DictionaryRLEToken{
		dict:  token.dict,
		data:  rleToken,
		Value: data,
	}, nil
}

func (d *DictionaryRLE) RowCount() int {
	return len(readAllDictionary2(d))
}

func (d *DictionaryRLE) SplitAt(index int64) (left, right *DictionaryRLE, err error) {
	left = NewDictionaryRLE(nil, nil)
	right = NewDictionaryRLE(nil, nil)

	var i int64
	var token DictionaryRLEToken
	for {
		token, err = d.Next(token)
		if err != nil {
			if err == io.EOF {
				return left, right, nil
			}
			return nil, nil, err
		}

		switch {
		case i < index:
			if err := left.InsertAt(i, token.Value); err != nil {
				return nil, nil, err
			}

		default:
			if err := right.InsertAt(i-index, token.Value); err != nil {
				return nil, nil, err
			}
		}

		i++
	}
}

func (d *DictionaryRLE) Size() int {
	return d.dict.Size() + d.data.Size()
}

func readAllDictionary2(d *DictionaryRLE) [][]byte {
	var got [][]byte
	var token DictionaryRLEToken
	var err error
	for {
		token, err = d.Next(token)
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}

		got = append(got, token.Value)
	}
	return got
}
