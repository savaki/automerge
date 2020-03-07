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
	"fmt"
	"github.com/willf/bloom"
	"reflect"
	"testing"
)

//func Test_shift(t *testing.T) {
//	testCases := map[string]struct {
//		buffer []byte
//		pos    int
//		offset int
//		want   []byte
//	}{
//		"nop": {
//			buffer: nil,
//			pos:    0,
//			offset: 0,
//			want:   nil,
//		},
//		"no shift": {
//			buffer: []byte("abc"),
//			pos:    0,
//			offset: 0,
//			want:   []byte("abc"),
//		},
//		"offset 1": {
//			buffer: []byte{1, 2, 3},
//			pos:    0,
//			offset: 1,
//			want:   []byte{1, 1, 2, 3},
//		},
//		"offset 2": {
//			buffer: []byte{1, 2, 3},
//			pos:    0,
//			offset: 2,
//			want:   []byte{1, 2, 1, 2, 3},
//		},
//		"offset 4": {
//			buffer: []byte{1, 2, 3},
//			pos:    0,
//			offset: 4,
//			want:   []byte{1, 2, 1, 2, 3},
//		},
//		"pos 1, shift 2": {
//			buffer: []byte{1, 2, 3},
//			pos:    1,
//			offset: 2,
//			want:   []byte{1, 2, 3, 2, 3},
//		},
//	}
//	for label, tt := range testCases {
//		t.Run(label, func(t *testing.T) {
//			if got := insertAt(tt.buffer, tt.pos, tt.offset); !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("shift() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}

func Test_insertAt(t *testing.T) {
	tests := map[string]struct {
		buffer string
		pos    int
		bytes  string
		want   string
	}{
		//"nop": {
		//	buffer: "",
		//	pos:    0,
		//	bytes:  "",
		//	want:   "",
		//},
		"no change": {
			buffer: "abc",
			pos:    0,
			bytes:  "",
			want:   "abc",
		},
		//"head": {
		//	buffer: "def",
		//	pos:    0,
		//	bytes:  "abc",
		//	want:   "abcdef",
		//},
		//"p1": {
		//	buffer: "def",
		//	pos:    1,
		//	bytes:  "abc",
		//	want:   "dabcef",
		//},
		//"p2": {
		//	buffer: "def",
		//	pos:    2,
		//	bytes:  "abc",
		//	want:   "deabcf",
		//},
		//"p3": {
		//	buffer: "abc",
		//	pos:    3,
		//	bytes:  "def",
		//	want:   "abcdef",
		//},
	}
	for label, tt := range tests {
		t.Run(label, func(t *testing.T) {
			if got := insertAt([]byte(tt.buffer), tt.pos, []byte(tt.bytes)...); string(got) != tt.want {
				t.Errorf("insertAt() = %v, want %v", string(got), tt.want)
			}
		})
	}
}

func Test_shift(t *testing.T) {
	testCases := map[string]struct {
		buffer []byte
		pos    int
		length int
		want   []byte
	}{
		"nop": {
			buffer: nil,
			pos:    0,
			length: 0,
			want:   nil,
		},
		"no change": {
			buffer: []byte{1, 2, 3},
			pos:    0,
			length: 0,
			want:   []byte{1, 2, 3},
		},
		"p0, l2": {
			buffer: []byte{1, 2, 3},
			pos:    0,
			length: 2,
			want:   []byte{1, 2, 1, 2, 3},
		},
		"p1, l2": {
			buffer: []byte{1, 2, 3},
			pos:    1,
			length: 2,
			want:   []byte{1, 2, 3, 2, 3},
		},
		"p2, l2": {
			buffer: []byte{1, 2, 3},
			pos:    2,
			length: 2,
			want:   []byte{1, 2, 3, 0, 3},
		},
		"p3, l2": {
			buffer: []byte{1, 2, 3},
			pos:    3,
			length: 2,
			want:   []byte{1, 2, 3, 0, 0},
		},
	}
	for label, tt := range testCases {
		t.Run(label, func(t *testing.T) {
			if got := shift(tt.buffer, tt.pos, tt.length); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("shift() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReadNil(t *testing.T) {
	v, n := binary.Varint(make([]byte, 0))
	fmt.Println(v, n)
}

func TestBloomFilter(t *testing.T) {
	filter := bloom.New(12365, 3)
	data, err := filter.GobEncode()
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}
	if got, want := data, 1576; len(got) != want {
		t.Fatalf("got %v; want %v", len(got), want)
	}
}
