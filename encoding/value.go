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
)

type RawType uint8
type LogicalType uint8

const (
	RawTypeUnknown   RawType = 0
	RawTypeVarInt    RawType = 1
	RawTypeByteArray RawType = 2
)

const (
	LogicalTypeUnknown  LogicalType = 0
	LogicalTypeInt64    LogicalType = 1
	LogicalTypeString   LogicalType = 2
	LogicalTypeProperty LogicalType = 3
)

type Value struct {
	length  int
	Int     int64
	Bytes   []byte
	RawType RawType
}

func (v Value) Append(buffer []byte) ([]byte, error) {
	switch v.RawType {
	case RawTypeVarInt:
		buf, n := putVarInt(v.Int)
		buffer = append(buffer, buf[:n]...)
		return buffer, nil

	case RawTypeByteArray:
		length := len(v.Bytes)
		buf, n := putVarInt(int64(length))
		buffer = append(buffer, buf[:n]...)
		buffer = append(buffer, v.Bytes...)
		return buffer, nil

	default:
		return nil, fmt.Errorf("unable to append: unknown raw type, %v", v.RawType)
	}
}

func (v Value) Copy(target []byte) {
	switch v.RawType {
	case RawTypeVarInt:
		buf, n := putVarInt(v.Int)
		copy(target, buf[0:n])

	case RawTypeByteArray:
		length := len(v.Bytes)
		buf, n := putVarInt(int64(length))
		copy(target, buf[:n])
		copy(target[n:], v.Bytes)
	}
}

// Length of encoded element
func (v Value) Length() int {
	if v.length > 0 {
		return v.length // returned cached length
	}

	switch v.RawType {
	case RawTypeVarInt:
		_, length := putVarInt(v.Int)
		return length

	case RawTypeByteArray:
		n := len(v.Bytes)
		_, length := putVarInt(int64(n))
		return length + n

	default:
		return -1
	}
}

func ReadValue(rawType RawType, buffer []byte) (Value, error) {
	switch rawType {
	case RawTypeVarInt:
		v, length := binary.Varint(buffer)
		return Value{
			length:  length,
			Int:     v,
			RawType: rawType,
		}, nil

	case RawTypeByteArray:
		v, vl := binary.Varint(buffer)

		length := vl + int(v)
		return Value{
			length:  length,
			Bytes:   buffer[vl:length],
			RawType: rawType,
		}, nil

	default:
		return Value{}, fmt.Errorf("unable to read value: unknown raw type, %v", rawType)
	}
}

// ByteSliceValue encodes to a var int length followed by the bytes
func ByteSliceValue(data []byte) Value {
	_, length := putVarInt(int64(len(data)))

	return Value{
		length:  length + len(data),
		Bytes:   data,
		RawType: RawTypeByteArray,
	}
}

// Int64Value encodes to var int value
func Int64Value(v int64) Value {
	return Value{
		Int:     v,
		RawType: RawTypeVarInt,
	}
}

// RuneValue encodes var int value
func RuneValue(r rune) Value {
	return Value{
		Int:     int64(r),
		RawType: RawTypeVarInt,
	}
}

// PropertyValue encodes to:
// * var int total length
// * var int key length
// * key bytes
// * var int value length
// * value bytes
//
// While we could remove total length at the top, this would require a new raw format.
// Instead, we'll take the var int hit to allow us to encode the property as a byte slice.
func PropertyValue(key int64, value []byte) Value {
	kb, kn := putVarInt(key)
	vb, vn := putVarInt(int64(len(value)))
	data := make([]byte, 0, kn+vn+len(value))
	data = append(data, kb[0:kn]...)
	data = append(data, vb[0:vn]...)
	data = append(data, value...)
	return ByteSliceValue(data)
}

// StringValue encodes to a var int length followed by a byte array
func StringValue(s string) Value {
	return ByteSliceValue([]byte(s))
}

func DecodePropertyValue(buffer []byte) (int64, []byte, error) {
	var (
		kv, kn = binary.Varint(buffer)
		vv, vn = binary.Varint(buffer[kn:])
	)

	return kv, buffer[kn+vn : kn+vn+int(vv)], nil
}
