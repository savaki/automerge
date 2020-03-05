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
	"testing"

	"github.com/tj/assert"
)

func TestDictionary_Lookup(t *testing.T) {
	d := NewDictionary(nil)

	// upsert
	v, err := d.LookupString("hello")
	assert.Nil(t, err)
	assert.EqualValues(t, 0, v)

	// idempotent
	v, err = d.LookupString("hello")
	assert.Nil(t, err)
	assert.EqualValues(t, 0, v)

	// new value
	v, err = d.LookupString("world")
	assert.Nil(t, err)
	assert.EqualValues(t, 1, v)

	buf := bytes.NewBuffer(nil)
	err = d.AppendTo(buf)
	assert.Nil(t, err)
	d = NewDictionary(buf.Bytes())

	// idempotent
	v, err = d.LookupString("world")
	assert.Nil(t, err)
	assert.EqualValues(t, 1, v)

	// and another
	v, err = d.LookupString("blah")
	assert.Nil(t, err)
	assert.EqualValues(t, 2, v)

	data, err := d.Get(0)
	assert.Nil(t, err)
	assert.Equal(t, "hello", string(data))

	data, err = d.Get(1)
	assert.Nil(t, err)
	assert.Equal(t, "world", string(data))
}

func BenchmarkDictionary(t *testing.B) {
	d := NewDictionary(nil)

	_, err := d.LookupString("hello")
	assert.Nil(t, err)

	_, err = d.LookupString("world")
	assert.Nil(t, err)

	buf := bytes.NewBuffer(nil)
	err = d.AppendTo(buf)
	assert.Nil(t, err)

	key := []byte("abc")
	d = NewDictionary(buf.Bytes())
	_, err = d.Lookup(key)
	assert.Nil(t, err)

	for i := 0; i < t.N; i++ {
		got, err := d.Lookup(key)
		if err != nil {
			t.Errorf("got %v; want nil", err)
		}
		if want := int64(2); got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
	}
}

func Test_keyNotFound_Error(t *testing.T) {
	err := keyNotFound{}
	assert.Equal(t, "key not found", err.Error())
}
