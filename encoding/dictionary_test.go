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
	"reflect"
	"testing"
)

func TestDictionary_Lookup(t *testing.T) {
	d := NewDictionary(nil)

	// upsert
	v, err := d.LookupString("hello")
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}
	if want, got := int64(0), v; want != got {
		t.Fatalf("got %v, want %v", got, want)
	}

	// idempotent
	v, err = d.LookupString("hello")
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}
	if want, got := int64(0), v; want != got {
		t.Fatalf("got %v, want %v", got, want)
	}

	// new value
	v, err = d.LookupString("world")
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}
	if want, got := int64(1), v; want != got {
		t.Fatalf("got %v, want %v", got, want)
	}

	buf := bytes.NewBuffer(nil)
	err = d.AppendTo(buf)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	d = NewDictionary(buf.Bytes())

	// idempotent
	v, err = d.LookupString("world")
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}
	if want, got := int64(1), v; want != got {
		t.Fatalf("got %v, want %v", got, want)
	}

	// and another
	v, err = d.LookupString("blah")
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}
	if want, got := int64(2), v; want != got {
		t.Fatalf("got %v, want %v", got, want)
	}

	data, err := d.Get(0)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}
	if want, got := "hello", string(data); got != want {
		t.Fatalf("got %v, want %v", got, want)
	}

	data, err = d.Get(1)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}
	if want, got := "world", string(data); got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func BenchmarkDictionary(t *testing.B) {
	d := NewDictionary(nil)

	_, err := d.LookupString("hello")
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	_, err = d.LookupString("world")
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	buf := bytes.NewBuffer(nil)
	err = d.AppendTo(buf)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	key := []byte("abc")
	d = NewDictionary(buf.Bytes())
	_, err = d.Lookup(key)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

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
	if got, want := err.Error(), "key not found"; !reflect.DeepEqual(want, got) {
		t.Fatalf("got %v; want %v", len(got), want)
	}
}
