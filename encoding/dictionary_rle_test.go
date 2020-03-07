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
	"reflect"
	"testing"
)

func TestDictionaryRLE_InsertAt(t *testing.T) {
	d := NewDictionaryRLE(nil, nil)

	a, b := []byte("hello"), []byte("world")

	err := d.InsertAt(0, a)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	err = d.InsertAt(1, b)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	v, err := d.Lookup(a)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	if want, got := int64(0), v; want != got {
		t.Fatalf("got %v, want %v", got, want)
	}

	v, err = d.Lookup(b)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}
	if want, got := int64(1), v; want != got {
		t.Fatalf("got %v, want %v", got, want)
	}

	got := readAllDictionary(t, d)
	if got, want := got, 2; len(got) != want {
		t.Fatalf("got %v; want %v", len(got), want)
	}
	if want, got := string(a), string(got[0]); got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if want, got := string(b), string(got[1]); got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestDictionaryRLE_SplitAt(t *testing.T) {
	t.Run("middle", func(t *testing.T) {
		d := NewDictionaryRLE(nil, nil)
		a, b, c := []byte("a"), []byte("b"), []byte("c")
		err := d.InsertAt(0, a)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		err = d.InsertAt(1, b)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		err = d.InsertAt(2, c)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		left, right, err := d.SplitAt(2)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		got := readAllDictionary(t, left)
		if got, want := got, 2; len(got) != want {
			t.Fatalf("got %v; want %v", len(got), want)
		}
		if want, got := a, got[0]; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v, want %v", got, want)
		}
		if want, got := b, got[1]; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v, want %v", got, want)
		}

		got = readAllDictionary(t, right)
		if got, want := got, 1; len(got) != want {
			t.Fatalf("got %v; want %v", len(got), want)
		}
		if want, got := c, got[0]; !reflect.DeepEqual(want, got) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})
}

func readAllDictionary(t *testing.T, d *DictionaryRLE) [][]byte {
	var got [][]byte
	var token DictionaryRLEToken
	var err error
	for {
		token, err = d.Next(token)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		got = append(got, token.Value)
	}
	return got
}
