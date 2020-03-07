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
	"reflect"
	"testing"
	"unicode/utf8"
)

func TestEquals(t *testing.T) {
	if want, got := 4, lengthUint32; !reflect.DeepEqual(want, got) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestEncodeVarInt(t *testing.T) {
	if want, got := 3, utf8.RuneLen('你'); got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}
