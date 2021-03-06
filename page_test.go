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

package automerge

import (
	"fmt"
	"github.com/savaki/automerge/encoding"
	"io"
	"testing"
)

func TestPage_InsertAt(t *testing.T) {
	const n = 1e3

	me := []byte("me")
	you := []byte("you")
	page := NewPage(encoding.RawTypeVarInt)
	for i := int64(0); i < n; i++ {
		actor, refActor := you, me
		if i%8 == 0 {
			actor, refActor = me, you
		}

		op := Op{
			ID:    NewID(i+1, actor),
			Ref:   NewID(i, refActor),
			Type:  0,
			Value: encoding.RuneValue('a'),
		}
		err := page.InsertAt(i, op)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	}

	var token IDToken
	var err error
	for {
		token, err = page.NextID(token)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	}

	fmt.Println(page.Size())
}

func BenchmarkPage_Next(t *testing.B) {
	const n = 1e3

	me := []byte("me")
	you := []byte("you")
	page := NewPage(encoding.RawTypeVarInt)
	for i := int64(0); i < n; i++ {
		actor, refActor := you, me
		if i%8 == 0 {
			actor, refActor = me, you
		}

		op := Op{
			ID:    NewID(i+1, actor),
			Ref:   NewID(i, refActor),
			Type:  0,
			Value: encoding.RuneValue('a'),
		}
		err := page.InsertAt(i, op)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	}

	for i := 0; i < t.N; i++ {
		var token IDToken
		var err error
		for {
			token, err = page.NextID(token)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("got %v; want nil", err)
			}
		}
	}
}
