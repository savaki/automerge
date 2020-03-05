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
	"github.com/tj/assert"
	"testing"
)

func TestNode_Insert(t *testing.T) {
	const n = 1e4

	me := []byte("me")
	node := NewNode(encoding.RawTypeVarInt)
	for i := int64(0); i < n; i++ {
		op := Op{
			OpCounter:  i + 1,
			OpActor:    me,
			RefCounter: i,
			RefActor:   nil,
			Type:       0,
			Value:      encoding.RuneValue('a'),
		}
		err := node.Insert(op)
		assert.Nil(t, err)
	}

	fmt.Println(node.Size())
}
