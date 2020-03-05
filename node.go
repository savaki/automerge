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
	"io"

	"github.com/savaki/automerge/encoding"
)

const maxRowCount = 100

type Node struct {
	pages   []*Page
	rawType encoding.RawType
}

func NewNode(rawType encoding.RawType) *Node {
	return &Node{
		pages:   []*Page{NewPage(rawType)},
		rawType: rawType,
	}
}

func (n *Node) findPage(counter int64, actor []byte) (int, int64, error) {
	key := makeBloomKey(counter, actor)
	for i, p := range n.pages {
		if p.Contains(key) || actor == nil {
			index, err := p.FindIndex(counter, actor)
			if err != nil {
				if err == io.EOF {
					continue
				}
				return 0, 0, err
			}
			return i, index, nil
		}
	}
	return 0, 0, io.EOF
}

func (n *Node) Insert(op Op) error {
	pageIndex, index, err := n.findPage(op.RefCounter, op.RefActor)
	if err != nil {
		return fmt.Errorf("unable to find page with counter, %v, and actor, %v: %w", op.RefCounter, op.RefActor, err)
	}

	page := n.pages[pageIndex]

	if err := page.InsertAt(index, op); err != nil {
		return fmt.Errorf("insert failed: %w", err)
	}

	if page.rowCount > maxRowCount {
		left, right, err := page.SplitAt(maxRowCount / 2)
		if err != nil {
			return fmt.Errorf("unable to insert record: failed to split page at index, %v: %w", 600, err)
		}
		n.pages = append(n.pages, nil)
		for i := len(n.pages) - 1; i > pageIndex; i-- {
			n.pages[i] = n.pages[i-1]
		}

		n.pages[pageIndex] = left
		n.pages[pageIndex+1] = right
	}

	return nil
}

func (n *Node) Size() int {
	var size int
	for _, p := range n.pages {
		size += p.Size()
	}
	return size
}
