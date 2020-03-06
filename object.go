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

const (
	defaultRowCount = 100
)

type objectOptions struct {
	maxPageSize int64
}

// Object encapsulates a logical object within the document e.g. a Text block, an Object, an Array, etc
type Object struct {
	options objectOptions
	pages   []*Page
	rawType encoding.RawType
}

func makeObjectOptions(opts ...ObjectOption) objectOptions {
	options := objectOptions{
		maxPageSize: defaultRowCount,
	}
	for _, opt := range opts {
		opt(&options)
	}
	return options
}

// ObjectOption provides functional options to Object
type ObjectOption func(*objectOptions)

// WithMaxPageSize defines maximum number of records contained in a single page
func WithMaxPageSize(n int64) ObjectOption {
	return func(o *objectOptions) {
		if n <= 0 {
			return
		}
		o.maxPageSize = n
	}
}

// NewObject returns a new object whose value is of RawType using the options provided
func NewObject(rawType encoding.RawType, options ...ObjectOption) *Object {
	return &Object{
		options: makeObjectOptions(options...),
		pages:   []*Page{NewPage(rawType)},
		rawType: rawType,
	}
}

func (o *Object) findPage(counter int64, actor []byte) (int, int64, error) {
	key := makeBloomKey(counter, actor)
	for i, p := range o.pages {
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

func (o *Object) Insert(op Op) error {
	pageIndex, index, err := o.findPage(op.RefCounter, op.RefActor)
	if err != nil {
		return fmt.Errorf("unable to find page with counter, %v, and actor, %v: %w", op.RefCounter, op.RefActor, err)
	}

	page := o.pages[pageIndex]

	if err := page.InsertAt(index, op); err != nil {
		return fmt.Errorf("insert failed: %w", err)
	}

	if page.rowCount > o.options.maxPageSize {
		left, right, err := page.SplitAt(o.options.maxPageSize / 2)
		if err != nil {
			return fmt.Errorf("unable to insert record: failed to split page at index, %v: %w", 600, err)
		}
		o.pages = append(o.pages, nil)
		for i := len(o.pages) - 1; i > pageIndex; i-- {
			o.pages[i] = o.pages[i-1]
		}

		o.pages[pageIndex] = left
		o.pages[pageIndex+1] = right
	}

	return nil
}

func (o *Object) Size() int {
	var size int
	for _, p := range o.pages {
		size += p.Size()
	}
	return size
}
