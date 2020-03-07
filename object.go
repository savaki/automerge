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
	"errors"
	"fmt"
	"io"

	"github.com/savaki/automerge/encoding"
	"github.com/willf/bloom"
)

const (
	defaultRowCount = 200
	defaultBloomM   = 15000
	defaultBloomK   = 8
)

type bloomOptions struct {
	M uint
	K uint
}

type objectOptions struct {
	Bloom       bloomOptions
	MaxPageSize int64
}

// Object encapsulates a logical object within the document e.g. a Text block, an Object, an Array, etc
type Object struct {
	options objectOptions
	pages   []*Page
	filters []*bloom.BloomFilter
	rawType encoding.RawType

	last struct {
		Filter    *bloom.BloomFilter
		ID        ID
		Index     int64
		PageIndex int
		Ok        bool
	}
}

type ValueToken struct {
	PageValueToken
	pageIndex int
}

func makeObjectOptions(opts ...ObjectOption) objectOptions {
	options := objectOptions{
		Bloom: bloomOptions{
			M: defaultBloomM,
			K: defaultBloomK,
		},
		MaxPageSize: defaultRowCount,
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
		o.MaxPageSize = n
	}
}

func WithBloomOptions(m, k uint) ObjectOption {
	return func(o *objectOptions) {
		if m <= 0 || k <= 0 {
			return
		}
		o.Bloom = bloomOptions{
			M: m,
			K: k,
		}
	}
}

// NewObject returns a new object whose value is of RawType using the options provided
func NewObject(rawType encoding.RawType, opts ...ObjectOption) *Object {
	options := makeObjectOptions(opts...)
	filter, _ := makeBloomFilter(options.Bloom, nil)
	return &Object{
		options: options,
		pages:   []*Page{NewPage(rawType)},
		filters: []*bloom.BloomFilter{filter},
		rawType: rawType,
	}
}

// findPageIndex accepts an id and returns the index within r.pages
func (o *Object) findPageIndex(id ID) (pageIndex int, index int64, err error) {
	var key *bloomKey
	if o.last.Ok {
		// many times, the next edit will follow the previous
		if o.last.ID.Equal(id) {
			return o.last.PageIndex, o.last.Index, nil
		}

		// even if not directly next, they next edit is often close to the previous
		key = makeBloomKey(id.Counter, id.Actor)
		defer key.Free()
		if o.last.Filter.Test(key.data) {
			page := o.pages[o.last.PageIndex]
			if index, err := page.FindIndex(id.Counter, id.Actor); err == nil {
				return o.last.PageIndex, index, nil
			}
		}
	}

	if id.Counter == 0 && len(id.Actor) == 0 {
		return 0, -1, nil
	}

	if key == nil {
		key = makeBloomKey(id.Counter, id.Actor)
		defer key.Free()
	}

	for i, p := range o.pages {
		if o.filters[i].Test(key.data) || id.Actor == nil {
			index, err := p.FindIndex(id.Counter, id.Actor)
			if err != nil {
				if err == io.EOF {
					continue
				}
				return 0, 0, fmt.Errorf("unable to find (%v,%v) in page, %v: false positive: %w", id.Counter, id.Actor, i, err)
			}
			return i, index, nil
		}
	}
	return 0, 0, io.EOF
}

func (o *Object) splitPageAt(pageIndex int, index int64) error {
	// when pages exceed optimal size, split them in half.  splitting the pages in half will
	// require recalculating the bloom filter for each of the resulting pages.
	// todo - consider algorithms to split on other boundaries

	page := o.pages[pageIndex]
	left, right, err := page.SplitAt(index)
	if err != nil {
		return fmt.Errorf("unable to insert record: failed to split page, %v, at index, %v: %w", pageIndex, index, err)
	}

	leftFilter, err := makeBloomFilter(o.options.Bloom, left)
	if err != nil {
		return fmt.Errorf("unable to split page at index, %v: failed to update left bloom filter: %w", index, err)
	}

	rightFilter, err := makeBloomFilter(o.options.Bloom, right)
	if err != nil {
		return fmt.Errorf("unable to split page at index, %v: failed to update right bloom filter: %w", index, err)
	}

	o.pages = append(o.pages, nil)
	o.filters = append(o.filters, nil)
	for i := len(o.pages) - 1; i > pageIndex; i-- {
		o.pages[i] = o.pages[i-1]
		o.filters[i] = o.filters[i-1]
	}

	o.pages[pageIndex] = left
	o.filters[pageIndex] = leftFilter

	o.pages[pageIndex+1] = right
	o.filters[pageIndex+1] = rightFilter

	return nil
}

func (o *Object) NextValue(token ValueToken) (ValueToken, error) {
	page := o.pages[token.pageIndex]
	pvToken, err := page.NextValue(token.PageValueToken)
	if err != nil {
		if token.pageIndex+1 >= len(o.pages) || !errors.Is(err, io.EOF) {
			return ValueToken{}, err
		}

		token.pageIndex++ // advance to next page
		page = o.pages[token.pageIndex]
		pvToken, err = page.NextValue(PageValueToken{})
		if err != nil {
			return ValueToken{}, err
		}
	}

	return ValueToken{
		PageValueToken: pvToken,
		pageIndex:      token.pageIndex,
	}, nil
}

func (o *Object) Insert(op Op) error {
	pageIndex, opIndex, err := o.findPageIndex(op.Ref)
	if err != nil {
		return fmt.Errorf("unable to find page with id (%v,%v): %w", op.Ref.Counter, op.Ref.Actor, err)
	}

	page := o.pages[pageIndex]

	if err := page.InsertAt(opIndex+1, op); err != nil {
		return err
	}

	key := makeBloomKey(op.ID.Counter, op.ID.Actor)
	defer key.Free()
	filter := o.filters[pageIndex]
	filter.Add(key.data)

	o.last.Filter = filter
	o.last.ID = op.ID
	o.last.Index = opIndex + 1
	o.last.PageIndex = pageIndex
	o.last.Ok = true

	if page.rowCount >= o.options.MaxPageSize {
		// when pages exceed optimal size, split them in half.  splitting the pages in half will
		// require recalculating the bloom filter for each of the resulting pages.
		// todo - consider algorithms to split on other boundaries

		splitAtIndex := o.options.MaxPageSize / 2
		if err := o.splitPageAt(pageIndex, splitAtIndex); err != nil {
			return err
		}

		o.last.Ok = false // things got rearranged after page split
	}

	return nil
}

func (o *Object) RowCount() (n int64) {
	for _, p := range o.pages {
		n += p.rowCount
	}
	return
}

func (o *Object) Size() int {
	var size int
	for _, p := range o.pages {
		size += p.Size()
	}
	return size
}
