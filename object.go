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
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/savaki/automerge/encoding"
	"github.com/willf/bloom"
)

const (
	defaultRowCount = 100
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

func (o *Object) findPageIndex(counter int64, actor []byte) (int, int64, error) {
	key := makeBloomKey(counter, actor)
	for i, p := range o.pages {
		if o.filters[i].Test(key[:]) || actor == nil {
			index, err := p.FindIndex(counter, actor)
			if err != nil {
				if err == io.EOF {
					break
				}
				return 0, 0, err
			}
			return i, index, nil
		}
	}
	return 0, 0, io.EOF
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
	pageIndex, opIndex, err := o.findPageIndex(op.RefCounter, op.RefActor)
	if err != nil {
		return fmt.Errorf("unable to find page with counter, %v, and actor, %v: %w", op.RefCounter, op.RefActor, err)
	}

	page := o.pages[pageIndex]

	if err := page.InsertAt(opIndex, op); err != nil {
		return fmt.Errorf("insert failed: %w", err)
	}

	key := makeBloomKey(op.Counter, op.Actor)
	o.filters[pageIndex].Add(key[:])

	if page.rowCount > o.options.MaxPageSize {
		// when pages exceed optimal size, split them in half.  splitting the pages in half will
		// require recalculating the bloom filter for each of the resulting pages.
		// todo - consider algorithms to split on other boundaries

		splitAtIndex := o.options.MaxPageSize / 2
		left, right, err := page.SplitAt(splitAtIndex)
		if err != nil {
			return fmt.Errorf("unable to insert record: failed to split page at index, %v: %w", splitAtIndex, err)
		}

		leftFilter, err := makeBloomFilter(o.options.Bloom, left)
		if err != nil {
			return fmt.Errorf("unable to split page at index, %v: failed to update left bloom filter: %w", splitAtIndex, err)
		}

		rightFilter, err := makeBloomFilter(o.options.Bloom, right)
		if err != nil {
			return fmt.Errorf("unable to split page at index, %v: failed to update right bloom filter: %w", splitAtIndex, err)
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

func makeBloomKey(counter int64, actor []byte) [40]byte {
	var key [40]byte
	offset := binary.PutVarint(key[:], counter)
	length := len(actor)
	if max := 40 - offset; length > max {
		length = max
	}
	copy(key[offset:], actor[0:length])
	return key
}

func makeBloomFilter(options bloomOptions, page *Page) (*bloom.BloomFilter, error) {
	filter := bloom.New(options.M, options.K)
	if page != nil {
		var token IDToken
		var err error
		for {
			token, err = page.NextID(token)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return nil, err
			}

			key := makeBloomKey(token.Counter, token.Actor)
			filter.Add(key[:])
		}
	}
	return filter, nil
}
