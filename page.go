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
	"bytes"
	"fmt"

	"github.com/savaki/automerge/encoding"
)

type Page struct {
	opCounter  *encoding.Delta
	opActor    *encoding.DictionaryRLE
	refCounter *encoding.Delta
	refActor   *encoding.DictionaryRLE
	opType     *encoding.RLE
	value      *encoding.Plain

	rowCount int64
}

type IDToken struct {
	counterToken encoding.DeltaToken
	actorToken   encoding.DictionaryRLEToken
	Counter      int64
	Actor        []byte
}

type PageToken struct {
	Op Op
}

type Op struct {
	OpCounter  int64
	OpActor    []byte
	RefCounter int64
	RefActor   []byte
	Type       int64
	Value      encoding.Value
}

func NewPage(rawType encoding.RawType) *Page {
	return &Page{
		opCounter:  encoding.NewDelta(nil),
		opActor:    encoding.NewDictionaryRLE(nil, nil),
		refCounter: encoding.NewDelta(nil),
		refActor:   encoding.NewDictionaryRLE(nil, nil),
		opType:     encoding.NewRLE(nil),
		value:      encoding.NewPlain(rawType, nil),
	}
}

func (p *Page) FindIndex(counter int64, actor []byte) (int64, error) {
	if actor == nil {
		return 0, nil
	}

	var i int64
	var token IDToken
	var err error
	for {
		token, err = p.NextID(token)
		if err != nil {
			return 0, err
		}

		if token.Counter == counter && bytes.Equal(token.Actor, actor) {
			return i, nil
		}

		i++
	}
}

func (p *Page) NextID(token IDToken) (IDToken, error) {
	counterToken, err := p.opCounter.Next(token.counterToken)
	if err != nil {
		return IDToken{}, err
	}

	actorToken, err := p.opActor.Next(token.actorToken)
	if err != nil {
		return IDToken{}, err
	}

	return IDToken{
		counterToken: counterToken,
		actorToken:   actorToken,
		Counter:      counterToken.Value,
		Actor:        actorToken.Value,
	}, nil
}

func (p *Page) InsertAt(index int64, op Op) error {
	if err := p.opCounter.InsertAt(index, op.OpCounter); err != nil {
		return fmt.Errorf("unable to insert op counter: %w", err)
	}
	if err := p.opActor.InsertAt(index, op.OpActor); err != nil {
		return fmt.Errorf("unable to insert op actor: %w", err)
	}
	if err := p.refCounter.InsertAt(index, op.RefCounter); err != nil {
		return fmt.Errorf("unable to insert ref counter: %w", err)
	}
	if err := p.refActor.InsertAt(index, op.RefActor); err != nil {
		return fmt.Errorf("unable to insert ref actor: %w", err)
	}
	if err := p.opType.InsertAt(index, op.Type); err != nil {
		return fmt.Errorf("unable to insert op type: %w", err)
	}
	if err := p.value.InsertAt(index, op.Value); err != nil {
		return fmt.Errorf("unable to insert value: %w", err)
	}

	p.rowCount++

	return nil
}

func (p *Page) SplitAt(index int64) (left, right *Page, err error) {
	lp := &Page{rowCount: index}
	rp := &Page{rowCount: p.rowCount - index}

	{
		left, right, err := p.opCounter.SplitAt(index)
		if err != nil {
			return nil, nil, fmt.Errorf("split at failed: op counter split failed: %w", err)
		}
		lp.opCounter = left
		rp.opCounter = right
	}
	{
		left, right, err := p.opActor.SplitAt(index)
		if err != nil {
			return nil, nil, fmt.Errorf("split at failed: op actor split failed: %w", err)
		}
		lp.opActor = left
		rp.opActor = right
	}
	{
		left, right, err := p.refCounter.SplitAt(index)
		if err != nil {
			return nil, nil, fmt.Errorf("split at failed: ref counter split failed: %w", err)
		}
		lp.refCounter = left
		rp.refCounter = right
	}
	{
		left, right, err := p.refActor.SplitAt(index)
		if err != nil {
			return nil, nil, fmt.Errorf("split at failed: ref actor split failed: %w", err)
		}
		lp.refActor = left
		rp.refActor = right
	}
	{
		left, right, err := p.opType.SplitAt(index)
		if err != nil {
			return nil, nil, fmt.Errorf("split at failed: op type split failed: %w", err)
		}
		lp.opType = left
		rp.opType = right
	}
	{
		left, right, err := p.value.SplitAt(index)
		if err != nil {
			return nil, nil, fmt.Errorf("split at failed: value split failed: %w", err)
		}
		lp.value = left
		rp.value = right
	}

	return lp, rp, nil
}

func (p *Page) Size() int {
	return p.opCounter.Size() + p.opActor.Size() + p.refCounter.Size() + p.refActor.Size() + p.opType.Size() + p.value.Size()
}
