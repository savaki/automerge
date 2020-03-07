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

type ID struct {
	Counter int64
	Actor   []byte
}

func (i ID) Equal(that ID) bool {
	return i.Counter == that.Counter && bytes.Equal(i.Actor, that.Actor)
}

func NewID(counter int64, actor []byte) ID {
	return ID{
		Counter: counter,
		Actor:   actor,
	}
}

type Page struct {
	counter    *encoding.Delta
	actor      *encoding.DictionaryRLE
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

type PageValueToken struct {
	opTypeToken encoding.RLEToken
	valueToken  encoding.PlainToken
	OpType      int64
	Value       encoding.Value
}

type PageToken struct {
	Op Op
}

type Op struct {
	ID    ID
	Ref   ID
	Type  int64
	Value encoding.Value
}

func NewPage(rawType encoding.RawType) *Page {
	return &Page{
		counter:    encoding.NewDelta(nil),
		actor:      encoding.NewDictionaryRLE(nil, nil),
		refCounter: encoding.NewDelta(nil),
		refActor:   encoding.NewDictionaryRLE(nil, nil),
		opType:     encoding.NewRLE(nil),
		value:      encoding.NewPlain(rawType, nil),
	}
}

func (p *Page) FindIndex(counter int64, actor []byte) (int64, error) {
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
	counterToken, err := p.counter.Next(token.counterToken)
	if err != nil {
		return IDToken{}, err
	}

	actorToken, err := p.actor.Next(token.actorToken)
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

func (p *Page) NextValue(token PageValueToken) (PageValueToken, error) {
	opTypeToken, err := p.opType.Next(token.opTypeToken)
	if err != nil {
		return PageValueToken{}, err
	}

	valueToken, err := p.value.Next(token.valueToken)
	if err != nil {
		return PageValueToken{}, err
	}

	return PageValueToken{
		opTypeToken: opTypeToken,
		valueToken:  valueToken,
		OpType:      opTypeToken.Value,
		Value:       valueToken.Value,
	}, nil
}

func (p *Page) InsertAt(index int64, op Op) error {
	if err := p.counter.InsertAt(index, op.ID.Counter); err != nil {
		return fmt.Errorf("unable to insert op counter: %w", err)
	}
	if err := p.actor.InsertAt(index, op.ID.Actor); err != nil {
		return fmt.Errorf("unable to insert op actor: %w", err)
	}
	if err := p.refCounter.InsertAt(index, op.Ref.Counter); err != nil {
		return fmt.Errorf("unable to insert ref counter: %w", err)
	}
	if err := p.refActor.InsertAt(index, op.Ref.Actor); err != nil {
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
		left, right, err := p.counter.SplitAt(index)
		if err != nil {
			return nil, nil, fmt.Errorf("split at failed: op counter split failed: %w", err)
		}
		lp.counter = left
		rp.counter = right
	}
	{
		left, right, err := p.actor.SplitAt(index)
		if err != nil {
			return nil, nil, fmt.Errorf("split at failed: op actor split failed: %w", err)
		}
		lp.actor = left
		rp.actor = right
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
	return p.counter.Size() + p.actor.Size() + p.refCounter.Size() + p.refActor.Size() + p.opType.Size() + p.value.Size()
}
