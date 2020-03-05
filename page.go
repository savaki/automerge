package automerge

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/savaki/automerge/encoding"
	"github.com/willf/bloom"
	"io"
)

const (
	bloomM = 15000
	bloomK = 8
)

type Page struct {
	opCounter  *encoding.Delta
	opActor    *encoding.DictionaryRLE
	refCounter *encoding.Delta
	refActor   *encoding.DictionaryRLE
	opType     *encoding.RLE
	value      *encoding.Plain

	filter   *bloom.BloomFilter
	rowCount int
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
		filter:     bloom.New(bloomM, bloomK),
	}
}

func (p *Page) init() error {
	var token IDToken
	var err error
	for {
		token, err = p.NextID(token)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		data := makeBloomKey(token.Counter, token.Actor)
		p.filter.Add(data[:])
	}
}

func (p *Page) Contains(key [40]byte) bool {
	return p.filter.Test(key[:])
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
	key := makeBloomKey(op.OpCounter, op.OpActor)
	p.filter.Add(key[:])

	return nil
}

func (p *Page) SplitAt(index int64) (left, right *Page, err error) {
	lp := &Page{
		filter:   bloom.New(bloomM, bloomK),
		rowCount: int(index),
	}
	rp := &Page{
		filter:   bloom.New(bloomM, bloomK),
		rowCount: p.rowCount - int(index),
	}

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

	if err := lp.init(); err != nil {
		return nil, nil, err
	}
	if err := rp.init(); err != nil {
		return nil, nil, err
	}

	return lp, rp, nil
}

func (p *Page) Size() int {
	return p.opCounter.Size() + p.opActor.Size() + p.refCounter.Size() + p.refActor.Size() + p.opType.Size() + p.value.Size()
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
