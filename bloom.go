package automerge

import (
	"encoding/binary"
	"errors"
	"io"
	"sync"

	"github.com/willf/bloom"
)

var pool = &sync.Pool{
	New: func() interface{} {
		return &bloomKey{data: make([]byte, 0, 16)}
	},
}

type bloomKey struct {
	data []byte
}

func (k *bloomKey) Free() {
	k.data = k.data[0:0]
	pool.Put(k)
}

func makeBloomKey(counter int64, actor []byte) *bloomKey {
	key := pool.Get().(*bloomKey)

	var buf [binary.MaxVarintLen64]byte
	length := binary.PutVarint(buf[:], counter)

	key.data = append(key.data, buf[0:length]...)
	key.data = append(key.data, actor...)
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
			filter.Add(key.data)
			key.Free()
		}
	}
	return filter, nil
}
