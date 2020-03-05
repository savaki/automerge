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
