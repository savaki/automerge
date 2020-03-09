package automerge

import (
	"github.com/savaki/automerge/encoding"
)

const (
	TextInsert = 0
	TextDelete = 1
)

const (
	defaultMaxNodeSize = 100
)

type ropeNode struct {
	weight      int
	offset      int
	deletes     int
	content     []rune
	left, right *ropeNode
}

func (t *ropeNode) InsertAt(loc int, r rune, isDelete bool) {
	switch {
	case loc < t.offset:
	}
	if loc < t.offset {
		t.left.InsertAt(loc, r, isDelete)
		return
	}
}

type Text struct {
	maxNodeSize int
	obj         *Object
	tree        *ropeNode
}

func NewText(opts ...ObjectOption) *Text {
	return &Text{
		maxNodeSize: defaultMaxNodeSize,
		obj:         NewObject(encoding.RawTypeVarInt, opts...),
		tree:        &ropeNode{},
	}
}

func (t *Text) Apply(op Op) error {
	_, err := t.obj.Apply(op)
	if err != nil {
		return err
	}

	return err
}

func (t *Text) InsertAt(rr ...rune) error {
	actor := []byte("me")
	for i, r := range rr {
		counter := int64(i)
		ref := actor
		if i == 0 {
			ref = nil
		}

		op := Op{
			ID: ID{
				Counter: counter + 1,
				Actor:   actor,
			},
			Ref: ID{
				Counter: counter,
				Actor:   ref,
			},
			Type:  TextInsert,
			Value: encoding.RuneValue(r),
		}
		if err := t.Apply(op); err != nil {
			return err
		}
	}
	return nil
}

func (t *Text) RowCount() int64 {
	return t.obj.RowCount()
}

func (t *Text) Size() int {
	return t.obj.Size()
}
