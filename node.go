package automerge

type node struct {
	left, right *node
	weight      int64
	deletes     int64
	page        *Page
	isDelete    func(int64) bool
	maxRowCount int64
}

func (n *node) insertLocal(index int64, op Op) error {
	switch n.deletes {
	case 0:
		if err := n.page.InsertAt(index, op); err != nil {
			return err
		}
	default:
		if err := n.page.InsertAtTranslated(index, op, n.isDelete); err != nil {
			return err
		}
	}

	if n.isDelete(op.Type) {
		n.deletes++
	}
	n.weight = n.page.rowCount - n.deletes<<1

	return nil
}

func (n *node) length() int64 {
	if n == nil {
		return 0
	}
	return n.left.length() + n.right.length() + n.weight - n.deletes<<1
}

func (n *node) InsertAt(index int64, op Op) error {
	switch {
	case n.left == nil || n.right == nil:
		return n.insertLocal(index, op)

	case index <= n.weight:
		if err := n.left.InsertAt(index-n.weight, op); err != nil {
			return err
		}
		n.weight = n.left.length()
		return nil

	default:
		return n.right.InsertAt(index-n.weight, op)
	}
}
