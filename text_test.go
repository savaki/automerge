package automerge

import "testing"

func TestText_Apply(t *testing.T) {
	text := NewText()
	err := text.InsertAt('h', 'e', 'l', 'l', 'o')
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}
}
