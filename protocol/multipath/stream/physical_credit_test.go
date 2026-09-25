package stream

import "testing"

// Regression for the confirmed release-order bug: Sender must clear its heap
// ownership and Buffer must clear Data before the memory callback can hand the
// backing allocation to a GC batch.
func TestManagedBufferDetachPrecedesReleaseCallback(t *testing.T) {
	s := NewSender(1 << 20)
	var buffer *Buffer
	called := false
	buffer = NewManagedBuffer(make([]byte, 1<<20), func(data []byte) func() {
		called = true
		if len(data) != 1<<20 {
			t.Fatalf("detached payload changed size: %d", len(data))
		}
		if buffer.Data != nil {
			t.Fatal("Buffer still references backing during release callback")
		}
		for i := range s.segments {
			if s.segments[i].Buffer == buffer {
				t.Fatal("Sender still references Buffer during release callback")
			}
		}
		return nil
	})
	if err := s.Append(buffer); err != nil {
		t.Fatal(err)
	}
	segment, ok := s.NextRange(1 << 20)
	if !ok {
		t.Fatal("missing send range")
	}
	if err := s.Sent(segment); err != nil {
		t.Fatal(err)
	}
	if err := s.Acknowledge(s.Next, s.WindowEnd); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("managed release callback not called")
	}
}

type prepaidReuseMemory struct {
	used    int
	retired int
}

func (m *prepaidReuseMemory) Acquire(bool) bool { m.used++; return true }
func (m *prepaidReuseMemory) Release(head bool) bool {
	m.used--
	return head && m.used == 0
}
func (m *prepaidReuseMemory) Retire() { m.retired++ }

func TestPrepaidHeadPageReusesSameBacking(t *testing.T) {
	memory := &prepaidReuseMemory{}
	r := NewReceiver(4*PageSize, memory)
	payload := make([]byte, PageSize)
	if _, err := r.Insert(0, payload); err != nil {
		t.Fatal(err)
	}
	first := r.pages[0]
	if first == nil || !first.head {
		t.Fatal("first head page missing")
	}
	r.Consume(PageSize)
	if r.prepaidHead != first {
		t.Fatal("prepaid head backing was not retained")
	}
	if memory.retired != 0 {
		t.Fatal("prepaid head was sent to physical reclaim")
	}
	if _, err := r.Insert(PageSize, payload); err != nil {
		t.Fatal(err)
	}
	if r.pages[1] != first {
		t.Fatal("next head did not reuse prepaid backing")
	}
	r.Close()
}
