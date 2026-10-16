package stream

import (
	"bytes"
	"sync"
	"testing"
)

func TestS1HeadroomOriginalPayloadAndFallback(t *testing.T) {
	raw := make([]byte, 2048)
	for i := 0; i < 1500; i++ {
		raw[32+i] = byte(i % 251)
	}
	buf := NewBufferWithHeadroom(raw, 32, 1500, nil)
	golden := append([]byte(nil), buf.Data...)
	header, ok := buf.TakeHeadroom(buf.Data, 29)
	if !ok || len(header) != 1529 {
		t.Fatal("full frame fast path should be eligible")
	}
	if _, other := buf.TakeHeadroom(buf.Data, 29); other {
		t.Fatal("concurrent writer must fall back")
	}
	if _, partial := buf.TakeHeadroom(buf.Data[1:], 29); partial {
		t.Fatal("partial/repair range must fall back")
	}
	copy(header[:29], bytes.Repeat([]byte{0xb7}, 29))
	if !bytes.Equal(buf.Data, golden) {
		t.Fatal("header write touched payload")
	}
	buf.ReturnHeadroom()
	if _, ok := buf.TakeHeadroom(buf.Data, 29); !ok {
		t.Fatal("reuse after write must work")
	}
	buf.ReturnHeadroom()
}

func TestS1BufferRetainAndReleaseLifecycle(t *testing.T) {
	raw := make([]byte, 2048)
	var mu sync.Mutex
	released := 0
	buf := NewBufferWithHeadroom(raw, 32, 1024, func() { mu.Lock(); defer mu.Unlock(); released++ })
	buf.Retain()
	buf.Retain()
	buf.Release()
	buf.Release()
	if released != 0 {
		t.Fatal("released while writer still owns payload")
	}
	buf.Release()
	if released != 1 {
		t.Fatalf("release count %d != 1", released)
	}
}

func TestS1HeadroomConcurrentSingleBorrow(t *testing.T) {
	raw := make([]byte, 2048)
	buf := NewBufferWithHeadroom(raw, 32, 1024, nil)
	first, ok := buf.TakeHeadroom(buf.Data, 29)
	if !ok || len(first) != 1053 {
		t.Fatal("initial borrow failed")
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5000; i++ {
			if _, ok := buf.TakeHeadroom(buf.Data, 29); ok {
				t.Error("double borrow")
			}
		}
	}()
	wg.Wait()
	buf.ReturnHeadroom()
}

func TestS1ReservedFallbackUnchanged(t *testing.T) {
	reserved := make([]byte, 1024)
	buf := NewBuffer(reserved, nil)
	if _, ok := buf.TakeHeadroom(buf.Data, 29); ok {
		t.Fatal("reserved Golden fallback cannot use headroom")
	}
}
