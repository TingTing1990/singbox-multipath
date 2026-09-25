package stream

import (
	"errors"
	"fmt"
	"testing"
	"time"
	"unsafe"
)

type recordLedger struct {
	used, peak, limit int64
}

func (m *recordLedger) AcquireRecords(bytes int64, _ bool) bool {
	if bytes < 0 || bytes > m.limit-m.used {
		return false
	}
	m.used += bytes
	m.peak = max(m.peak, m.used)
	return true
}

func (m *recordLedger) ReleaseRecords(bytes int64) {
	if bytes < 0 || bytes > m.used {
		panic("record storage released twice or without ownership")
	}
	m.used -= bytes
}

func TestRecordStorageSenderAccounting(t *testing.T) {
	// Boundaries include the inline reserve, consecutive growths and large bursts.
	for _, count := range []int{1, 16, 17, 31, 32, 33, 255, 1025, 4097, 20000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			ledger := &recordLedger{limit: 8 << 20}
			s := NewSender(uint64(count+1), ledger)
			released := 0
			for i := 0; i < count; i++ {
				b := NewBuffer([]byte{byte(i)}, func() { released++ })
				if err := s.Append(b); err != nil {
					t.Fatal(err)
				}
				segment, ok := s.NextRange(1)
				if !ok || s.Sent(segment) != nil {
					t.Fatal("could not submit retained data")
				}
			}
			if count > 16 && ledger.used != int64(cap(s.segments))*int64(unsafe.Sizeof(Segment{})) {
				t.Fatalf("capacity not charged: cap=%d used=%d", cap(s.segments), ledger.used)
			}
			if err := s.Acknowledge(uint64(count/2), uint64(count+1)); err != nil {
				t.Fatal(err)
			}
			for i := count / 2; i < count; i++ {
				segment, ok := s.Range(uint64(i), 1)
				if !ok || segment.Data()[0] != byte(i) {
					t.Fatalf("partial ACK corrupted byte %d", i)
				}
			}
			if err := s.Acknowledge(uint64(count), uint64(count+1)); err != nil {
				t.Fatal(err)
			}
			if ledger.used != 0 || cap(s.segments) > 16 || released != count {
				t.Fatalf("idle retained uncharged storage: ledger=%+v cap=%d released=%d", ledger, cap(s.segments), released)
			}
			// Reuse after returning a large allocation must keep buffer ownership.
			if err := s.Append(NewBuffer([]byte{9}, func() { released++ })); err != nil {
				t.Fatal(err)
			}
			s.Close()
			s.Close()
			if ledger.used != 0 || released != count+1 {
				t.Fatal("close/reuse leaked or double-released storage")
			}
		})
	}
}

func TestRecordStorageGrowthChargesBothAllocations(t *testing.T) {
	ledger := &recordLedger{limit: 1 << 20}
	s := NewSender(1000, ledger)
	defer s.Close()
	for i := 0; i < 33; i++ {
		if err := s.Append(NewBuffer([]byte{1}, nil)); err != nil {
			t.Fatal(err)
		}
	}
	size := int64(unsafe.Sizeof(Segment{}))
	if ledger.used != 64*size || ledger.peak != (32+64)*size {
		t.Fatalf("replacement peak undercharged: %+v", ledger)
	}
}

func TestRecordStorageDeniedGrowthKeepsCallerOwnership(t *testing.T) {
	ledger := &recordLedger{}
	s := NewSender(1000, ledger)
	defer s.Close()
	for i := 0; i < 16; i++ {
		if err := s.Append(NewBuffer([]byte{byte(i)}, nil)); err != nil {
			t.Fatal(err)
		}
	}
	released := 0
	pending := NewBuffer([]byte{77}, func() { released++ })
	if err := s.Append(pending); !errors.Is(err, ErrRecordMemory) {
		t.Fatalf("growth without budget: %v", err)
	}
	if released != 0 || s.WriteNext != 16 || ledger.used != 0 {
		t.Fatal("denied append consumed bytes or caller ownership")
	}
	ledger.limit = 1 << 20
	if err := s.Append(pending); err != nil {
		t.Fatal(err)
	}
	s.Close()
	if released != 1 || ledger.used != 0 {
		t.Fatal("retry did not retain/release the same buffer exactly once")
	}
}

func TestRecordStoragePathEmergencyReserve(t *testing.T) {
	ledger := &recordLedger{}
	paid, prepaid := 0, 0
	p := Path{Generation: 7, RecordMemory: ledger, RecordPrimary: true}
	p.ReleaseFlight = func(reserved bool) {
		if reserved {
			prepaid++
		} else {
			paid++
		}
	}
	now := time.Now()
	for i := 0; i < 16; i++ {
		if _, err := p.Submitted(1, now); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := p.Submitted(1, now); !errors.Is(err, ErrRecordMemory) || p.Sent != 16 {
		t.Fatalf("ordinary submissions consumed emergency slots: %v", err)
	}
	for i := 0; i < 16; i++ {
		if _, err := p.Submitted(1, now, true); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := p.Submitted(1, now, true); !errors.Is(err, ErrRecordMemory) || p.Sent != 32 {
		t.Fatalf("emergency reserve was not bounded: %v", err)
	}
	if err := p.Feedback(Receipt{Generation: 7, Next: 32}, now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	p.Close()
	p.Close()
	if paid != 16 || prepaid != 16 || ledger.used != 0 {
		t.Fatalf("flight release mismatch: paid=%d prepaid=%d ledger=%+v", paid, prepaid, ledger)
	}
}

func TestRecordStoragePathReceiptLifecycle(t *testing.T) {
	ledger := &recordLedger{limit: 8 << 20}
	released := 0
	p := Path{Generation: 3, RecordMemory: ledger, RecordPrimary: true, ReleaseFlight: func(bool) { released++ }}
	now := time.Now()
	for i := 0; i < 20000; i++ {
		if _, err := p.Submitted(1, now); err != nil {
			t.Fatal(err)
		}
	}
	if ledger.used != int64(cap(p.flights))*int64(unsafe.Sizeof(Flight{})) {
		t.Fatal("flight capacity not charged")
	}
	before := ledger.used
	if err := p.Feedback(Receipt{Generation: 2, Next: 20000}, now); err != nil {
		t.Fatal(err)
	}
	if ledger.used != before || released != 0 {
		t.Fatal("old generation released current storage")
	}
	if err := p.Feedback(Receipt{Generation: 3, Next: 10000}, now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if ledger.used == 0 || released != 10000 {
		t.Fatal("partial receipt released live capacity")
	}
	if err := p.Feedback(Receipt{Generation: 3, Next: 20000}, now.Add(2*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if ledger.used != 0 || cap(p.flights) > 32 || released != 20000 {
		t.Fatal("receipt retained unpaid flight array")
	}
	p.Close()
}

// Regression for the staged-ACK blind spot found by the cross-candidate audit.
// A prior compaction must not prevent the final one-byte tail from returning to
// the prepaid record array and releasing the dynamic-capacity charge.
func TestRecordStorageSenderStagedACKTail(t *testing.T) {
	ledger := &recordLedger{limit: 8 << 20}
	s := NewSender(20001, ledger)
	for i := 0; i < 20000; i++ {
		if err := s.Append(NewBuffer([]byte{byte(i)}, nil)); err != nil {
			t.Fatal(err)
		}
		segment, ok := s.NextRange(1)
		if !ok || s.Sent(segment) != nil {
			t.Fatal("could not submit staged-ACK fixture")
		}
	}
	if ledger.used == 0 || cap(s.segments) < 10000 {
		t.Fatalf("fixture did not build dynamic high-water storage: used=%d cap=%d", ledger.used, cap(s.segments))
	}
	for _, next := range []uint64{17000, 19000, 19999} {
		if err := s.Acknowledge(next, 20001); err != nil {
			t.Fatal(err)
		}
	}
	if s.Buffered() != 1 {
		t.Fatalf("unexpected staged tail: %d", s.Buffered())
	}
	if ledger.used != 0 || cap(s.segments) > RecordReserve {
		t.Fatalf("staged ACK retained dynamic storage: used=%d cap=%d", ledger.used, cap(s.segments))
	}
	s.Close()
}
