package stream

import "math"

// Buffer ownership is shared only by the connection send queue and active
// writers. ACK processing must not recycle bytes referenced by a blocked Write.
type Buffer struct {
	Data        []byte
	refs        int
	release     func()
	releaseData func([]byte) func()
}

func NewBuffer(data []byte, release func()) *Buffer {
	return &Buffer{Data: data, refs: 1, release: release}
}

// NewManagedBuffer is for memoryBudget-owned payloads. On the last reference,
// Buffer first removes its own backing reference, then hands the detached slice
// to the memory owner. Legacy NewBuffer callbacks remain unchanged for reserved
// buffers whose callback intentionally transfers ownership elsewhere.
func NewManagedBuffer(data []byte, release func([]byte) func()) *Buffer {
	return &Buffer{Data: data, refs: 1, releaseData: release}
}

func (b *Buffer) Retain() {
	if b.refs <= 0 {
		panic("multipath: retaining released buffer")
	}
	b.refs++
}
func (b *Buffer) Release() {
	if b.refs <= 0 {
		panic("multipath: releasing unowned buffer")
	}
	b.refs--
	if b.refs != 0 {
		return
	}
	data, releaseData, release := b.Data, b.releaseData, b.release
	b.Data, b.releaseData, b.release = nil, nil, nil
	if releaseData != nil {
		finish := releaseData(data)
		data = nil
		if finish != nil {
			finish()
		}
		return
	}
	if release != nil {
		release()
	}
}

type Segment struct {
	Seq    uint64
	Buffer *Buffer
	Offset int
	Length int
}

func (s Segment) End() uint64  { return s.Seq + uint64(s.Length) }
func (s Segment) Data() []byte { return s.Buffer.Data[s.Offset : s.Offset+s.Length] }

// Sender is the single connection-level retransmission queue. Path receipts
// deliberately have no API which removes its data.
type Sender struct {
	Una           uint64
	Next          uint64
	WriteNext     uint64
	WindowEnd     uint64
	FIN           uint64
	HasFIN        bool
	FINSent       bool
	FINAcked      bool
	segments      []Segment
	head          int
	recordMemory  RecordAllocator
	recordBytes   int64
	recordReserve [RecordReserve]Segment
}

func NewSender(initialWindow uint64, memory ...RecordAllocator) *Sender {
	s := &Sender{WindowEnd: initialWindow}
	if len(memory) > 0 {
		s.recordMemory = memory[0]
	}
	return s
}

// Append transfers the caller's one buffer reference on success only.
func (s *Sender) Append(buffer *Buffer) error {
	length := len(buffer.Data)
	if s.HasFIN || length == 0 || uint64(length) >= math.MaxUint64-s.WriteNext {
		return ErrSequence
	}
	if !GrowRecords(&s.segments, &s.head, &s.recordBytes, s.recordReserve[:], s.recordMemory, 1, true) {
		return ErrRecordMemory
	}
	s.segments = append(s.segments, Segment{Seq: s.WriteNext, Buffer: buffer, Length: length})
	s.WriteNext += uint64(length)
	return nil
}

func (s *Sender) CloseWrite() {
	if !s.HasFIN {
		s.FIN, s.HasFIN = s.WriteNext, true
	}
}

// Range borrows a subrange of one retained segment. A writer must Retain its
// buffer before releasing the connection lock and Release after Write returns.
func (s *Sender) Range(seq uint64, maxLength int) (Segment, bool) {
	if seq < s.Una || seq >= s.WriteNext || maxLength <= 0 {
		return Segment{}, false
	}
	lo, hi := s.head, len(s.segments)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if s.segments[mid].End() <= seq {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == len(s.segments) {
		return Segment{}, false
	}
	segment := s.segments[lo]
	if seq < segment.Seq {
		return Segment{}, false
	}
	offset := int(seq - segment.Seq)
	segment.Seq = seq
	segment.Offset += offset
	segment.Length = min(segment.Length-offset, maxLength)
	return segment, true
}

func (s *Sender) NextRange(maxLength int) (Segment, bool) {
	if s.Next >= s.WindowEnd {
		return Segment{}, false
	}
	return s.Range(s.Next, int(min(uint64(maxLength), s.WindowEnd-s.Next)))
}

func (s *Sender) Sent(segment Segment) error {
	if segment.Seq != s.Next || segment.Length <= 0 || segment.End() > s.WriteNext || segment.End() > s.WindowEnd {
		return ErrSequence
	}
	s.Next = segment.End()
	return nil
}

func (s *Sender) SendFIN() bool {
	if !s.HasFIN || s.FINSent || s.Next != s.FIN || s.Next >= s.WindowEnd {
		return false
	}
	s.FINSent = true
	s.Next++
	return true
}

// Acknowledge handles duplicate/reordered window updates without shrinking the
// right edge. Partial byte ACKs trim the queue without retaining frame indexes.
func (s *Sender) Acknowledge(next, windowEnd uint64) error {
	if next > s.Next || windowEnd < next {
		return ErrSequence
	}
	s.WindowEnd = max(s.WindowEnd, windowEnd)
	if next <= s.Una {
		return nil
	}
	s.Una = next
	for s.head < len(s.segments) {
		segment := &s.segments[s.head]
		if segment.Seq >= next {
			break
		}
		if segment.End() > next {
			consumed := int(next - segment.Seq)
			segment.Seq = next
			segment.Offset += consumed
			segment.Length -= consumed
			break
		}
		buffer := segment.Buffer
		*segment = Segment{}
		s.head++
		buffer.Release()
	}
	TrimRecords(&s.segments, &s.head, &s.recordBytes, s.recordReserve[:], s.recordMemory, 1024, 0)
	s.FINAcked = s.FINSent && next == s.FIN+1
	return nil
}

func (s *Sender) Buffered() uint64 {
	return s.WriteNext - min(s.Una, s.WriteNext)
}

func (s *Sender) Close() {
	for i := s.head; i < len(s.segments); i++ {
		buffer := s.segments[i].Buffer
		s.segments[i] = Segment{}
		if buffer != nil {
			buffer.Release()
		}
	}
	CloseRecords(&s.segments, &s.head, &s.recordBytes, s.recordReserve[:], s.recordMemory)
}
