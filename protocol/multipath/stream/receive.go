// Package stream implements the connection-level byte sequence space. It has
// no transport, timer, goroutine, or path-activation policy. Callers serialize
// state transitions; returned readable bytes remain immutable until Consume.
package stream

import (
	"errors"
	"math"
	"math/bits"
)

const (
	PageSize = 16 << 10
	// Charge the payload, the presence bitmap, and the page/map bookkeeping.
	PageCharge = PageSize + PageSize/8 + 256
	// Keep a small per-receiver physical page working set charged and reusable.
	// This prevents steady traffic from turning every consumed 16 KiB page into
	// GC garbage while preserving the global PageMemory admission budget.
	receivePageReuseLimit = 8
)

var (
	ErrSequence = errors.New("invalid multipath byte sequence")
	ErrWindow   = errors.New("multipath data exceeds receive window")
	ErrFIN      = errors.New("inconsistent multipath data FIN")
)

// PageMemory is an admission interface, not a blocking allocator. Acquire must
// not wait: a missing head mapping and its controls may be behind this frame on
// the same reliable transport. A receiver can evict only unacknowledged pages.
// The caller reserves at least one head page independently of speculative data.
type PageMemory interface {
	Acquire(head bool) bool
	Release(head bool)
}

type receivePage struct {
	data    [PageSize]byte
	present [PageSize / 64]uint64
	count   int
	head    bool
}

// Receiver separates received-prefix acknowledgement from application reads.
// Its sparse, byte-addressed pages bound metadata even for one-byte frames.
// Individual path receipts do not belong to this state machine.
type Receiver struct {
	Next          uint64
	ReadNext      uint64
	WindowEnd     uint64
	Capacity      uint64
	FIN           uint64
	HasFIN        bool
	Pruned        uint64
	Dropped       uint64
	buffered      uint64
	pages         map[uint64]*receivePage
	freeHeadPages []*receivePage
	freePages     []*receivePage
	memory        PageMemory
}

func NewReceiver(capacity uint64, memory PageMemory) *Receiver {
	if capacity == 0 {
		capacity = PageSize
	}
	return &Receiver{WindowEnd: capacity, Capacity: capacity, pages: make(map[uint64]*receivePage), memory: memory}
}

func (r *Receiver) Ack() uint64 {
	if r.HasFIN && r.Next == r.FIN {
		return r.Next + 1
	}
	return r.Next
}

func (r *Receiver) Complete() bool { return r.HasFIN && r.Next == r.FIN }
func (r *Receiver) EOF() bool      { return r.HasFIN && r.ReadNext == r.FIN }

func (r *Receiver) SetFIN(seq uint64) error {
	if seq == math.MaxUint64 || seq < r.Next || seq >= r.WindowEnd || r.HasFIN && seq != r.FIN {
		return ErrFIN
	}
	for id, page := range r.pages {
		start := id * PageSize
		if start >= seq {
			if page.count != 0 {
				return ErrFIN
			}
			continue
		}
		if start+PageSize > seq {
			for offset := int(seq - start); offset < PageSize; {
				index, bit := offset/64, offset%64
				if page.present[index]&(math.MaxUint64<<bit) != 0 {
					return ErrFIN
				}
				offset = (index + 1) * 64
			}
		}
	}
	r.FIN, r.HasFIN = seq, true
	return nil
}

// Insert accepts only bytes not already received. A duplicate never overwrites
// data, including out-of-order data. Admission failures leave holes, not a false
// acknowledgement; the sender still owns every byte beyond Ack().
func (r *Receiver) Insert(seq uint64, data []byte) (int, error) {
	if len(data) == 0 || uint64(len(data)) > math.MaxUint64-seq {
		return 0, ErrSequence
	}
	end := seq + uint64(len(data))
	if r.HasFIN && end > r.FIN {
		return 0, ErrFIN
	}
	if end > r.WindowEnd {
		return 0, ErrWindow
	}
	if end <= r.Next {
		return 0, nil
	}
	if seq < r.Next {
		data = data[r.Next-seq:]
		seq = r.Next
	}
	accepted := 0
	for len(data) > 0 {
		id, offset := seq/PageSize, int(seq%PageSize)
		length := min(len(data), PageSize-offset)
		page := r.pages[id]
		if page == nil {
			head := id == r.Next/PageSize
			page = r.takeReusablePage(head)
			if page == nil {
				admitted := r.memory == nil || r.memory.Acquire(head)
				if !admitted && head {
					// Under pressure, repurpose the farthest speculative page as
					// the missing head. Its ordinary memory charge remains live,
					// so no Release->new allocation GC churn is introduced.
					page = r.pruneTailForHead(id)
				}
				if admitted && page == nil {
					page = &receivePage{head: head}
				}
			}
			if page != nil {
				r.pages[id] = page
			}
		}
		if page == nil {
			r.Dropped += uint64(length)
		} else {
			added := page.insert(offset, data[:length])
			accepted += added
			r.buffered += uint64(added)
		}
		seq += uint64(length)
		data = data[length:]
		r.advance()
	}
	return accepted, nil
}

func resetReceivePage(page *receivePage) {
	page.present = [PageSize / 64]uint64{}
	page.count = 0
}

func (r *Receiver) takeReusablePage(head bool) *receivePage {
	var page *receivePage
	if head {
		if n := len(r.freeHeadPages); n > 0 {
			page = r.freeHeadPages[n-1]
			r.freeHeadPages[n-1] = nil
			r.freeHeadPages = r.freeHeadPages[:n-1]
		}
	} else if n := len(r.freePages); n > 0 {
		page = r.freePages[n-1]
		r.freePages[n-1] = nil
		r.freePages = r.freePages[:n-1]
	}
	if page != nil {
		resetReceivePage(page)
	}
	return page
}

func (r *Receiver) recyclePage(page *receivePage) {
	if page == nil {
		return
	}
	resetReceivePage(page)
	if page.head {
		if len(r.freeHeadPages) < receivePageReuseLimit {
			r.freeHeadPages = append(r.freeHeadPages, page)
			return
		}
	} else if len(r.freePages) < receivePageReuseLimit {
		r.freePages = append(r.freePages, page)
		return
	}
	if r.memory != nil {
		r.memory.Release(page.head)
	}
}

func (p *receivePage) insert(offset int, data []byte) int {
	added := 0
	for len(data) > 0 {
		word, bit := offset/64, offset%64
		n := min(len(data), 64-bit)
		mask := (uint64(1)<<n - 1) << bit
		if n == 64 {
			mask = math.MaxUint64
		}
		fresh := mask &^ p.present[word]
		if fresh == mask {
			copy(p.data[offset:offset+n], data[:n])
			added += n
		} else if fresh != 0 {
			for todo := fresh; todo != 0; todo &= todo - 1 {
				index := bits.TrailingZeros64(todo) - bit
				p.data[offset+index] = data[index]
				added++
			}
		}
		p.present[word] |= mask
		offset += n
		data = data[n:]
	}
	p.count += added
	return added
}

func (r *Receiver) advance() {
	for {
		page := r.pages[r.Next/PageSize]
		if page == nil {
			return
		}
		offset := int(r.Next % PageSize)
		if page.count == PageSize {
			r.Next += uint64(PageSize - offset)
			continue
		}
		word, bit := offset/64, offset%64
		missing := ^page.present[word] & (math.MaxUint64 << bit)
		if missing != 0 {
			r.Next += uint64(bits.TrailingZeros64(missing) - bit)
			return
		}
		r.Next += uint64(64 - bit)
	}
}

// Readable returns at most one page. Insert will not modify the returned range;
// its presence bits are already set. Consume is called only after the reader
// finishes using this slice (including partial writes to the application).
func (r *Receiver) Readable() []byte {
	if r.ReadNext == r.Next {
		return nil
	}
	page := r.pages[r.ReadNext/PageSize]
	if page == nil {
		panic("multipath: acknowledged receive page lost")
	}
	offset := int(r.ReadNext % PageSize)
	n := int(min(uint64(PageSize-offset), r.Next-r.ReadNext))
	return page.data[offset : offset+n]
}

func (r *Receiver) Consume(length int) {
	if length < 0 || uint64(length) > r.Next-r.ReadNext {
		panic("multipath: invalid receive consumption")
	}
	old := r.ReadNext
	r.ReadNext += uint64(length)
	r.buffered -= uint64(length)
	for id := old / PageSize; id < r.ReadNext/PageSize; id++ {
		if page := r.pages[id]; page != nil {
			delete(r.pages, id)
			r.recyclePage(page)
		}
	}
}

// Advertise slides an existing window only while memory permits. Freezing the
// right edge under pressure never retracts previously advertised capacity.
func (r *Receiver) Advertise(grow bool) uint64 {
	capacity := r.Capacity
	if !grow {
		capacity = min(capacity, PageSize)
	}
	end := uint64(math.MaxUint64)
	if capacity <= math.MaxUint64-r.ReadNext {
		end = r.ReadNext + capacity
	}
	r.WindowEnd = max(r.WindowEnd, end)
	return r.WindowEnd
}

func (r *Receiver) pruneTailForHead(keep uint64) *receivePage {
	var last uint64
	found := false
	for id := range r.pages {
		if id != keep && id*PageSize >= r.Next && (!found || id > last) {
			last, found = id, true
		}
	}
	if !found {
		return nil
	}
	page := r.pages[last]
	r.Pruned += uint64(page.count)
	r.buffered -= uint64(page.count)
	delete(r.pages, last)
	// Keep the original ordinary-page charge and head flag. A charged
	// speculative page is safe to use for head progress; it is conservative
	// compared with consuming the prepaid head reservation.
	resetReceivePage(page)
	return page
}

func (r *Receiver) Buffered() (bytes, outOfOrder uint64, pages int) {
	if len(r.pages) == 0 {
		return 0, 0, 0
	}
	bytes = r.buffered
	outOfOrder = bytes - (r.Next - r.ReadNext)
	pages = len(r.pages) - int(r.Next/PageSize-r.ReadNext/PageSize)
	if page := r.pages[r.Next/PageSize]; page != nil && page.count == int(r.Next%PageSize) {
		pages--
	}
	return bytes, outOfOrder, pages
}

func (r *Receiver) Close() {
	for id, page := range r.pages {
		delete(r.pages, id)
		if r.memory != nil {
			r.memory.Release(page.head)
		}
	}
	if r.memory != nil {
		for _, page := range r.freeHeadPages {
			r.memory.Release(page.head)
		}
		for _, page := range r.freePages {
			r.memory.Release(page.head)
		}
	}
	r.freeHeadPages = nil
	r.freePages = nil
	r.pages = nil
	r.buffered = 0
}
