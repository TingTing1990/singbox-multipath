package stream

import (
	"errors"
	"unsafe"
)

// RecordReserve is small, fixed storage paid for by the session reservation.
// Path storage additionally leaves room for the existing 16 prepaid flights.
const (
	RecordReserve     = 16
	PathRecordReserve = 2 * RecordReserve
	SenderRecordBytes = int64(RecordReserve) * int64(unsafe.Sizeof(Segment{}))
	PathRecordBytes   = int64(PathRecordReserve) * int64(unsafe.Sizeof(Flight{}))
)

var ErrRecordMemory = errors.New("multipath: record storage budget exhausted")

// RecordAllocator charges backing storage, independently of per-item charges.
// Calls are serialized by the connection lock. Acquisition must never wait.
type RecordAllocator interface {
	AcquireRecords(bytes int64, primary bool) bool
	ReleaseRecords(bytes int64)
}

// GrowRecords reserves the complete replacement allocation before making it.
// Both old and new arrays are charged during copying. Consumed prefixes are
// reused before growing; append must only follow a successful reservation.
func GrowRecords[T any](records *[]T, head *int, charged *int64, fixed []T, memory RecordAllocator, additional int, primary bool) bool {
	if additional <= cap(*records)-len(*records) {
		return true
	}
	live := len(*records) - *head
	if additional <= cap(*records)-live {
		n := copy(*records, (*records)[*head:])
		clear((*records)[n:])
		*records, *head = (*records)[:n], 0
		return true
	}
	if live+additional <= len(fixed) {
		n := copy(fixed, (*records)[*head:])
		clear(*records)
		oldCharge := *charged
		*records, *head, *charged = fixed[:n], 0, 0
		if memory != nil {
			memory.ReleaseRecords(oldCharge)
		}
		return true
	}
	capacity := max(live+additional, 2*cap(*records), len(fixed))
	var value T
	size := int64(unsafe.Sizeof(value))
	if size <= 0 || int64(capacity) > (1<<63-1)/size {
		return false
	}
	bytes := int64(capacity) * size
	if memory != nil && !memory.AcquireRecords(bytes, primary) {
		return false
	}
	replacement := make([]T, live, capacity)
	copy(replacement, (*records)[*head:])
	clear(*records)
	oldCharge := *charged
	*records, *head, *charged = replacement, 0, bytes
	if memory != nil {
		memory.ReleaseRecords(oldCharge)
	}
	return true
}

// TrimRecords returns small queues to their prepaid storage without allocating.
// Larger retained capacity stays charged, including slots in consumed prefixes.
// spare preserves the path's emergency flight slots when shrinking.
func TrimRecords[T any](records *[]T, head *int, charged *int64, fixed []T, memory RecordAllocator, compactAt, spare int) {
	live := len(*records) - *head
	if *charged > 0 && live+spare <= len(fixed) {
		n := copy(fixed, (*records)[*head:])
		clear(*records)
		oldCharge := *charged
		*records, *head, *charged = fixed[:n], 0, 0
		if memory != nil {
			memory.ReleaseRecords(oldCharge)
		}
	} else if live == 0 {
		*records, *head = (*records)[:0], 0
	} else if *head >= compactAt && *head >= live {
		n := copy(*records, (*records)[*head:])
		clear((*records)[n:])
		*records, *head = (*records)[:n], 0
	}
}

func CloseRecords[T any](records *[]T, head *int, charged *int64, fixed []T, memory RecordAllocator) {
	clear(*records)
	clear(fixed)
	oldCharge := *charged
	*records, *head, *charged = nil, 0, 0
	if memory != nil {
		memory.ReleaseRecords(oldCharge)
	}
}
