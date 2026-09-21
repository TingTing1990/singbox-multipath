package multipath

import (
	"context"
	"errors"
	"math"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common/byteformats"
)

const (
	automaticMemoryLimitCap      = 512 << 20
	automaticMemoryLimitFallback = 256 << 20
	memoryCacheLimitCap          = 16 << 20
	memoryIdleTrimDelay          = 2 * time.Second
	memoryMaintenanceInterval    = 250 * time.Millisecond
	sessionMemoryBase            = 128 << 10
	wireFrameMemoryEstimate      = int64(unsafe.Sizeof(wireFrame{}))
)

var (
	errMemoryLimit = errors.New("multipath memory limit reached")
	// Kept as a variable so memory tests can verify trim decisions without
	// forcing process-wide GC on every unit-test assertion. Production always
	// uses runtime/debug.FreeOSMemory.
	memoryScavenge = debug.FreeOSMemory
	// debug.FreeOSMemory is process-wide. Serialize it across all multipath
	// inbound/outbound budgets so simultaneous idle trims do not launch
	// overlapping forced-GC/scavenge cycles. Individual budget mutexes are never
	// held while this mutex or the runtime operation is active.
	memoryScavengeAccess sync.Mutex
)

type memorySnapshot struct {
	LimitBytes         int64
	UsedBytes          int64
	CachedBytes        int64
	ActiveBytes        int64
	BoosterLimitBytes  int64
	BoosterResumeBytes int64
	Automatic          bool
	Pressure           bool
	PressureSince      time.Time
	PressureEvents     uint64
	BackpressureEvents uint64
	PeakUsedBytes      int64
	PeakCachedBytes    int64
	FreshAllocations   uint64
	ReusedAllocations  uint64
	TrimEvents         uint64
	TrimmedBytes       uint64
}

type memoryPressureEvent struct {
	entered  bool
	snapshot memorySnapshot
	duration time.Duration
}

type memoryBudget struct {
	access   sync.Mutex
	sessions atomic.Int64

	limit         int64
	boosterLimit  int64
	boosterResume int64
	cacheLimit    int64
	automatic     bool

	// used is the total charged memory, including reusable TX/data slabs in
	// cache. A released slab stays charged until it is either reused or actually
	// dropped and scavenged. This is the key physical-credit invariant: a logical
	// Release must not immediately mint credit for another fresh make() while the
	// old backing array is still resident in the Go heap.
	used   int64
	cached int64
	cache  map[int][][]byte

	pressure      bool
	pressureSince time.Time
	pressureCount uint64
	waitCount     uint64
	peakUsed      int64
	peakCached    int64

	lastActivity      time.Time
	freshAllocations  uint64
	reusedAllocations uint64
	trimCount         uint64
	trimmedBytes      uint64
	pendingReclaim    int64
	garbageHintBytes  int64
	scavenging        bool

	changed chan struct{}
	events  chan memoryPressureEvent
	logOnce sync.Once
}

func resolveMemoryLimit(configured uint64) (int64, bool, error) {
	if configured > math.MaxInt64 {
		return 0, false, errors.New("memory_limit exceeds the supported range")
	}
	if configured > 0 {
		return int64(configured), false, nil
	}
	available, err := availableMemory()
	if err != nil || available < 2 {
		return automaticMemoryLimitFallback, true, err
	}
	return automaticMemoryLimit(available), true, nil
}

func automaticMemoryLimit(available uint64) int64 {
	limit := available / 2
	if limit > automaticMemoryLimitCap {
		limit = automaticMemoryLimitCap
	}
	return int64(limit)
}

func newMemoryBudget(limit int64, automatic bool) *memoryBudget {
	boosterLimit := limit/8*7 + limit%8*7/8
	boosterResume := limit/4*3 + limit%4*3/4
	cacheLimit := limit / 16
	if cacheLimit > memoryCacheLimitCap {
		cacheLimit = memoryCacheLimitCap
	}
	return &memoryBudget{
		limit:         limit,
		boosterLimit:  boosterLimit,
		boosterResume: boosterResume,
		cacheLimit:    cacheLimit,
		automatic:     automatic,
		cache:         make(map[int][][]byte),
		lastActivity:  time.Now(),
		changed:       make(chan struct{}),
		events:        make(chan memoryPressureEvent, 4),
	}
}

func sessionMemoryReservation(cfg coreConfig) int64 {
	// Fixed channels: one assignment, 32 controls and one coalesced feedback
	// frame per path. The base also covers worker stacks and 16 prepaid primary
	// flight records. Two reader scratch buffers and one primary TX reserve
	// keep head recovery independent of speculative allocations.
	return sessionMemoryBase + 2*34*wireFrameMemoryEstimate + int64(cfg.ChunkSize)*3
}

func (b *memoryBudget) activeBytesLocked() int64 {
	active := b.used - b.cached
	if active < 0 {
		return 0
	}
	return active
}

func (b *memoryBudget) noteActivityLocked(now time.Time) {
	b.lastActivity = now
}

// takeCachedLocked returns the smallest reusable slab whose capacity satisfies
// size. The slab remains charged in b.used; only its cached subset changes.
func (b *memoryBudget) takeCachedLocked(size int) []byte {
	best := 0
	for capacity, buffers := range b.cache {
		if len(buffers) == 0 || capacity < size {
			continue
		}
		if best == 0 || capacity < best {
			best = capacity
		}
	}
	if best == 0 {
		return nil
	}
	buffers := b.cache[best]
	last := len(buffers) - 1
	buffer := buffers[last]
	buffers[last] = nil
	if last == 0 {
		delete(b.cache, best)
	} else {
		b.cache[best] = buffers[:last]
	}
	b.cached -= int64(best)
	if b.cached < 0 {
		b.cached = 0
	}
	b.reusedAllocations++
	return buffer[:size]
}

// detachCachedBytesLocked severs references to at least requested bytes of
// reusable backing storage without immediately minting fresh-allocation credit.
// The detached bytes stay charged in b.used and move to pendingReclaim until a
// process-wide GC/scavenge has completed. This closes the race where old slabs
// remained resident while the same memory_limit credit was reused for new make().
func (b *memoryBudget) detachCachedBytesLocked(requested int64) int64 {
	if requested <= 0 || b.cached <= 0 || b.scavenging {
		return 0
	}
	var detached int64
	for detached < requested && b.cached > 0 {
		largest := 0
		for capacity, buffers := range b.cache {
			if len(buffers) > 0 && capacity > largest {
				largest = capacity
			}
		}
		if largest == 0 {
			break
		}
		buffers := b.cache[largest]
		need := int((requested - detached + int64(largest) - 1) / int64(largest))
		if need > len(buffers) {
			need = len(buffers)
		}
		start := len(buffers) - need
		clear(buffers[start:])
		if start == 0 {
			delete(b.cache, largest)
		} else {
			b.cache[largest] = buffers[:start]
		}
		bytes := int64(need * largest)
		detached += bytes
		b.cached -= bytes
	}
	if b.cached < 0 {
		b.cached = 0
	}
	b.pendingReclaim += detached
	return detached
}

// scavengePending performs the expensive process-wide GC/scavenge without
// holding the memory-budget mutex. Detached slabs remain charged in b.used for
// the entire scavenge, so concurrent callers cannot spend their credit early.
func (b *memoryBudget) scavengePending(includeGarbageHint bool) int64 {
	if b == nil {
		return 0
	}
	b.access.Lock()
	if b.scavenging || b.pendingReclaim == 0 && (!includeGarbageHint || b.garbageHintBytes == 0) {
		b.access.Unlock()
		return 0
	}
	b.scavenging = true
	pending := b.pendingReclaim
	hint := int64(0)
	if includeGarbageHint {
		hint = b.garbageHintBytes
	}
	b.access.Unlock()

	// FreeOSMemory forces a GC and asks the runtime to return free pages to the
	// OS. It is intentionally outside b.access: a process-wide runtime operation
	// must never hold a multipath data-plane mutex.
	memoryScavengeAccess.Lock()
	memoryScavenge()
	memoryScavengeAccess.Unlock()

	b.access.Lock()
	if pending > b.pendingReclaim {
		pending = b.pendingReclaim
	}
	b.pendingReclaim -= pending
	b.used -= pending
	if b.used < 0 {
		b.used = 0
	}
	if includeGarbageHint {
		if hint > b.garbageHintBytes {
			hint = b.garbageHintBytes
		}
		b.garbageHintBytes -= hint
	}
	b.scavenging = false
	if pending > 0 || hint > 0 {
		b.trimCount++
		b.trimmedBytes += uint64(max(int64(0), pending))
	}
	b.updatePressureLocked(time.Now())
	b.signalLocked()
	b.access.Unlock()
	return pending
}

func (b *memoryBudget) tryAcquirePrimary(size int) ([]byte, <-chan struct{}) {
	if b == nil || size <= 0 {
		return nil, nil
	}
	for {
		b.access.Lock()
		now := time.Now()
		b.noteActivityLocked(now)
		if buffer := b.takeCachedLocked(size); buffer != nil {
			b.updatePressureLocked(now)
			b.access.Unlock()
			return buffer, nil
		}
		if b.used+int64(size) <= b.limit {
			b.used += int64(size)
			b.freshAllocations++
			b.updatePressureLocked(now)
			b.access.Unlock()
			return make([]byte, size), nil
		}
		// Size-class fragmentation may leave charged reusable slabs which cannot
		// satisfy this request. Detach enough of them, but keep their credit charged
		// until scavenge completes.
		detached := b.detachCachedBytesLocked(b.used + int64(size) - b.limit)
		pending := b.pendingReclaim
		if detached == 0 && pending == 0 {
			b.waitCount++
			b.enterPressureLocked(now)
			changed := b.changed
			b.access.Unlock()
			return nil, changed
		}
		b.access.Unlock()
		b.scavengePending(false)
	}
}

// The caller has already charged a reusable session scratch/TX reservation.
func (b *memoryBudget) takeReservedBuffer(size int) []byte {
	b.access.Lock()
	now := time.Now()
	b.noteActivityLocked(now)
	if buffers := b.cache[size]; len(buffers) > 0 {
		buffer := buffers[len(buffers)-1]
		buffers[len(buffers)-1] = nil
		if len(buffers) == 1 {
			delete(b.cache, size)
		} else {
			b.cache[size] = buffers[:len(buffers)-1]
		}
		b.cached -= int64(size)
		b.used -= int64(size)
		b.reusedAllocations++
		b.updatePressureLocked(now)
		b.access.Unlock()
		return buffer[:size]
	}
	b.freshAllocations++
	b.access.Unlock()
	return make([]byte, size)
}

// Returning a reserved buffer transfers it from the session reservation into
// the reusable physical cache when there is budget room. If not, it is left to
// the runtime; this path is bounded by the fixed per-session reservation and is
// not the high-volume DATA allocator.
func (b *memoryBudget) putReservedBuffer(buffer []byte) {
	if b == nil || cap(buffer) == 0 {
		return
	}
	size := cap(buffer)
	b.access.Lock()
	now := time.Now()
	b.noteActivityLocked(now)
	if b.used+int64(size) <= b.limit {
		b.cache[size] = append(b.cache[size], buffer[:size])
		b.cached += int64(size)
		b.used += int64(size)
	} else if b.garbageHintBytes < b.limit {
		b.garbageHintBytes = min(b.limit, b.garbageHintBytes+int64(size))
	}
	b.updatePressureLocked(now)
	b.signalLocked()
	b.access.Unlock()
}

func (b *memoryBudget) reserveSession(bytes int64) bool {
	if b == nil || bytes <= 0 {
		return true
	}
	for {
		b.access.Lock()
		now := time.Now()
		b.noteActivityLocked(now)
		if b.used+bytes <= b.limit {
			b.used += bytes
			b.updatePressureLocked(now)
			b.access.Unlock()
			return true
		}
		detached := b.detachCachedBytesLocked(b.used + bytes - b.limit)
		pending := b.pendingReclaim
		b.access.Unlock()
		if detached == 0 && pending == 0 {
			return false
		}
		b.scavengePending(false)
	}
}

// releasePage retires receive-page physical credit without immediately making
// it spendable again. The Receiver has dropped its last structural reference before
// this returns to the caller, but the Go heap backing object can remain resident until
// a later GC. Keep the credit charged in pendingReclaim until scavenge completes.
func (b *memoryBudget) releasePage(bytes int64) {
	if b == nil || bytes <= 0 {
		return
	}
	b.access.Lock()
	now := time.Now()
	b.noteActivityLocked(now)
	b.pendingReclaim += bytes
	b.updatePressureLocked(now)
	b.signalLocked()
	b.access.Unlock()
}

func (b *memoryBudget) releaseSession(bytes int64) {
	if b == nil || bytes <= 0 {
		return
	}
	b.access.Lock()
	now := time.Now()
	b.noteActivityLocked(now)
	b.used -= bytes
	if b.used < 0 {
		b.used = 0
	}
	// Session/page/metadata releases can make heap objects unreachable without
	// passing through the DATA slab cache. Record a bounded hint so the next idle
	// maintenance cycle does not wait for the runtime's long forced-GC interval.
	if b.garbageHintBytes < b.limit {
		b.garbageHintBytes = min(b.limit, b.garbageHintBytes+bytes)
	}
	b.updatePressureLocked(now)
	b.signalLocked()
	b.access.Unlock()
}

func (b *memoryBudget) release(buffer []byte) {
	if b == nil || cap(buffer) == 0 {
		return
	}
	size := cap(buffer)
	b.access.Lock()
	now := time.Now()
	b.noteActivityLocked(now)
	// DATA slabs always remain physically charged while reusable. The previous
	// implementation dropped this charge under pressure/cache overflow even
	// though the Go backing array was still resident until a later GC, allowing
	// repeated fresh allocations to multiply RSS far beyond memory_limit.
	b.cache[size] = append(b.cache[size], buffer[:size])
	b.cached += int64(size)
	b.updatePressureLocked(now)
	b.signalLocked()
	b.access.Unlock()
}

func (b *memoryBudget) boosterAllowed() bool {
	if b == nil {
		return true
	}
	b.access.Lock()
	b.updatePressureLocked(time.Now())
	allowed := !b.pressure && b.activeBytesLocked() < b.boosterLimit
	b.access.Unlock()
	return allowed
}

func (b *memoryBudget) snapshot() memorySnapshot {
	if b == nil {
		return memorySnapshot{}
	}
	b.access.Lock()
	b.updatePressureLocked(time.Now())
	snapshot := b.snapshotLocked()
	b.access.Unlock()
	return snapshot
}

func (b *memoryBudget) snapshotLocked() memorySnapshot {
	return memorySnapshot{
		LimitBytes:         b.limit,
		UsedBytes:          b.used,
		CachedBytes:        b.cached,
		ActiveBytes:        b.activeBytesLocked(),
		BoosterLimitBytes:  b.boosterLimit,
		BoosterResumeBytes: b.boosterResume,
		Automatic:          b.automatic,
		Pressure:           b.pressure,
		PressureSince:      b.pressureSince,
		PressureEvents:     b.pressureCount,
		BackpressureEvents: b.waitCount,
		PeakUsedBytes:      b.peakUsed,
		PeakCachedBytes:    b.peakCached,
		FreshAllocations:   b.freshAllocations,
		ReusedAllocations:  b.reusedAllocations,
		TrimEvents:         b.trimCount,
		TrimmedBytes:       b.trimmedBytes,
	}
}

func (b *memoryBudget) updatePressureLocked(now time.Time) {
	b.updatePeaksLocked()
	active := b.activeBytesLocked()
	if b.pressure {
		if active <= b.boosterResume {
			duration := now.Sub(b.pressureSince)
			b.pressure = false
			b.pressureSince = time.Time{}
			b.signalLocked()
			b.emitEventLocked(false, duration)
		}
		return
	}
	if active >= b.boosterLimit {
		b.enterPressureLocked(now)
	}
}

func (b *memoryBudget) enterPressureLocked(now time.Time) {
	if b.pressure {
		return
	}
	b.pressure = true
	b.pressureSince = now
	b.pressureCount++
	b.signalLocked()
	b.emitEventLocked(true, 0)
}

func (b *memoryBudget) updatePeaksLocked() {
	if b.used > b.peakUsed {
		b.peakUsed = b.used
	}
	if b.cached > b.peakCached {
		b.peakCached = b.cached
	}
}

func (b *memoryBudget) emitEventLocked(entered bool, duration time.Duration) {
	event := memoryPressureEvent{
		entered:  entered,
		duration: duration,
		snapshot: b.snapshotLocked(),
	}
	select {
	case b.events <- event:
	default:
	}
}

// trimIdle drops excess reusable DATA slabs after the node has been quiet long
// enough to distinguish end-of-burst from ordinary ACK turnover. When no
// multipath session remains, the target is zero. Detached slab credit is not
// returned until the subsequent process-wide scavenge completes. The scavenge
// also covers non-slab garbage hints (receive pages, session metadata, stacks).
func (b *memoryBudget) trimIdle(now time.Time) int64 {
	if b == nil {
		return 0
	}
	b.access.Lock()
	if now.Sub(b.lastActivity) < memoryIdleTrimDelay || b.scavenging {
		b.access.Unlock()
		return 0
	}
	target := b.cacheLimit
	if b.sessions.Load() == 0 {
		target = 0
	}
	detached := int64(0)
	if b.cached > target {
		detached = b.detachCachedBytesLocked(b.cached - target)
	}
	// Scavenge when physical slabs were detached, when a closed instance/session
	// left any garbage hint, or when a live instance accumulated a meaningful
	// amount of non-slab garbage. This makes single-thread and multi-thread idle
	// release use the same bounded path.
	threshold := max(int64(1<<20), b.cacheLimit/2)
	includeHint := b.garbageHintBytes >= threshold || b.sessions.Load() == 0 && b.garbageHintBytes > 0
	pending := b.pendingReclaim
	b.access.Unlock()
	if pending > 0 || includeHint {
		b.scavengePending(includeHint)
	}
	return detached
}

func (b *memoryBudget) startLogging(ctx context.Context, logger log.ContextLogger, side string) {
	if b == nil {
		return
	}
	b.logOnce.Do(func() {
		snapshot := b.snapshot()
		source := "configured"
		if snapshot.Automatic {
			source = "automatic"
		}
		logger.InfoContext(
			ctx,
			"multipath memory budget: side=", side,
			" limit=", byteformats.FormatMemoryBytes(uint64(snapshot.LimitBytes)),
			" source=", source,
			" high=", byteformats.FormatMemoryBytes(uint64(snapshot.BoosterLimitBytes)),
			" resume=", byteformats.FormatMemoryBytes(uint64(snapshot.BoosterResumeBytes)),
			" idle_cache_limit=", byteformats.FormatMemoryBytes(uint64(b.cacheLimit)),
			" idle_trim=", memoryIdleTrimDelay,
		)
		go func() {
			ticker := time.NewTicker(memoryMaintenanceInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case now := <-ticker.C:
					b.trimIdle(now)
				case event := <-b.events:
					active := event.snapshot.ActiveBytes
					if event.entered {
						logger.InfoContext(
							ctx,
							"multipath memory pressure entered: side=", side,
							" active=", byteformats.FormatMemoryBytes(uint64(max(int64(0), active))),
							" committed=", byteformats.FormatMemoryBytes(uint64(max(int64(0), event.snapshot.UsedBytes))),
							" high=", byteformats.FormatMemoryBytes(uint64(event.snapshot.BoosterLimitBytes)),
							" limit=", byteformats.FormatMemoryBytes(uint64(event.snapshot.LimitBytes)),
						)
					} else {
						logger.InfoContext(
							ctx,
							"multipath memory pressure cleared: side=", side,
							" active=", byteformats.FormatMemoryBytes(uint64(max(int64(0), active))),
							" committed=", byteformats.FormatMemoryBytes(uint64(max(int64(0), event.snapshot.UsedBytes))),
							" resume=", byteformats.FormatMemoryBytes(uint64(event.snapshot.BoosterResumeBytes)),
							" duration=", event.duration.Round(time.Millisecond),
						)
					}
				}
			}
		}()
	})
}

func (b *memoryBudget) signalLocked() {
	close(b.changed)
	b.changed = make(chan struct{})
}
