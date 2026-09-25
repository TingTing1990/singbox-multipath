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
	"github.com/sagernet/sing-box/protocol/multipath/stream"
	"github.com/sagernet/sing/common/byteformats"
)

const (
	automaticMemoryLimitCap      = 512 << 20
	automaticMemoryLimitFallback = 256 << 20
	memoryCacheLimitCap          = 16 << 20
	memoryIdleReclaimDelay       = 2 * time.Second
	sessionMemoryBase            = 128 << 10
	wireFrameMemoryEstimate      = int64(unsafe.Sizeof(wireFrame{}))
)

var (
	errMemoryLimit = errors.New("multipath memory limit reached")
	// Production uses runtime/debug.FreeOSMemory. Tests replace this function
	// only to hold a batch at the GC boundary and verify ordering.
	memoryScavenge = debug.FreeOSMemory
	// The runtime operation is process-wide. Never hold memoryBudget.access while
	// waiting here, and never run more than one process-wide scavenge at once.
	memoryScavengeAccess sync.Mutex
)

type memorySnapshot struct {
	LimitBytes         int64
	UsedBytes          int64
	CachedBytes        int64
	BoosterLimitBytes  int64
	BoosterResumeBytes int64
	Automatic          bool
	Pressure           bool
	PressureSince      time.Time
	PressureEvents     uint64
	BackpressureEvents uint64
	PeakUsedBytes      int64
	PeakCachedBytes    int64
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
	used          int64
	cached        int64
	cache         map[int][][]byte
	pressure      bool
	pressureSince time.Time
	pressureCount uint64
	waitCount     uint64
	peakUsed      int64
	peakCached    int64

	// Physical-credit reclaim is batch/epoch based. pendingReclaim contains only
	// allocations detached after the current GC batch began. reclaimIssued is a
	// monotonic handoff epoch; reclaimCompleted advances only after the GC that
	// owns that batch returns. New releases during GC therefore cannot be credited
	// by the older batch and cannot lose a wakeup.
	pendingReclaim   int64
	reclaimIssued    uint64
	reclaimCompleted uint64
	reclaimRunning   bool

	lastActivity         time.Time
	maintenanceScheduled bool

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
		changed:       make(chan struct{}),
		events:        make(chan memoryPressureEvent, 4),
	}
}

func sessionMemoryReservation(cfg coreConfig) int64 {
	// Fixed channels: one assignment, 32 controls and one coalesced feedback
	// frame per path. The base also covers worker stacks and 16 prepaid primary
	// flight records. Two reader scratch buffers and one primary TX reserve
	// keep head recovery independent of speculative allocations.
	// Inline record arrays are always live, even while a larger array is in use.
	// Charge them at admission so head progress does not depend on new storage.
	records := stream.SenderRecordBytes + 2*stream.PathRecordBytes +
		int64(stream.RecordReserve)*(int64(unsafe.Sizeof(dataMapping{}))+2*int64(unsafe.Sizeof(preferredCapacityPathRange{})))
	return sessionMemoryBase + 2*34*wireFrameMemoryEstimate + int64(cfg.ChunkSize)*3 + records
}

func (b *memoryBudget) tryAcquirePrimary(size int) ([]byte, <-chan struct{}) {
	b.access.Lock()
	defer b.access.Unlock()
	if buffer := b.popCacheLocked(size); buffer != nil {
		return buffer[:size], nil
	}
	now := time.Now()
	if b.used+int64(size) > b.limit && b.cached > 0 {
		b.dropCacheLocked(now)
	}
	if b.used+int64(size) <= b.limit {
		b.used += int64(size)
		b.lastActivity = now
		b.updatePressureLocked(now)
		return make([]byte, size), nil
	}
	// Allocation remains non-blocking. If detached physical credit exists, start
	// its asynchronous batch and let the existing changed signal wake the caller.
	b.startReclaimLocked()
	return nil, b.changed
}

// The caller has already charged a reusable session scratch/TX reservation.
func (b *memoryBudget) takeReservedBuffer(size int) []byte {
	b.access.Lock()
	if buffer := b.popCacheLocked(size); buffer != nil {
		b.used -= int64(size)
		b.lastActivity = time.Now()
		b.updatePressureLocked(b.lastActivity)
		b.access.Unlock()
		return buffer[:size]
	}
	b.lastActivity = time.Now()
	b.access.Unlock()
	return make([]byte, size)
}

// A slice's unused capacity is still scanned by GC. Clear checked-out entries
// and shrink sparse indexes so neither payloads nor historical size-class peaks
// can remain reachable outside the cache's byte accounting.
func (b *memoryBudget) popCacheLocked(size int) []byte {
	buffers := b.cache[size]
	if len(buffers) == 0 {
		return nil
	}
	last := len(buffers) - 1
	buffer := buffers[last]
	buffers[last] = nil
	if last == 0 {
		delete(b.cache, size)
	} else if cap(buffers) > 16 && last <= cap(buffers)/4 {
		b.cache[size] = append([][]byte(nil), buffers[:last]...)
	} else {
		b.cache[size] = buffers[:last]
	}
	b.cached -= int64(size)
	return buffer
}

// Returning a reserved buffer does not release its session reservation. Cache
// storage is charged separately until reused or dropped.
func (b *memoryBudget) prepareReservedRelease(buffer []byte) func() {
	size := cap(buffer)
	if size == 0 {
		return nil
	}
	now := time.Now()
	b.access.Lock()
	if !b.pressure && b.cached+int64(size) <= b.cacheLimit && b.used+int64(size) <= b.limit {
		b.cache[size] = append(b.cache[size], buffer[:size])
		b.cached += int64(size)
		b.used += int64(size)
		b.lastActivity = now
		b.updatePressureLocked(now)
		b.signalLocked()
		b.access.Unlock()
		return nil
	}
	// This allocation was physically covered by the session reservation. Move an
	// equal charge out of that reservation now, but do not hand it to GC until the
	// caller has cleared its final slice reference.
	b.used += int64(size)
	b.lastActivity = now
	b.updatePressureLocked(now)
	b.access.Unlock()
	return func() { b.retireDetached(int64(size)) }
}

// putReservedBuffer is kept for focused accounting tests. Production owners use
// prepareReservedRelease so they can clear their own slice before finalizing.
func (b *memoryBudget) putReservedBuffer(buffer []byte) {
	finish := b.prepareReservedRelease(buffer)
	buffer = nil
	if finish != nil {
		finish()
	}
}

func (b *memoryBudget) reserveSession(bytes int64) bool {
	if b == nil || bytes <= 0 {
		return true
	}
	b.access.Lock()
	defer b.access.Unlock()
	now := time.Now()
	if b.used+bytes > b.limit && b.cached > 0 {
		b.dropCacheLocked(now)
	}
	if b.used+bytes > b.limit {
		// Preserve setup admission as an immediate success/failure decision. A
		// pending physical batch is kicked asynchronously but never waited here.
		b.startReclaimLocked()
		return false
	}
	b.used += bytes
	b.lastActivity = now
	b.updatePressureLocked(now)
	return true
}

func (b *memoryBudget) releaseSession(bytes int64) {
	if b == nil || bytes <= 0 {
		return
	}
	b.access.Lock()
	b.used -= bytes
	if b.used < 0 {
		b.used = 0
	}
	b.updatePressureLocked(time.Now())
	b.signalLocked()
	b.access.Unlock()
}

func (b *memoryBudget) prepareRelease(buffer []byte) func() {
	if b == nil || cap(buffer) == 0 {
		return nil
	}
	size := cap(buffer)
	now := time.Now()
	b.access.Lock()
	b.updatePressureLocked(now)
	if !b.pressure && b.cached+int64(size) <= b.cacheLimit {
		b.cache[size] = append(b.cache[size], buffer[:size])
		b.cached += int64(size)
		b.lastActivity = now
		b.updatePressureLocked(now)
		b.signalLocked()
		b.access.Unlock()
		return nil
	}
	b.access.Unlock()
	// The returned closure captures only budget+size, never the backing slice.
	// Its caller clears the final protocol reference before invoking it.
	return func() { b.retireDetached(int64(size)) }
}

// release is retained for focused budget tests. Protocol owners must use the
// two-phase prepareRelease path so GC cannot race their last backing reference.
func (b *memoryBudget) release(buffer []byte) {
	finish := b.prepareRelease(buffer)
	buffer = nil
	if finish != nil {
		finish()
	}
}

// retireDetached is used only after the last protocol reference to an actual
// backing allocation has been cleared. used is intentionally unchanged here.
func (b *memoryBudget) retireDetached(bytes int64) uint64 {
	if b == nil || bytes <= 0 {
		return 0
	}
	b.access.Lock()
	epoch := b.retireDetachedLocked(bytes, time.Now())
	b.access.Unlock()
	return epoch
}

func (b *memoryBudget) retireDetachedLocked(bytes int64, now time.Time) uint64 {
	if bytes <= 0 {
		return b.reclaimIssued
	}
	b.pendingReclaim += bytes
	b.reclaimIssued++
	b.lastActivity = now
	b.scheduleMaintenanceLocked(now)
	return b.reclaimIssued
}

// retireReservedDetached transfers a concrete backing allocation out of a
// fixed session reservation before that reservation is released.
func (b *memoryBudget) retireReservedDetached(bytes int64) uint64 {
	if b == nil || bytes <= 0 {
		return 0
	}
	b.access.Lock()
	b.used += bytes
	epoch := b.retireDetachedLocked(bytes, time.Now())
	b.updatePressureLocked(time.Now())
	b.access.Unlock()
	return epoch
}

// reclaimCutoff starts pending work and returns the latest detach epoch visible
// at this point. A close waits only for this cutoff; later traffic cannot extend
// its boundary.
func (b *memoryBudget) reclaimCutoff() uint64 {
	if b == nil {
		return 0
	}
	b.access.Lock()
	cutoff := b.reclaimIssued
	b.startReclaimLocked()
	b.access.Unlock()
	return cutoff
}

func (b *memoryBudget) waitReclaim(cutoff uint64) {
	if b == nil || cutoff == 0 {
		return
	}
	for {
		b.access.Lock()
		if b.reclaimCompleted >= cutoff {
			b.access.Unlock()
			return
		}
		b.startReclaimLocked()
		changed := b.changed
		b.access.Unlock()
		<-changed
	}
}

func (b *memoryBudget) boosterAllowed() bool {
	if b == nil {
		return true
	}
	b.access.Lock()
	b.updatePressureLocked(time.Now())
	allowed := !b.pressure && b.used < b.boosterLimit
	b.access.Unlock()
	return allowed
}

func (b *memoryBudget) snapshot() memorySnapshot {
	if b == nil {
		return memorySnapshot{}
	}
	b.access.Lock()
	b.updatePressureLocked(time.Now())
	snapshot := memorySnapshot{
		LimitBytes:         b.limit,
		UsedBytes:          b.used,
		CachedBytes:        b.cached,
		BoosterLimitBytes:  b.boosterLimit,
		BoosterResumeBytes: b.boosterResume,
		Automatic:          b.automatic,
		Pressure:           b.pressure,
		PressureSince:      b.pressureSince,
		PressureEvents:     b.pressureCount,
		BackpressureEvents: b.waitCount,
		PeakUsedBytes:      b.peakUsed,
		PeakCachedBytes:    b.peakCached,
	}
	b.access.Unlock()
	return snapshot
}

func (b *memoryBudget) updatePressureLocked(now time.Time) {
	b.updatePeaksLocked()
	if b.pressure {
		if b.used <= b.boosterResume {
			duration := now.Sub(b.pressureSince)
			b.pressure = false
			b.pressureSince = time.Time{}
			b.signalLocked()
			b.emitEventLocked(false, duration)
		}
		return
	}
	if b.used >= b.boosterLimit {
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
		snapshot: memorySnapshot{
			LimitBytes:         b.limit,
			UsedBytes:          b.used,
			CachedBytes:        b.cached,
			BoosterLimitBytes:  b.boosterLimit,
			BoosterResumeBytes: b.boosterResume,
			Automatic:          b.automatic,
			Pressure:           b.pressure,
			PressureSince:      b.pressureSince,
			PressureEvents:     b.pressureCount,
			BackpressureEvents: b.waitCount,
			PeakUsedBytes:      b.peakUsed,
			PeakCachedBytes:    b.peakCached,
		},
	}
	select {
	case b.events <- event:
	default:
	}
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
			" cache_limit=", byteformats.FormatMemoryBytes(uint64(b.cacheLimit)),
		)
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case event := <-b.events:
					if event.entered {
						logger.InfoContext(
							ctx,
							"multipath memory pressure entered: side=", side,
							" used=", byteformats.FormatMemoryBytes(uint64(event.snapshot.UsedBytes)),
							" high=", byteformats.FormatMemoryBytes(uint64(event.snapshot.BoosterLimitBytes)),
							" limit=", byteformats.FormatMemoryBytes(uint64(event.snapshot.LimitBytes)),
						)
					} else {
						logger.InfoContext(
							ctx,
							"multipath memory pressure cleared: side=", side,
							" used=", byteformats.FormatMemoryBytes(uint64(event.snapshot.UsedBytes)),
							" resume=", byteformats.FormatMemoryBytes(uint64(event.snapshot.BoosterResumeBytes)),
							" duration=", event.duration.Round(time.Millisecond),
						)
					}
				}
			}
		}()
	})
}

func (b *memoryBudget) dropCacheLocked(now time.Time) {
	if b.cached == 0 {
		return
	}
	bytes := b.cached
	// Clear every stored slice pointer before the byte charge is handed to the
	// reclaim worker. The cache map itself may survive until GC; payload owners do
	// not.
	for key, buffers := range b.cache {
		clear(buffers)
		delete(b.cache, key)
	}
	b.cached = 0
	b.retireDetachedLocked(bytes, now)
}

func (b *memoryBudget) scheduleMaintenanceLocked(now time.Time) {
	if b.pendingReclaim == 0 && b.cached == 0 {
		return
	}
	b.lastActivity = now
	if b.maintenanceScheduled {
		return
	}
	b.maintenanceScheduled = true
	time.AfterFunc(memoryIdleReclaimDelay, b.runIdleMaintenance)
}

func (b *memoryBudget) runIdleMaintenance() {
	b.access.Lock()
	idleFor := time.Since(b.lastActivity)
	if idleFor < memoryIdleReclaimDelay {
		delay := memoryIdleReclaimDelay - idleFor
		b.access.Unlock()
		time.AfterFunc(delay, b.runIdleMaintenance)
		return
	}
	b.maintenanceScheduled = false
	now := time.Now()
	if b.sessions.Load() == 0 && b.cached > 0 {
		b.dropCacheLocked(now)
	}
	b.startReclaimLocked()
	b.access.Unlock()
}

// startReclaimLocked moves the current pending set into one immutable GC batch.
// Releases that arrive after this point accumulate in pendingReclaim for the
// next batch. There is deliberately no queued boolean: the running flag and the
// pending byte count are sufficient to avoid the confirmed stale-queue state.
func (b *memoryBudget) startReclaimLocked() {
	if b.reclaimRunning || b.pendingReclaim <= 0 {
		return
	}
	bytes := b.pendingReclaim
	cutoff := b.reclaimIssued
	b.pendingReclaim = 0
	b.reclaimRunning = true
	go b.runReclaimBatch(bytes, cutoff)
}

func (b *memoryBudget) runReclaimBatch(bytes int64, cutoff uint64) {
	if bytes <= 0 {
		b.access.Lock()
		b.reclaimRunning = false
		b.signalLocked()
		b.startReclaimLocked()
		b.access.Unlock()
		return
	}
	memoryScavengeAccess.Lock()
	memoryScavenge()
	memoryScavengeAccess.Unlock()

	b.access.Lock()
	b.used -= bytes
	if b.used < 0 {
		b.used = 0
	}
	if cutoff > b.reclaimCompleted {
		b.reclaimCompleted = cutoff
	}
	b.reclaimRunning = false
	b.updatePressureLocked(time.Now())
	b.signalLocked()
	// A GC can retire only the batch captured before it started. Any release that
	// arrived while it ran remains pending and is immediately assigned a new batch.
	b.startReclaimLocked()
	b.access.Unlock()
}

func (b *memoryBudget) signalLocked() {
	close(b.changed)
	b.changed = make(chan struct{})
}
