package multipath

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common/byteformats"
)

const (
	automaticMemoryLimitCap      = 512 << 20
	automaticMemoryLimitFallback = 256 << 20
	memoryCacheLimitCap          = 16 << 20
	sessionMemoryBase            = 128 << 10
	wireFrameMemoryEstimate      = 64
)

var errMemoryLimit = errors.New("multipath memory limit reached")

type memoryClass uint8

const (
	memoryClassPrimary memoryClass = iota
	memoryClassBooster
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
	access sync.Mutex

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
	changed       chan struct{}
	events        chan memoryPressureEvent
	logOnce       sync.Once
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
	boosterLimit := limit * 7 / 8
	boosterResume := limit * 3 / 4
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
	// Covers goroutine stacks, maps, and the incoming plus two per-leg channel
	// backing arrays. Payload buffers are charged separately at allocation time.
	return sessionMemoryBase + int64(cfg.QueueFrames)*4*wireFrameMemoryEstimate
}

func (b *memoryBudget) reserveSession(bytes int64) bool {
	if b == nil || bytes <= 0 {
		return true
	}
	b.access.Lock()
	defer b.access.Unlock()
	if b.used+bytes > b.limit {
		b.dropCacheLocked()
	}
	if b.used+bytes > b.limit {
		return false
	}
	b.used += bytes
	b.updatePressureLocked(time.Now())
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

func (b *memoryBudget) acquire(ctx context.Context, size int, class memoryClass) ([]byte, error) {
	if size <= 0 {
		return nil, errors.New("invalid multipath buffer size")
	}
	waited := false
	for {
		b.access.Lock()
		b.updatePressureLocked(time.Now())
		if class == memoryClassBooster && b.pressure {
			if !waited {
				b.waitCount++
				waited = true
			}
			changed := b.changed
			b.access.Unlock()
			select {
			case <-ctx.Done():
				return nil, errCoreClosed
			case <-changed:
			}
			continue
		}

		if buffers := b.cache[size]; len(buffers) > 0 {
			buffer := buffers[len(buffers)-1]
			if len(buffers) == 1 {
				delete(b.cache, size)
			} else {
				b.cache[size] = buffers[:len(buffers)-1]
			}
			b.cached -= int64(size)
			b.access.Unlock()
			return buffer[:size], nil
		}

		allocationLimit := b.limit
		if class == memoryClassBooster {
			allocationLimit = b.boosterLimit
		}
		if b.used+int64(size) <= allocationLimit {
			b.used += int64(size)
			b.updatePressureLocked(time.Now())
			b.access.Unlock()
			return make([]byte, size), nil
		}

		if b.cached > 0 {
			b.dropCacheLocked()
			b.updatePressureLocked(time.Now())
			b.signalLocked()
			b.access.Unlock()
			continue
		}
		if class == memoryClassBooster {
			b.enterPressureLocked(time.Now())
		}
		if !waited {
			b.waitCount++
			waited = true
		}
		changed := b.changed
		b.access.Unlock()
		select {
		case <-ctx.Done():
			return nil, errCoreClosed
		case <-changed:
		}
	}
}

func (b *memoryBudget) release(buffer []byte) {
	if b == nil || cap(buffer) == 0 {
		return
	}
	size := cap(buffer)
	b.access.Lock()
	b.updatePressureLocked(time.Now())
	if !b.pressure && b.cached+int64(size) <= b.cacheLimit {
		b.cache[size] = append(b.cache[size], buffer[:size])
		b.cached += int64(size)
	} else {
		b.used -= int64(size)
		if b.used < 0 {
			b.used = 0
		}
	}
	b.updatePressureLocked(time.Now())
	b.signalLocked()
	b.access.Unlock()
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

func (b *memoryBudget) dropCacheLocked() {
	if b.cached == 0 {
		return
	}
	b.used -= b.cached
	b.cached = 0
	b.cache = make(map[int][][]byte)
}

func (b *memoryBudget) signalLocked() {
	close(b.changed)
	b.changed = make(chan struct{})
}
