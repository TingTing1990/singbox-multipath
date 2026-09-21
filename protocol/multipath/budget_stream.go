package multipath

import "time"

// The node budget is authoritative. Automatic per-direction ceilings share its
// ordinary allocation region; they are not preallocated or promised credits.
// Explicit per-connection limits remain hard caps. This removes the historical
// fixed 64 MiB bottleneck without introducing a bandwidth-dependent knob.
func automaticBufferLimit(b *memoryBudget, chunk int) int64 {
	return max(int64(chunk), min(int64(maxReorderBytes), b.boosterLimit/2))
}

func (b *memoryBudget) reservePage(size int64, head bool) bool {
	for {
		b.access.Lock()
		now := time.Now()
		b.noteActivityLocked(now)
		limit := b.boosterLimit
		if head {
			limit = b.limit
		}
		if b.used+size <= limit {
			b.used += size
			b.updatePressureLocked(now)
			b.access.Unlock()
			return true
		}
		detached := b.detachCachedBytesLocked(b.used + size - limit)
		pending := b.pendingReclaim
		if detached == 0 && pending == 0 {
			b.enterPressureLocked(now)
			b.access.Unlock()
			return false
		}
		b.access.Unlock()
		b.scavengePending(false)
	}
}
