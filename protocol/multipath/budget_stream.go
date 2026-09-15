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
	b.access.Lock()
	defer b.access.Unlock()
	limit := b.boosterLimit
	if head {
		limit = b.limit
	}
	if b.used+size > limit && b.cached > 0 {
		b.dropCacheLocked()
	}
	if b.used+size > limit {
		b.enterPressureLocked(time.Now())
		return false
	}
	b.used += size
	b.updatePressureLocked(time.Now())
	return true
}
