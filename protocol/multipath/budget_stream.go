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

	// PageMemory.Acquire is explicitly non-blocking and is called from receive
	// and scheduler paths while core stateMu may be held. Never run process-wide
	// GC/scavenge inline here. Detach reclaimable TX cache, queue one background
	// reclaim cycle, then fail admission for this attempt. The receiver may reuse
	// or prune an already charged page; otherwise the sender retains un-ACKed data
	// and a later attempt can succeed after reclaim signals b.changed.
	if !b.scavenging {
		b.detachCachedBytesLocked(b.used + size - limit)
	}
	if b.pendingReclaim > 0 {
		b.queueScavengeLocked()
	}
	b.enterPressureLocked(now)
	b.access.Unlock()
	return false
}
