package multipath

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAutomaticMemoryLimit(t *testing.T) {
	if limit := automaticMemoryLimit(256 << 20); limit != 128<<20 {
		t.Fatalf("unexpected limit for small host: %d", limit)
	}
	if limit := automaticMemoryLimit(4 << 30); limit != 512<<20 {
		t.Fatalf("automatic limit was not capped: %d", limit)
	}
}

func TestIdleSessionsDoNotAllocateAdvertisedWindows(t *testing.T) {
	cfg := testCoreConfig()
	cfg.ChunkSize, cfg.QueueFrames, cfg.QueueBytes = 65536, 256, 16<<20
	budget := newMemoryBudget(512<<20, false)
	cfg.Memory = budget
	var cores []*mpCore
	defer func() {
		for _, core := range cores {
			core.Close()
		}
		for _, core := range cores {
			<-core.released
		}
		if budget.sessions.Load() != 0 {
			t.Error("session share leaked")
		}
	}()
	for range 32 {
		core, _, err := newCoreWithError(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		cores = append(cores, core)
	}
	if budget.sessions.Load() != 32 {
		t.Fatal("incorrect live session count")
	}
	if budget.snapshot().Pressure {
		t.Fatal("idle session reservations exhausted the budget")
	}
	if budget.snapshot().UsedBytes > budget.limit/4 {
		t.Fatal("idle connections allocated their entire advertised windows")
	}
}

func acquireTestMemory(t *testing.T, budget *memoryBudget, size int) []byte {
	t.Helper()
	buffer, _ := budget.tryAcquirePrimary(size)
	if buffer == nil {
		t.Fatalf("test allocation failed: %d", size)
	}
	return buffer
}

func TestMemoryBudgetBoosterBackpressurePreservesPrimaryReserve(t *testing.T) {
	budget := newMemoryBudget(1024, false)
	first := acquireTestMemory(t, budget, 800)
	second := acquireTestMemory(t, budget, 100)
	if snapshot := budget.snapshot(); !snapshot.Pressure || snapshot.BoosterLimitBytes != 896 || snapshot.BoosterResumeBytes != 768 {
		t.Fatalf("unexpected pressure snapshot: %+v", snapshot)
	}
	if budget.boosterAllowed() || budget.reservePage(64, false) {
		t.Fatal("booster passed high watermark")
	}
	primary := acquireTestMemory(t, budget, 100)
	budget.release(primary)
	budget.release(second)
	if budget.boosterAllowed() {
		t.Fatal("booster resumed above low watermark")
	}
	budget.release(first)
	if !budget.boosterAllowed() || !budget.reservePage(64, false) {
		t.Fatal("booster did not resume")
	}
	budget.releaseSession(64)
	cached := acquireTestMemory(t, budget, 64)
	budget.release(cached)
	snapshot := budget.snapshot()
	if snapshot.Pressure || snapshot.PressureEvents != 1 {
		t.Fatalf("unexpected final memory snapshot: %+v", snapshot)
	}
	if snapshot.PeakUsedBytes < 1000 || snapshot.PeakCachedBytes < 64 {
		t.Fatalf("memory peaks not retained: %+v", snapshot)
	}
}

func TestMemoryBudgetSessionAdmissionIsReleased(t *testing.T) {
	cfg := testCoreConfig()
	reservation := minimumSessionMemory(cfg)
	budget := newMemoryBudget(reservation+1, false)
	cfg.Memory = budget
	first, _, err := newCoreWithError(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = newCoreWithError(context.Background(), cfg); !errors.Is(err, errMemoryLimit) {
		t.Fatalf("expected session admission failure, got %v", err)
	}
	first.Close()
	deadline := time.Now().Add(time.Second)
	for budget.snapshot().UsedBytes != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	second, _, err := newCoreWithError(context.Background(), cfg)
	if err != nil {
		t.Fatalf("released session budget was not reusable: %v", err)
	}
	second.Close()
}

func TestCoreMemoryPressureKeepsLeg0Available(t *testing.T) {
	cfg := testCoreConfig()
	budget := newMemoryBudget(4<<20, false)
	cfg.Memory = budget
	core, _, err := newCoreWithError(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer core.Close()
	core.activate(activationInfo{Reason: activationReasonBytes})

	leg0Core, leg0Peer := net.Pipe()
	defer leg0Peer.Close()
	leg1Core, leg1Peer := net.Pipe()
	defer leg1Peer.Close()
	if _, err = core.addLeg(0, leg0Core, nil); err != nil {
		t.Fatal(err)
	}
	if _, err = core.addLeg(1, leg1Core, nil); err != nil {
		t.Fatal(err)
	}

	snapshot := budget.snapshot()
	pressureBytes := int(snapshot.BoosterLimitBytes - snapshot.UsedBytes)
	pressureBuffer := acquireTestMemory(t, budget, pressureBytes)
	defer budget.release(pressureBuffer)
	core.stateMu.Lock()
	selected := core.choosePathLocked(cfg.ChunkSize)
	core.stateMu.Unlock()
	if selected == nil || selected.id != 0 {
		t.Fatalf("memory pressure selected leg %v instead of leg0", selected)
	}
}

func TestMemoryBudgetReleasedSlabKeepsPhysicalCreditAndIsReused(t *testing.T) {
	budget := newMemoryBudget(4<<20, false)
	const size = 64 << 10
	first := acquireTestMemory(t, budget, size)
	firstPtr := &first[0]
	budget.release(first)

	afterRelease := budget.snapshot()
	if afterRelease.UsedBytes != size || afterRelease.CachedBytes != size || afterRelease.ActiveBytes != 0 {
		t.Fatalf("released slab lost physical credit: %+v", afterRelease)
	}
	second := acquireTestMemory(t, budget, size)
	if &second[0] != firstPtr {
		t.Fatal("released slab was not reused before a fresh allocation")
	}
	afterReuse := budget.snapshot()
	if afterReuse.FreshAllocations != 1 || afterReuse.ReusedAllocations != 1 {
		t.Fatalf("unexpected allocation counters: %+v", afterReuse)
	}
	budget.release(second)
}

func TestMemoryBudgetTurnoverDoesNotMintFreshPhysicalCredit(t *testing.T) {
	budget := newMemoryBudget(4<<20, false)
	const (
		size       = 64 << 10
		iterations = 10000
	)
	for range iterations {
		buffer := acquireTestMemory(t, budget, size)
		budget.release(buffer)
	}
	snapshot := budget.snapshot()
	if snapshot.FreshAllocations != 1 {
		t.Fatalf("turnover performed %d fresh allocations, want 1", snapshot.FreshAllocations)
	}
	if snapshot.ReusedAllocations != iterations-1 {
		t.Fatalf("turnover reused %d allocations, want %d", snapshot.ReusedAllocations, iterations-1)
	}
	if snapshot.UsedBytes != size || snapshot.CachedBytes != size || snapshot.PeakUsedBytes > budget.limit {
		t.Fatalf("physical credit escaped budget: %+v", snapshot)
	}
}

func TestMemoryBudgetBestFitReuseAvoidsSizeClassChurn(t *testing.T) {
	budget := newMemoryBudget(4<<20, false)
	large := acquireTestMemory(t, budget, 64<<10)
	budget.release(large)
	small := acquireTestMemory(t, budget, 8<<10)
	if cap(small) != 64<<10 {
		t.Fatalf("smaller request did not reuse larger committed slab: cap=%d", cap(small))
	}
	if snapshot := budget.snapshot(); snapshot.FreshAllocations != 1 || snapshot.ReusedAllocations != 1 {
		t.Fatalf("unexpected allocation counters: %+v", snapshot)
	}
	budget.release(small)
}

func TestMemoryBudgetIdleTrimScavengesExcessCommittedCache(t *testing.T) {
	budget := newMemoryBudget(8<<20, false)
	budget.sessions.Store(1)
	var buffers [][]byte
	for range 32 {
		buffers = append(buffers, acquireTestMemory(t, budget, 64<<10))
	}
	for _, buffer := range buffers {
		budget.release(buffer)
	}
	before := budget.snapshot()
	if before.CachedBytes <= budget.cacheLimit {
		t.Fatalf("fixture did not exceed idle cache target: %+v", before)
	}

	oldScavenge := memoryScavenge
	calls := 0
	memoryScavenge = func() { calls++ }
	t.Cleanup(func() { memoryScavenge = oldScavenge })

	budget.access.Lock()
	budget.lastActivity = time.Now().Add(-memoryIdleTrimDelay - time.Second)
	budget.access.Unlock()
	if dropped := budget.trimIdle(time.Now()); dropped <= 0 {
		t.Fatal("idle trim did not drop excess committed cache")
	}
	after := budget.snapshot()
	if after.CachedBytes > budget.cacheLimit || after.UsedBytes > budget.cacheLimit {
		t.Fatalf("idle trim retained excessive cache: %+v", after)
	}
	if calls != 1 || after.TrimEvents == 0 || after.TrimmedBytes == 0 {
		t.Fatalf("idle trim did not invoke controlled scavenge: calls=%d snapshot=%+v", calls, after)
	}
}

func TestMemoryBudgetIdleTrimDropsAllCacheWithoutSessions(t *testing.T) {
	budget := newMemoryBudget(8<<20, false)
	var buffers [][]byte
	for range 8 {
		buffers = append(buffers, acquireTestMemory(t, budget, 64<<10))
	}
	for _, buffer := range buffers {
		budget.release(buffer)
	}
	oldScavenge := memoryScavenge
	memoryScavenge = func() {}
	t.Cleanup(func() { memoryScavenge = oldScavenge })
	budget.access.Lock()
	budget.lastActivity = time.Now().Add(-memoryIdleTrimDelay - time.Second)
	budget.access.Unlock()
	budget.trimIdle(time.Now())
	if snapshot := budget.snapshot(); snapshot.CachedBytes != 0 || snapshot.UsedBytes != 0 {
		t.Fatalf("zero-session trim retained committed cache: %+v", snapshot)
	}
}

func TestMemoryBudgetScavengeRunsOutsideBudgetLockAndKeepsCredit(t *testing.T) {
	budget := newMemoryBudget(128<<10, false)
	first := acquireTestMemory(t, budget, 64<<10)
	second := acquireTestMemory(t, budget, 64<<10)
	budget.release(first)
	budget.release(second)

	oldScavenge := memoryScavenge
	calls := 0
	memoryScavenge = func() {
		calls++
		// This lock acquisition would deadlock if production called the
		// process-wide scavenge while holding budget.access.
		budget.access.Lock()
		if budget.pendingReclaim != 128<<10 || budget.used != 128<<10 {
			t.Errorf("physical credit returned before scavenge: pending=%d used=%d", budget.pendingReclaim, budget.used)
		}
		budget.access.Unlock()
	}
	t.Cleanup(func() { memoryScavenge = oldScavenge })

	done := make(chan []byte, 1)
	go func() {
		buffer, _ := budget.tryAcquirePrimary(96 << 10)
		done <- buffer
	}()
	select {
	case buffer := <-done:
		if buffer == nil {
			t.Fatal("allocation did not resume after scavenging detached cache")
		}
		if calls != 1 {
			t.Fatalf("scavenge calls=%d, want 1", calls)
		}
		snapshot := budget.snapshot()
		if snapshot.UsedBytes != 96<<10 || budget.pendingReclaim != 0 || budget.scavenging {
			t.Fatalf("post-scavenge accounting mismatch: snapshot=%+v pending=%d scavenging=%v", snapshot, budget.pendingReclaim, budget.scavenging)
		}
		budget.release(buffer)
	case <-time.After(time.Second):
		t.Fatal("scavenge path blocked on memoryBudget mutex")
	}
}

func TestMemoryBudgetIdleScavengeCoversNonSlabGarbage(t *testing.T) {
	budget := newMemoryBudget(8<<20, false)
	budget.sessions.Store(1)
	if !budget.reserveSession(2 << 20) {
		t.Fatal("fixture reservation failed")
	}
	budget.releaseSession(2 << 20)
	if budget.garbageHintBytes < 2<<20 {
		t.Fatalf("non-slab release did not create reclaim hint: %d", budget.garbageHintBytes)
	}
	oldScavenge := memoryScavenge
	calls := 0
	memoryScavenge = func() { calls++ }
	t.Cleanup(func() { memoryScavenge = oldScavenge })
	budget.access.Lock()
	budget.lastActivity = time.Now().Add(-memoryIdleTrimDelay - time.Second)
	budget.access.Unlock()
	budget.trimIdle(time.Now())
	if calls != 1 || budget.garbageHintBytes != 0 {
		t.Fatalf("idle non-slab scavenge failed: calls=%d hint=%d", calls, budget.garbageHintBytes)
	}
}

func TestMemoryBudgetScavengeSerializesAcrossInstances(t *testing.T) {
	budgets := []*memoryBudget{newMemoryBudget(128<<10, false), newMemoryBudget(128<<10, false)}
	for _, budget := range budgets {
		buffer := acquireTestMemory(t, budget, 64<<10)
		budget.release(buffer)
		budget.access.Lock()
		if got := budget.detachCachedBytesLocked(64 << 10); got != 64<<10 {
			budget.access.Unlock()
			t.Fatalf("detach=%d, want %d", got, 64<<10)
		}
		budget.access.Unlock()
	}

	oldScavenge := memoryScavenge
	var active atomic.Int32
	var peak atomic.Int32
	var calls atomic.Int32
	memoryScavenge = func() {
		calls.Add(1)
		n := active.Add(1)
		for {
			old := peak.Load()
			if n <= old || peak.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		active.Add(-1)
	}
	t.Cleanup(func() { memoryScavenge = oldScavenge })

	var wg sync.WaitGroup
	for _, budget := range budgets {
		wg.Add(1)
		go func(b *memoryBudget) {
			defer wg.Done()
			b.scavengePending(false)
		}(budget)
	}
	wg.Wait()
	if calls.Load() != 2 {
		t.Fatalf("scavenge calls=%d, want 2", calls.Load())
	}
	if peak.Load() != 1 {
		t.Fatalf("process-wide scavenges overlapped: peak=%d", peak.Load())
	}
	for i, budget := range budgets {
		if snapshot := budget.snapshot(); snapshot.UsedBytes != 0 || budget.pendingReclaim != 0 || budget.scavenging {
			t.Fatalf("budget[%d] reclaim incomplete: snapshot=%+v pending=%d scavenging=%v", i, snapshot, budget.pendingReclaim, budget.scavenging)
		}
	}
}

func TestMemoryBudgetReleasedPageKeepsPhysicalCreditUntilScavenge(t *testing.T) {
	const page = int64(32 << 10)
	budget := newMemoryBudget(4*page, false)
	if !budget.reservePage(page, false) {
		t.Fatal("page reservation failed")
	}
	budget.releasePage(page)
	before := budget.snapshot()
	if before.UsedBytes != page || budget.pendingReclaim != page {
		t.Fatalf("released page minted physical credit early: snapshot=%+v pending=%d", before, budget.pendingReclaim)
	}
	oldScavenge := memoryScavenge
	memoryScavenge = func() {}
	t.Cleanup(func() { memoryScavenge = oldScavenge })
	budget.scavengePending(false)
	after := budget.snapshot()
	if after.UsedBytes != 0 || budget.pendingReclaim != 0 {
		t.Fatalf("page credit not returned after scavenge: snapshot=%+v pending=%d", after, budget.pendingReclaim)
	}
}

func TestMemoryBudgetPendingPageCreditScavengedBeforeFreshPageAdmission(t *testing.T) {
	const page = int64(32 << 10)
	budget := newMemoryBudget(2*page, false)
	if !budget.reservePage(page, true) || !budget.reservePage(page, true) {
		t.Fatal("fixture page reservations failed")
	}
	budget.releasePage(page)
	if budget.pendingReclaim != page || budget.snapshot().UsedBytes != 2*page {
		t.Fatalf("released page credit became spendable before scavenge: used=%d pending=%d", budget.snapshot().UsedBytes, budget.pendingReclaim)
	}
	oldScavenge := memoryScavenge
	calls := 0
	memoryScavenge = func() { calls++ }
	t.Cleanup(func() { memoryScavenge = oldScavenge })
	if !budget.reservePage(page, true) {
		t.Fatal("fresh page admission did not reclaim pending physical page credit")
	}
	if calls != 1 {
		t.Fatalf("scavenge calls=%d, want 1", calls)
	}
	if snapshot := budget.snapshot(); snapshot.UsedBytes != 2*page || budget.pendingReclaim != 0 {
		t.Fatalf("post-reclaim page accounting mismatch: snapshot=%+v pending=%d", snapshot, budget.pendingReclaim)
	}
}
