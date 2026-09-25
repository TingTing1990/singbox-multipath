package multipath

import (
	"testing"
	"time"

	"github.com/sagernet/sing-box/protocol/multipath/stream"
)

// withBlockedBatchGC replaces only the process-wide GC boundary. Production
// allocation, detach, batching, accounting and wakeup code remain unchanged.
func withBlockedBatchGC(t *testing.T) <-chan chan struct{} {
	t.Helper()
	original := memoryScavenge
	started := make(chan chan struct{}, 8)
	memoryScavenge = func() {
		gate := make(chan struct{})
		started <- gate
		<-gate
	}
	t.Cleanup(func() { memoryScavenge = original })
	return started
}

func waitReclaimState(t *testing.T, b *memoryBudget, wantUsed int64, wantCompleted uint64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		b.access.Lock()
		used, completed := b.used, b.reclaimCompleted
		b.access.Unlock()
		if used == wantUsed && completed >= wantCompleted {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("reclaim state timeout: used=%d completed=%d want_used=%d want_completed=%d", used, completed, wantUsed, wantCompleted)
		}
		time.Sleep(time.Millisecond)
	}
}

// Regression for the confirmed stale-queue/lost-wakeup failure: a release that
// arrives while GC batch 1 is running belongs to batch 2. Batch 1 must not
// credit it, and batch 2 must start without a queued boolean or a later nudge.
func TestPhysicalCreditReclaimDuringGCStartsNextBatch(t *testing.T) {
	started := withBlockedBatchGC(t)
	b := newMemoryBudget(1600, false)
	b.cacheLimit = 0
	b.maintenanceScheduled = true // pressure below starts batches explicitly

	first, _ := b.tryAcquirePrimary(800)
	second, _ := b.tryAcquirePrimary(400)
	if first == nil || second == nil {
		t.Fatal("fixture allocation failed")
	}
	b.release(first)
	first = nil

	if fresh, _ := b.tryAcquirePrimary(800); fresh != nil {
		t.Fatal("physical credit was reused before batch 1 GC")
	}
	var gate1 chan struct{}
	select {
	case gate1 = <-started:
	case <-time.After(time.Second):
		t.Fatal("batch 1 did not start")
	}

	b.release(second)
	second = nil
	b.access.Lock()
	if b.pendingReclaim != 400 || b.reclaimIssued != 2 || !b.reclaimRunning {
		t.Fatalf("release during GC not isolated into next batch: pending=%d issued=%d running=%v", b.pendingReclaim, b.reclaimIssued, b.reclaimRunning)
	}
	b.access.Unlock()

	close(gate1)
	var gate2 chan struct{}
	select {
	case gate2 = <-started:
	case <-time.After(time.Second):
		t.Fatal("batch 2 did not start after batch 1")
	}
	b.access.Lock()
	used, pending, completed, running := b.used, b.pendingReclaim, b.reclaimCompleted, b.reclaimRunning
	b.access.Unlock()
	if used != 400 || pending != 0 || completed != 1 || !running {
		t.Fatalf("batch 1 credited bytes outside its cutoff: used=%d pending=%d completed=%d running=%v", used, pending, completed, running)
	}

	close(gate2)
	waitReclaimState(t, b, 0, 2)
}

// A close captures an epoch cutoff. Releases from other live traffic after that
// point may form another batch but cannot extend the close boundary.
func TestPhysicalCreditCloseWaitsOnlyCapturedCutoff(t *testing.T) {
	started := withBlockedBatchGC(t)
	b := newMemoryBudget(2048, false)
	b.cacheLimit = 0
	b.maintenanceScheduled = true

	first, _ := b.tryAcquirePrimary(500)
	second, _ := b.tryAcquirePrimary(500)
	if first == nil || second == nil {
		t.Fatal("fixture allocation failed")
	}
	b.release(first)
	first = nil
	cutoff := b.reclaimCutoff()
	if cutoff != 1 {
		t.Fatalf("unexpected first cutoff: %d", cutoff)
	}
	var gate1 chan struct{}
	select {
	case gate1 = <-started:
	case <-time.After(time.Second):
		t.Fatal("first cutoff GC did not start")
	}

	// This is deliberately after the close boundary was captured.
	b.release(second)
	second = nil
	done := make(chan struct{})
	go func() {
		b.waitReclaim(cutoff)
		close(done)
	}()
	close(gate1)

	var gate2 chan struct{}
	select {
	case gate2 = <-started:
	case <-time.After(time.Second):
		t.Fatal("future batch did not start")
	}
	select {
	case <-done:
		// Correct: cutoff 1 completed even though cutoff 2 is still blocked.
	case <-time.After(time.Second):
		t.Fatal("close cutoff was extended by later traffic")
	}
	close(gate2)
	waitReclaimState(t, b, 0, 2)
}

// PageMemory is a data-plane path. Under pressure it may start an asynchronous
// reclaim batch, but it must return before process-wide GC completes.
func TestPhysicalCreditPageAdmissionNeverWaitsForGC(t *testing.T) {
	started := withBlockedBatchGC(t)
	b := newMemoryBudget(1024, false)
	b.cacheLimit = 0
	b.maintenanceScheduled = true
	buffer, _ := b.tryAcquirePrimary(800)
	if buffer == nil {
		t.Fatal("fixture allocation failed")
	}
	b.release(buffer)
	buffer = nil

	result := make(chan bool, 1)
	go func() { result <- b.reservePage(300, true) }()
	select {
	case admitted := <-result:
		if admitted {
			t.Fatal("page admitted above physical limit")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("PageMemory waited for process-wide GC")
	}
	var gate chan struct{}
	select {
	case gate = <-started:
	case <-time.After(time.Second):
		t.Fatal("PageMemory did not start asynchronous reclaim")
	}
	close(gate)
	waitReclaimState(t, b, 0, 1)
}

// Regression for the confirmed early-credit bug. The real Sender ACK path must
// remove both the segment owner and Buffer.Data before a reclaim batch may run;
// while that batch is blocked, its physical credit remains unavailable.
func TestPhysicalCreditSenderDetachBeforeBatchCredit(t *testing.T) {
	started := withBlockedBatchGC(t)
	b := newMemoryBudget(1024, false)
	b.cacheLimit = 0
	b.maintenanceScheduled = true

	payload, _ := b.tryAcquirePrimary(1024)
	if payload == nil {
		t.Fatal("fixture allocation failed")
	}
	managed := stream.NewManagedBuffer(payload, b.prepareRelease)
	payload = nil
	sender := stream.NewSender(1 << 20)
	if err := sender.Append(managed); err != nil {
		t.Fatal(err)
	}
	segment, ok := sender.NextRange(1024)
	if !ok {
		t.Fatal("missing send range")
	}
	if err := sender.Sent(segment); err != nil {
		t.Fatal(err)
	}
	if err := sender.Acknowledge(sender.Next, sender.WindowEnd); err != nil {
		t.Fatal(err)
	}
	if managed.Data != nil {
		t.Fatal("Buffer still owns backing after ACK detach")
	}
	if fresh, _ := b.tryAcquirePrimary(1024); fresh != nil {
		t.Fatal("credit was reusable before detached backing completed its GC batch")
	}
	var gate chan struct{}
	select {
	case gate = <-started:
	case <-time.After(time.Second):
		t.Fatal("reclaim batch did not start")
	}
	b.access.Lock()
	used, running := b.used, b.reclaimRunning
	b.access.Unlock()
	if used != 1024 || !running {
		t.Fatalf("credit changed while GC batch blocked: used=%d running=%v", used, running)
	}
	close(gate)
	waitReclaimState(t, b, 0, 1)
	fresh, _ := b.tryAcquirePrimary(1024)
	if fresh == nil {
		t.Fatal("credit did not become reusable after the owning GC batch completed")
	}
}
