package multipath

import (
	"testing"
	"time"

	"github.com/sagernet/sing-box/protocol/multipath/stream"
)

// Exercise the actual Golden Capacity submission wrapper, with only the
// server-side research scope switched on. No stubbed path-selection function.
func d2ECFTestCore() (*mpCore, *mpLeg, *mpLeg) {
	c, p, b := capacitySchedulerCore(nil)
	c.cfg.ServerDownload = true
	c.tx = stream.NewSender(8 << 20)
	c.writerWaiting.Store(true)
	p.path.MinimumRTT = 20 * time.Millisecond
	b.path.MinimumRTT = 100 * time.Millisecond
	return c, p, b
}

func TestD2ECFWeakBusyPreferredReject(t *testing.T) {
	c, p, b := d2ECFTestCore()
	p.busy = true
	p.path.Rate = 100_000_000
	b.path.Rate = 100_000
	p.path.Sent, b.path.Sent = 500_000, 0
	if sel := c.choosePathForSubmitLocked(c.cfg.ChunkSize, time.Now()); sel.leg != nil {
		t.Fatal("weak booster displaced faster temporarily busy preferred")
	}
}

func TestD2ECFHealthyBoosterRemainsEligible(t *testing.T) {
	c, p, b := d2ECFTestCore()
	p.busy = true
	p.path.Rate = 20_000_000 // 160 Mbps preferred
	b.path.Rate = 75_000_000 // 600 Mbps booster
	p.path.MinimumRTT = 65 * time.Millisecond
	b.path.MinimumRTT = 110 * time.Millisecond
	p.path.Sent = 1 << 20
	if sel := c.choosePathForSubmitLocked(c.cfg.ChunkSize, time.Now()); sel.leg != b {
		t.Fatal("healthy 160+600 booster starved")
	}
}

func TestD2ECFLegacyUploadAndInactive(t *testing.T) {
	c, p, b := d2ECFTestCore()
	p.busy = true
	p.path.Rate = 100_000_000
	b.path.Rate = 100_000
	c.cfg.ServerDownload = false
	if sel := c.choosePathForSubmitLocked(c.cfg.ChunkSize, time.Now()); sel.leg != b {
		t.Fatal("upload did not retain Golden choice")
	}
	c.cfg.ServerDownload = true
	c.active.Store(false)
	if sel := c.choosePathForSubmitLocked(c.cfg.ChunkSize, time.Now()); sel.leg != nil {
		t.Fatal("inactive was allowed to use secondary")
	}
}

func TestD2ECFBLESTWindowWait(t *testing.T) {
	c, p, b := d2ECFTestCore()
	p.busy = true
	p.path.Rate = 100_000_000
	b.path.Rate = 10_000_000
	p.path.Sent = 4 << 20
	// Enough outstanding to keep preferred theoretically best, even as its
	// physical writer is busy; a minimal window must block slower new DATA.
	c.tx.WindowEnd = uint64(c.cfg.ChunkSize)
	if sel := c.choosePathForSubmitLocked(c.cfg.ChunkSize, time.Now()); sel.leg != nil {
		t.Fatal("BLEST receiver-window rejection missing")
	}
}

// A healthy server Recovery group is not a failover state; its explicit
// two-path mask must retain normal beta10 aggregate selection. If a path is
// disallowed, the Golden recovery semantics continue to own selection.
func TestD2ECFHealthyRecoveryGroupUsesAggregateSelection(t *testing.T) {
	c, preferred, booster := d2ECFTestCore()
	policy := new(recoveryPolicy)
	policy.mask.Store(3)
	c.cfg.Recovery = policy
	preferred.busy = true
	preferred.path.Rate = 100_000_000
	booster.path.Rate = 100_000
	preferred.path.Sent = 500_000
	if sel := c.choosePathForSubmitLocked(c.cfg.ChunkSize, time.Now()); sel.leg != nil {
		t.Fatal("weak booster was assigned despite healthy two-path recovery group")
	}
	policy.mask.Store(2)
	if sel := c.choosePathForSubmitLocked(c.cfg.ChunkSize, time.Now()); sel.leg != booster {
		t.Fatal("single-path recovery must retain Golden emergency selection")
	}
}
