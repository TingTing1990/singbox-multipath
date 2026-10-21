package multipath

import (
	"math"
	"testing"
	"time"

	"github.com/sagernet/sing-box/protocol/multipath/stream"
)

// The original R2 pattern was a leg1 first transmission which continued to
// make intermittent physical progress, postponing RTO-stale repair while
// delaying the connection head. Compare the pre-repair Golden branch (not
// stale) and the actual new core function on the same live sender/Path state.
func d2EarlyRepairFixture(t *testing.T, now time.Time, preferredRate, boosterRate float64, sentAge time.Duration) (*mpCore, *mpLeg, *mpLeg) {
	t.Helper()
	c, preferred, booster := d2ECFTestCore()
	c.cfg.ServerDownload = true
	c.tx = stream.NewSender(uint64(c.cfg.ChunkSize))
	chunk := c.cfg.ChunkSize
	for i := 0; i < 2; i++ {
		if err := c.tx.Append(stream.NewBuffer(make([]byte, chunk), nil)); err != nil {
			t.Fatal(err)
		}
	}
	head, ok := c.tx.NextRange(chunk)
	if !ok {
		t.Fatal("missing connection head")
	}
	if err := c.tx.Sent(head); err != nil {
		t.Fatal(err)
	}
	preferred.send = make(chan wireFrame, 1)
	booster.send = make(chan wireFrame, 1)
	preferred.path.Rate, booster.path.Rate = preferredRate, boosterRate
	preferred.path.SRTT = 65 * time.Millisecond
	preferred.path.MinimumRTT = 65 * time.Millisecond
	booster.path.SRTT = 110 * time.Millisecond
	booster.path.MinimumRTT = 110 * time.Millisecond
	booster.path.LastProgress = now.Add(-sentAge)
	pathSeq, err := booster.path.Submitted(chunk, now.Add(-sentAge))
	if err != nil {
		t.Fatal(err)
	}
	c.mappings = []dataMapping{{seq: head.Seq, end: head.End(), path: 1, generation: booster.path.Generation, pathEnd: pathSeq + uint64(chunk), sentAt: now.Add(-sentAge)}}
	t.Cleanup(func() {
		select {
		case f := <-preferred.send:
			f.buffer.Release()
		default:
		}
		// No real writer is started in this isolated core. Return retained frames.
		c.tx.Close()
		preferred.path.Close()
		booster.path.Close()
	})
	return c, preferred, booster
}

func TestD2EarlyRepairBeforeStaleAtBlockedLogicalHead(t *testing.T) {
	now := time.Now()
	c, preferred, booster := d2EarlyRepairFixture(t, now, 20e6, 200_000, 400*time.Millisecond)
	if sent, err := c.reinjectLocked(now); err != nil || sent {
		t.Fatalf("Golden repair must still wait for stale/loss: sent=%v err=%v", sent, err)
	}
	if booster.path.Stale {
		t.Fatal("test needs non-stale physical leg1")
	}
	if sent, err := c.d2OpportunisticLocked(now); err != nil || !sent {
		t.Fatalf("no early head repair: sent=%v err=%v", sent, err)
	}
	if preferred.path.Sent != uint64(c.cfg.ChunkSize) {
		t.Fatal("preferred did not receive one repair physical flight")
	}
	if c.tx.Next != uint64(c.cfg.ChunkSize) {
		t.Fatal("repair advanced normal logical sequence")
	}
	if c.mappings[0].path != 1 || c.mappings[0].repairedAt.IsZero() {
		t.Fatal("original leg1 mapping must be retained with cooldown")
	}
	if !booster.penaltyUntil.After(now) {
		t.Fatal("successful early repair did not suppress new normal leg1")
	}
	if sent, err := c.d2OpportunisticLocked(now.Add(time.Millisecond)); err != nil || sent {
		t.Fatal("duplicate repair within cooldown")
	}
}

func TestD2EarlyRepairHealthyFastBoosterNotPenalized(t *testing.T) {
	now := time.Now()
	c, _, booster := d2EarlyRepairFixture(t, now, 20e6, 75e6, 40*time.Millisecond)
	if sent, err := c.d2OpportunisticLocked(now); err != nil || sent {
		t.Fatalf("healthy fast booster incorrectly repaired: sent=%v err=%v", sent, err)
	}
	if !booster.penaltyUntil.IsZero() {
		t.Fatal("healthy booster penalized")
	}
}

func TestD2EarlyRepairScopeAndFreshHead(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name           string
		server, active bool
		age            time.Duration
	}{
		{"inactive", true, false, time.Second}, {"upload", false, true, time.Second}, {"fresh_head", true, true, 10 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _, _ := d2EarlyRepairFixture(t, now, 20e6, 200_000, tc.age)
			c.cfg.ServerDownload = tc.server
			c.active.Store(tc.active)
			if sent, err := c.d2OpportunisticLocked(now); err != nil || sent {
				t.Fatalf("out-of-scope repair sent=%v err=%v", sent, err)
			}
		})
	}
}

func TestD2EarlyRepairNoRateSampleOrCompletedPhysicalHead(t *testing.T) {
	now := time.Now()
	t.Run("unknown_booster_rate", func(t *testing.T) {
		c, _, booster := d2EarlyRepairFixture(t, now, 20e6, 0, time.Second)
		if sent, err := c.d2OpportunisticLocked(now); err != nil || sent {
			t.Fatalf("startup path repaired without delivery estimate: sent=%v err=%v", sent, err)
		}
		if !booster.penaltyUntil.IsZero() {
			t.Fatal("startup booster penalized")
		}
	})
	t.Run("head_already_path_confirmed", func(t *testing.T) {
		c, _, booster := d2EarlyRepairFixture(t, now, 20e6, 200_000, time.Second)
		booster.path.Received = c.mappings[0].pathEnd
		if sent, err := c.d2OpportunisticLocked(now); err != nil || sent {
			t.Fatalf("path-received bytes redundantly repaired: sent=%v err=%v", sent, err)
		}
	})
}

func TestD2EarlyRepairDoesNotConsumePreferredCapacityReservation(t *testing.T) {
	now := time.Now()
	c, _, _ := d2EarlyRepairFixture(t, now, 20e6, 200_000, 400*time.Millisecond)
	controller := newPreferredCapacityController(60, time.Second, c.cfg.ChunkSize)
	c.cfg.PreferredCapacity = controller
	before := controller.snapshot().AssignedBytes
	sent, err := c.d2OpportunisticLocked(now)
	if err != nil || !sent {
		t.Fatalf("expected physical early repair: sent=%v err=%v", sent, err)
	}
	if got := controller.snapshot().AssignedBytes; got != before {
		t.Fatalf("repair incorrectly consumed normal preferred Capacity reservation: before=%d after=%d", before, got)
	}
}

// Regression: an opportunistic repair is not a one-shot operation. If the
// first preferred physical copy has not advanced the logical ACK, the
// original unacknowledged head remains eligible after one preferred RTT.
func TestD2EarlyRepairRetriesAfterMeasuredRTT(t *testing.T) {
	now := time.Now()
	c, preferred, _ := d2EarlyRepairFixture(t, now, 20e6, 200_000, 400*time.Millisecond)
	if sent, err := c.d2OpportunisticLocked(now); err != nil || !sent {
		t.Fatalf("first repair: sent=%v err=%v", sent, err)
	}
	f := <-preferred.send
	f.buffer.Release() // Simulate completed transport write, but no Data ACK.
	preferred.queuedBytes.Add(-int64(len(f.data)))
	preferred.busy = false
	before := preferred.path.Sent
	if sent, err := c.d2OpportunisticLocked(now.Add(preferred.path.SRTT - time.Millisecond)); err != nil || sent {
		t.Fatalf("premature retry within cooldown: sent=%v err=%v", sent, err)
	}
	if sent, err := c.d2OpportunisticLocked(now.Add(preferred.path.SRTT + time.Millisecond)); err != nil || !sent {
		t.Fatalf("repair missing after RTT despite blocked head: sent=%v err=%v", sent, err)
	}
	if preferred.path.Sent != before+uint64(c.cfg.ChunkSize) {
		t.Fatalf("retry physical flight not submitted: sent=%d before=%d", preferred.path.Sent, before)
	}
	if c.tx.Una != 0 || c.tx.Next != uint64(c.cfg.ChunkSize) {
		t.Fatal("retry wrongly moved logical ACK or first-transmission cursor")
	}
}

// Source-semantic regression: the real Beta10 stuck branch must not be
// overridden by an unrealistically high rate learned before a long stall.
func TestD2EarlyRepairStuckDespiteOldOptimisticRate(t *testing.T) {
	now := time.Now()
	c, _, booster := d2EarlyRepairFixture(t, now, 20e6, 500e6, 400*time.Millisecond)
	booster.path.MinimumRTT = 5 * time.Millisecond
	if sent, err := c.d2OpportunisticLocked(now); err != nil || !sent {
		t.Fatalf("stuck head with old optimistic rate was not repaired: sent=%v err=%v", sent, err)
	}
}

func TestD2EarlyRepairRejectsNonFiniteRate(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name       string
		fast, slow float64
	}{
		{"preferred_nan", math.NaN(), 200_000},
		{"preferred_inf", math.Inf(1), 200_000},
		{"booster_nan", 20e6, math.NaN()},
		{"booster_inf", 20e6, math.Inf(1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _, booster := d2EarlyRepairFixture(t, now, tc.fast, tc.slow, time.Second)
			if sent, err := c.d2OpportunisticLocked(now); err != nil || sent {
				t.Fatalf("invalid Rate allowed speculative repair: sent=%v err=%v", sent, err)
			}
			if !booster.penaltyUntil.IsZero() {
				t.Fatal("invalid Rate penalized a booster")
			}
		})
	}
}

// A physically confirmed speculative copy must not be sent again solely
// because the connection-level ordered Data ACK is delayed.
func TestD2EarlyRepairPhysicalReceiptDefersDuplicateUntilRTO(t *testing.T) {
	now := time.Now()
	c, preferred, _ := d2EarlyRepairFixture(t, now, 20e6, 200_000, 400*time.Millisecond)
	if sent, err := c.d2OpportunisticLocked(now); err != nil || !sent {
		t.Fatalf("first repair: sent=%v err=%v", sent, err)
	}
	frame := <-preferred.send
	frame.buffer.Release()
	preferred.queuedBytes.Add(-int64(len(frame.data)))
	preferred.busy = false
	originalEnd := preferred.path.Sent
	if c.mappings[0].repairGeneration != preferred.path.Generation || c.mappings[0].repairPathEnd != originalEnd {
		t.Fatal("speculative physical flight identity not retained")
	}
	preferred.path.Received = originalEnd // path receipt, not Data ACK
	if sent, err := c.d2OpportunisticLocked(now.Add(preferred.path.SRTT + time.Millisecond)); err != nil || sent {
		t.Fatalf("physically confirmed repair duplicated before RTO: sent=%v err=%v", sent, err)
	}
	if preferred.path.Sent != originalEnd || c.tx.Una != 0 {
		t.Fatal("path receipt changed logical ACK or submitted an extra repair")
	}
}

// No observed sender pressure means the upstream opportunistic trigger is
// closed, even if the logical head is old and the weak path has stopped.
func TestD2EarlyRepairDoesNotSpeculateWithoutSenderPressure(t *testing.T) {
	now := time.Now()
	c, _, booster := d2EarlyRepairFixture(t, now, 20e6, 200_000, time.Second)
	c.tx.WindowEnd = c.tx.WriteNext + uint64(c.cfg.ChunkSize)*8
	if sent, err := c.d2OpportunisticLocked(now); err != nil || sent {
		t.Fatalf("unblocked sender unexpectedly repaired: sent=%v err=%v", sent, err)
	}
	if !booster.penaltyUntil.IsZero() {
		t.Fatal("unblocked sender spuriously penalized booster")
	}
}
