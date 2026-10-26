package multipath

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/sing-box/protocol/multipath/audit"
	D "github.com/sagernet/sing-box/protocol/multipath/downloadevidence"
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

// Compare the real submitted physical frame with the encoded producer journal
// and offline accounting. Internal counters alone would miss a producer which
// silently omits both the DECISION event and its cumulative repair total.
func TestD2EarlyRepairAuditTrail(t *testing.T) {
	for _, tc := range []struct {
		name, reason string
		fail, reject bool
	}{
		{name: "window", reason: "d2_early_repair_window"},
		{name: "history", reason: "d2_early_repair_history"},
		{name: "confirmed_hol", reason: "d2_early_repair_confirmed_hol"},
		{name: "submit_failure", reason: "d2_early_repair_window", fail: true},
		{name: "upload_out_of_scope", reject: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			c, preferred, booster := d2EarlyRepairFixture(t, now, 20e6, 200_000, 400*time.Millisecond)
			if tc.name == "history" {
				c.tx.WindowEnd = c.tx.WriteNext + uint64(c.cfg.ChunkSize)*8
				c.cfg.ReplayBytes = int64(c.tx.Buffered())
			}
			if tc.name == "confirmed_hol" {
				c.tx.WindowEnd = c.tx.WriteNext + uint64(c.cfg.ChunkSize)*8
				tail, ok := c.tx.NextRange(c.cfg.ChunkSize)
				if !ok {
					t.Fatal("missing later DATA")
				}
				if err := c.tx.Sent(tail); err != nil {
					t.Fatal(err)
				}
				pathSeq, err := preferred.path.Submitted(tail.Length, now.Add(-200*time.Millisecond))
				if err != nil {
					t.Fatal(err)
				}
				c.mappings = append(c.mappings, dataMapping{seq: tail.Seq, end: tail.End(), path: 0,
					generation: preferred.path.Generation, pathEnd: pathSeq + uint64(tail.Length), sentAt: now.Add(-200 * time.Millisecond)})
				if err := preferred.path.Feedback(stream.Receipt{Generation: preferred.path.Generation,
					Next: pathSeq + uint64(tail.Length), ReceivedAt: 500000}, now.Add(-50*time.Millisecond)); err != nil {
					t.Fatal(err)
				}
			}
			if tc.fail {
				c.memory = newMemoryBudget(1, false)
				preferred.prepaidFlights = 16 // Exhaust the existing reserved fallback too.
			}
			if tc.reject {
				c.cfg.ServerDownload = false
			}
			c.cfg.Memory = c.memory
			c.cfg.DownloadSession = "d2-audit-fixture"
			var output bytes.Buffer
			observer, err := newDownloadAudit("d2-audit", c.cfg, func(s string) { fmt.Fprintln(&output, s) })
			if err != nil {
				t.Fatal(err)
			}
			observer.start()
			t.Cleanup(func() { observer.close(); <-observer.done })
			c.cfg.DownloadAudit = observer
			beforeNext, beforeUna := c.tx.Next, c.tx.Una
			sent, err := c.d2OpportunisticLocked(now)
			if tc.fail {
				if sent || err != errMemoryLimit {
					t.Fatalf("injected submission failure: sent=%v err=%v", sent, err)
				}
			} else if err != nil || sent == tc.reject {
				t.Fatalf("unexpected submission: sent=%v err=%v", sent, err)
			}
			var physical wireFrame
			if sent {
				physical = <-preferred.send
				defer physical.buffer.Release()
				preferred.queuedBytes.Add(-int64(len(physical.data)))
				preferred.busy = false
				if again, err := c.d2OpportunisticLocked(now.Add(time.Millisecond)); again || err != nil {
					t.Fatalf("cooldown allowed another submission: sent=%v err=%v", again, err)
				}
			} else if len(preferred.send) != 0 || c.fallbackB.Load() != 0 || !booster.penaltyUntil.IsZero() {
				t.Fatal("failed/rejected assignment changed successful repair state")
			}
			if c.tx.Next != beforeNext || c.tx.Una != beforeUna {
				t.Fatal("repair moved logical first-transmission or ACK cursor")
			}
			// Give START and STOP a real interval on coarse platform clocks.
			time.Sleep(time.Millisecond)
			observer.close()
			<-observer.done
			report, err := audit.AnalyzeServerDownload(strings.NewReader(output.String()), "d2-audit", "")
			if err != nil || !report.Complete {
				t.Fatalf("producer journal did not replay completely: %v", err)
			}
			var decisions []D.Decision
			var final D.Snapshot
			for _, event := range report.Events {
				if event.Kind == "DECISION" {
					var d D.Decision
					if err := json.Unmarshal(event.Data, &d); err != nil {
						t.Fatal(err)
					}
					decisions = append(decisions, d)
				}
				if event.Kind == "STOP" {
					if err := json.Unmarshal(event.Data, &final); err != nil {
						t.Fatal(err)
					}
				}
			}
			if tc.reject {
				if len(decisions) != 0 || final.Totals != (D.Totals{}) {
					t.Fatal("out-of-scope rejection fabricated an assignment")
				}
				return
			}
			if len(decisions) != 1 {
				t.Fatalf("actual D2 submission attempt missing from journal: decisions=%d want=1", len(decisions))
			}
			d := decisions[0]
			if !d.Repair || d.Selected != 0 || d.Candidate != 0 || d.Reason != tc.reason || d.Paths[0].Busy {
				t.Fatalf("wrong repair identity or snapshot taken after submission: %+v", d)
			}
			want := D.Totals{Decisions: 1}
			if tc.fail {
				want.SubmitFailures = 1
				if d.Outcome != "failed" || d.Length != c.cfg.ChunkSize || d.LogicalSeq != beforeUna {
					t.Fatalf("failed attempt misreported: %+v", d)
				}
			} else {
				want.AssignedRepair[0] = uint64(len(physical.data))
				if d.Outcome != "submitted" || !physical.replay || d.Length != len(physical.data) ||
					d.LogicalSeq != physical.seq || d.Paths[0].Generation != physical.generation {
					t.Fatalf("journal differs from actual queued repair: %+v", d)
				}
			}
			if final.Totals != want {
				t.Fatalf("repair counters differ from physical submission: got=%+v want=%+v", final.Totals, want)
			}
		})
	}
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

// D2-B regression: a later logical mapping is physically received on leg0,
// while the earlier leg1 head remains physically unreceived and Data ACK
// remains stalled. The sender has spare window and replay capacity.
// Unlike an aged but merely idle head, this exhibits receiver-side HOL evidence.
func TestD2UnblockedHOLLaterPathReceiptRequiresEarlyRepair(t *testing.T) {
	now := time.Now()
	c, preferred, booster := d2EarlyRepairFixture(t, now, 20e6, 200_000, 400*time.Millisecond)
	c.tx.WindowEnd = c.tx.WriteNext + uint64(c.cfg.ChunkSize)*8
	tail, ok := c.tx.NextRange(c.cfg.ChunkSize)
	if !ok {
		t.Fatal("second mapping missing")
	}
	if err := c.tx.Sent(tail); err != nil {
		t.Fatal(err)
	}
	pathSeq, err := preferred.path.Submitted(tail.Length, now.Add(-200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	c.mappings = append(c.mappings, dataMapping{
		seq: tail.Seq, end: tail.End(), path: 0, generation: preferred.path.Generation,
		pathEnd: pathSeq + uint64(tail.Length), sentAt: now.Add(-200 * time.Millisecond),
	})
	if err := preferred.path.Feedback(stream.Receipt{
		Generation: preferred.path.Generation, Next: pathSeq + uint64(tail.Length),
		ReceivedAt: 500000, FirstNext: 0,
	}, now.Add(-50*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if c.tx.Una != 0 || c.tx.Next != c.tx.WriteNext || c.tx.Next >= c.tx.WindowEnd {
		t.Fatal("fixture must have stale ordered ACK, no unsent data, open send window")
	}
	if c.tx.Buffered() >= uint64(c.cfg.ReplayBytes) {
		t.Fatal("fixture must have spare replay history")
	}
	if booster.path.Received >= c.mappings[0].pathEnd {
		t.Fatal("head is already path confirmed")
	}
	sent, err := c.d2OpportunisticLocked(now)
	if err != nil || !sent {
		t.Fatalf("physical later DATA arrived but unreceived head did not repair: sent=%v err=%v", sent, err)
	}
}

// Negative companion: elapsed time without physical later-data evidence is
// not sufficient to speculate on an otherwise unblocked sender.
func TestD2UnblockedHOLWithoutLaterReceiptMustNotRepair(t *testing.T) {
	now := time.Now()
	c, _, _ := d2EarlyRepairFixture(t, now, 20e6, 200_000, 400*time.Millisecond)
	c.tx.WindowEnd = c.tx.WriteNext + uint64(c.cfg.ChunkSize)*8
	if sent, err := c.d2OpportunisticLocked(now); err != nil || sent {
		t.Fatalf("no downstream HOL evidence: sent=%v err=%v", sent, err)
	}
}

// Even with physical progress on the preferred leg, only a fully received
// later DATA mapping in the same path generation proves this specific HOL gap.
func TestD2UnblockedHOLRejectsPartialOrOldGenerationReceipt(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		oldGeneration, partial bool
	}{
		{name: "later_partial_receipt", partial: true},
		{name: "later_old_generation", oldGeneration: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			c, preferred, _ := d2EarlyRepairFixture(t, now, 20e6, 200_000, 400*time.Millisecond)
			c.tx.WindowEnd = c.tx.WriteNext + uint64(c.cfg.ChunkSize)*8
			tail, ok := c.tx.NextRange(c.cfg.ChunkSize)
			if !ok {
				t.Fatal("missing tail")
			}
			if err := c.tx.Sent(tail); err != nil {
				t.Fatal(err)
			}
			pathSeq, err := preferred.path.Submitted(tail.Length, now.Add(-200*time.Millisecond))
			if err != nil {
				t.Fatal(err)
			}
			generation := preferred.path.Generation
			if tc.oldGeneration {
				generation++
			}
			c.mappings = append(c.mappings, dataMapping{
				seq: tail.Seq, end: tail.End(), path: 0, generation: generation,
				pathEnd: pathSeq + uint64(tail.Length), sentAt: now.Add(-200 * time.Millisecond),
			})
			receipt := pathSeq + uint64(tail.Length)
			if tc.partial {
				receipt--
			}
			if err := preferred.path.Feedback(stream.Receipt{Generation: preferred.path.Generation, Next: receipt, ReceivedAt: 500000}, now.Add(-50*time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			if sent, err := c.d2OpportunisticLocked(now); err != nil || sent {
				t.Fatalf("unproven HOL caused repair: sent=%v err=%v", sent, err)
			}
		})
	}
}
