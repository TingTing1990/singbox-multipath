package multipath

import (
	"math"
	"time"

	D "github.com/sagernet/sing-box/protocol/multipath/downloadevidence"
)

// B10's ECF/BLEST normal-DATA selector, scoped by the caller to active
// server-download only. The B10 scheduling thresholds and best/chosen order
// are preserved; original Golden eligibility (Recovery, memory, peer pressure,
// Pipeline, leg readiness) continues to be enforced before any assignment.
//
// The Golden sender has an unshrinking WindowEnd and its writer waits in
// txLoop/net.Pipe, rather than B10's direct application writer. The latter
// is observed by waitTXSpace without changing the Golden send queue. These
// differences require independent A/B and FIELD validation before promotion.
const beta10MaxSkewRTTs = 8
const beta10WriterBacklogHold = 10 * time.Millisecond

func (c *mpCore) beta10ProvisionalLocked() (rate float64, delay time.Duration) {
	for _, leg := range c.availableLegs() {
		rate = max(rate, leg.path.Rate)
		if leg.path.MinimumRTT > 0 {
			delay = max(delay, leg.path.MinimumRTT)
		} else {
			delay = max(delay, leg.path.SRTT)
		}
	}
	return
}

func (c *mpCore) beta10WriterBacklogged(now time.Time) bool {
	return c.writerWaiting.Load() || now.UnixNano()-c.writerReleasedAt.Load() < int64(beta10WriterBacklogHold)
}

// selectPath: only one physical writer on each path; busy paths are included
// in theoretical earliest completion but can NEVER be selected for submit.
func (c *mpCore) chooseB10ECFLocked(length int, now time.Time, trace *D.Decision) *mpLeg {
	legs := c.availableLegs()
	provisionalRate, provisionalDelay := c.beta10ProvisionalLocked()
	var chosen, best *mpLeg
	chosenTime, bestTime := math.Inf(1), math.Inf(1)
	initial := min(uint64(c.cfg.QueueBytes), uint64(c.cfg.ChunkSize)*4)
	for _, leg := range legs {
		if trace != nil {
			trace.Paths[leg.id].Evaluated = true
		}
		if c.cfg.Recovery != nil && !c.cfg.Recovery.allows(leg.id) {
			if trace != nil {
				trace.Paths[leg.id].Excluded = "recovery"
			}
			continue
		}
		if leg.path.Stale {
			if trace != nil {
				trace.Paths[leg.id].Excluded = "stale"
			}
			continue
		}
		// Beta10 opportunistic repair stops only NEW normal booster DATA for
		// one path RTT. It never blocks existing flights, emergency recovery,
		// or original repair, and is not active on upload/inactive cores.
		if leg.id == 1 && leg.penaltyUntil.After(now) {
			if trace != nil {
				trace.Paths[leg.id].Excluded = "d2_opportunistic_repair_penalty"
			}
			continue
		}
		if leg.id == 1 && (!leg.ready.Load() || c.peerPressure || !c.memory.boosterAllowed()) {
			if trace != nil {
				trace.Paths[leg.id].Excluded = "activation_ready_peer_or_memory"
			}
			continue
		}
		completion := leg.path.CompletionTime(length, provisionalRate, provisionalDelay, now)
		if completion < bestTime {
			best, bestTime = leg, completion
		}
		if leg.busy {
			if trace != nil {
				trace.Paths[leg.id].Excluded = "busy"
			}
			continue
		}
		if leg.path.Outstanding()+uint64(length) > leg.path.Pipeline(initial, uint64(c.cfg.ReplayBytes)) {
			if trace != nil {
				trace.Paths[leg.id].Excluded = "pipeline_full"
			}
			continue
		}
		if completion < chosenTime {
			chosen, chosenTime = leg, completion
		}
	}
	if chosen == nil || chosen == best || best == nil {
		if trace != nil {
			trace.Reason = "b10_earliest_completion"
		}
		return chosen
	}
	// Compatibility guard for Golden's unshrinking receiver window and
	// net.Pipe-backed application writer. Do not turn an immediately eligible
	// preferred assignment into WAIT solely because the theoretical faster
	// booster is currently owned by its single child writer. That WAIT would
	// bypass Golden's natural preferred Capacity V2 reservation and could
	// starve an otherwise usable leg0. Busy is still never bypassed for submit.
	bestPipeline := best.path.Pipeline(initial, uint64(c.cfg.ReplayBytes))
	bestOutstanding := best.path.Outstanding()
	bestPipelineFull := bestOutstanding > bestPipeline || uint64(length) > bestPipeline-bestOutstanding
	if chosen.id == 0 && best.id == 1 && (best.busy || bestPipelineFull) {
		if trace != nil {
			if best.busy {
				trace.Reason = "b10_preferred_available_busy_booster"
			} else {
				trace.Reason = "b10_preferred_available_pipeline_full_booster"
			}
		}
		return chosen
	}
	bestRate := best.path.Rate
	if bestRate <= 0 {
		bestRate = provisionalRate
	}
	// Original beta10 receiver-window BLEST check. Never underflow when
	// Golden's window right edge equals/below Next (or uint64 is saturated).
	futureNext := c.tx.Next + uint64(length)
	if futureNext < c.tx.Next {
		futureNext = math.MaxUint64
	}
	room := c.tx.WindowEnd - min(c.tx.WindowEnd, futureNext)
	if bestRate*(chosenTime-bestTime) > float64(room) {
		if trace != nil {
			trace.Reason = "b10_blest_window_wait"
		}
		return nil
	}
	rtt := chosen.path.MinimumRTT
	if rtt == 0 {
		rtt = max(chosen.path.SRTT, provisionalDelay)
	}
	if chosenTime-bestTime > beta10MaxSkewRTTs*rtt.Seconds() {
		if trace != nil {
			trace.Reason = "b10_rtt_skew_wait"
		}
		return nil
	}
	pending := c.tx.WriteNext - min(c.tx.Next, c.tx.WriteNext)
	limit := uint64(max(c.cfg.QueueBytes, int64(c.cfg.ChunkSize)))
	backlogged := c.beta10WriterBacklogged(now) || pending+uint64(c.cfg.ChunkSize) > limit
	if c.tx.HasFIN || !backlogged {
		drain := bestTime + float64(pending-min(pending, uint64(length)))/max(bestRate, 1)
		if chosenTime > drain+0.002 {
			if trace != nil {
				trace.Reason = "b10_tail_wait"
			}
			return nil
		}
	}
	if trace != nil {
		trace.Reason = "b10_healthy_parallel"
	}
	return chosen
}
