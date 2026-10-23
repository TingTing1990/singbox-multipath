package multipath

import (
	"math"
	"time"
)

// d2OpportunisticLocked is scoped to activated server normal downloads.
// It adapts multipath-beta10 scheduler.opportunisticLocked without changing
// Golden's authoritative stale/lost/pruned repair, sender ACK, framing or
// original Capacity reservation. Caller holds stateMu; at most one repair is
// submitted per invocation and the original mapping remains on leg1.
//
// Only genuinely blocked connection heads may be repaired early. This does
// not treat a path receipt as a logical ACK and does not reclaim a buffer.
func (c *mpCore) d2OpportunisticLocked(now time.Time) (bool, error) {
	if !c.cfg.ServerDownload || !c.active.Load() || c.mappingHead >= len(c.mappings) {
		return false, nil
	}
	if c.cfg.Recovery != nil && (!c.cfg.Recovery.allows(0) || !c.cfg.Recovery.allows(1)) {
		return false, nil
	}
	chunk := uint64(c.cfg.ChunkSize)
	if chunk == 0 {
		return false, nil
	}
	windowBlocked := c.tx.WriteNext > c.tx.Next && c.tx.Next >= c.tx.WindowEnd
	historyLimit := uint64(max(c.cfg.ReplayBytes, int64(c.cfg.ChunkSize)))
	historyBlocked := c.tx.Buffered() >= historyLimit || chunk > historyLimit-c.tx.Buffered()
	head := &c.mappings[c.mappingHead]
	if head.path != 1 || head.seq > c.tx.Una || head.end <= c.tx.Una {
		return false, nil
	}
	slow, fast := c.getLeg(1), c.getLeg(0)
	if slow == nil || fast == nil || slow.path.Generation != head.generation || head.pathEnd <= slow.path.Received {
		return false, nil
	}
	// Beta10's opportunistic repair uses a measured-RTT cooldown, not a
	// permanent ban after the first physical copy. A missing Data ACK may
	// otherwise strand the connection head even after that copy is lost.
	if !head.repairedAt.IsZero() {
		// A physically acknowledged repair can still be waiting for ordered
		// Data ACK. Do not duplicate it after only one RTT. Preserve original
		// RTO recovery if that ACK never arrives.
		if head.repairPathEnd != 0 && head.repairGeneration == fast.path.Generation &&
			fast.path.Received >= head.repairPathEnd &&
			now.Sub(head.repairedAt) < fast.path.RTO(c.cfg.ReplayTimeout) {
			return false, nil
		}
		if now.Sub(head.repairedAt) < fast.path.SRTT {
			return false, nil
		}
	}
	// A missing or non-finite delivery sample cannot justify speculative
	// retransmission, even if the old path has made no progress.
	if fast.busy || !fast.ready.Load() || fast.path.Stale || slow.path.Stale || fast.path.SRTT <= 0 || slow.path.SRTT <= 0 || fast.path.Rate <= 0 || slow.path.Rate <= 0 ||
		math.IsNaN(fast.path.Rate) || math.IsInf(fast.path.Rate, 0) || math.IsNaN(slow.path.Rate) || math.IsInf(slow.path.Rate, 0) || slow.path.LastProgress.IsZero() {
		return false, nil
	}
	// Candidate D2-B HOL extension: normal sender pressure is one trigger;
	// a second, independently grounded trigger requires physical receipt of
	// later logical DATA on leg0 while this leg1 head is still unreceived.
	// An aged but otherwise idle head is NOT sufficient evidence.
	if !windowBlocked && !historyBlocked && !c.d2ConfirmedHOLLocked(head, fast) {
		return false, nil
	}
	// A blocked receive window alone does not justify replaying a newly
	// dispatched healthy booster DATA frame. Wait at least a measured RTT
	// before using the upstream ECF/remaining-delivery comparison.
	if now.Sub(head.sentAt) < max(slow.path.SRTT, fast.path.SRTT) {
		return false, nil
	}
	length := head.end - max(head.seq, c.tx.Una)
	if length == 0 || length > chunk {
		return false, nil
	}
	initial := min(uint64(c.cfg.QueueBytes), chunk*4)
	pipeline := fast.path.Pipeline(initial, uint64(c.cfg.ReplayBytes))
	outstanding := fast.path.Outstanding()
	if outstanding > pipeline || length > pipeline-outstanding {
		return false, nil
	}
	// As in Beta10, distinguish ordinary RTT skew from a head which
	// has made no physical progress for a full RTT. A fresh send is not
	// repaired solely because it was placed on the slower path.
	stuck := now.Sub(slow.path.LastProgress) >= max(slow.path.SRTT, 2*fast.path.SRTT)
	provisionalRate, provisionalDelay := c.beta10ProvisionalLocked()
	fastArrival := fast.path.CompletionTime(int(length), provisionalRate, provisionalDelay, now)
	slowArrival := slow.path.RemainingDelivery(head.pathEnd, head.sentAt, provisionalDelay, now)
	// Beta10 allows a head with no physical progress for a measured RTT
	// to be repaired despite an optimistic stale Rate estimate. The other
	// path must still be eligible and the head old enough. Do not accept
	// NaN/Inf estimates as proof of a late arrival.
	if math.IsNaN(fastArrival) || math.IsInf(fastArrival, 0) || math.IsNaN(slowArrival) || math.IsInf(slowArrival, 0) {
		return false, nil
	}
	late := fastArrival+0.002 < slowArrival
	if !late && !stuck {
		return false, nil
	}
	segment, ok := c.tx.Range(max(head.seq, c.tx.Una), int(length))
	if !ok {
		return false, nil
	}
	// This is original submitLocked repair ownership, never a new logical
	// first transmission. A failing submit remains a normal fatal error.
	if err := c.submitLocked(fast, segment, true, now); err != nil {
		return false, err
	}
	head.repairedAt = now
	head.repairGeneration = fast.path.Generation
	head.repairPathEnd = fast.path.Sent
	// Beta10's RTT-based penalty follows EARLY repair rather than waiting
	// for the RTO/stale event (where the old D2-R1 penalty was ineffective).
	penalty := slow.path.SRTT
	if !stuck && slow.path.MinimumRTT > 0 {
		penalty = slow.path.MinimumRTT
	}
	slow.penaltyUntil = now.Add(penalty)
	c.fallbackB.Add(uint64(segment.Length))
	c.fallbackF.Add(1)
	return true, nil
}

// d2ConfirmedHOLLocked identifies receiver-side head-of-line blocking from
// existing path receipts; it is never inferred from a stale Data ACK alone.
// The mapping must be newer in logical sequence, fully physically received
// on the current preferred generation, while the leg1 head remains missing.
// This is a research-only admission extension, not an upstream Beta10 rule.
// Caller holds stateMu. No new state, wire format, or credit accounting.
func (c *mpCore) d2ConfirmedHOLLocked(head *dataMapping, fast *mpLeg) bool {
	if fast.path.Received == 0 || len(c.mappings)-c.mappingHead < 2 {
		return false
	}
	for i := len(c.mappings) - 1; i > c.mappingHead; i-- {
		later := &c.mappings[i]
		if later.seq < head.end || later.path != 0 ||
			later.generation != fast.path.Generation || later.pathEnd == 0 {
			continue
		}
		if later.pathEnd <= fast.path.Received {
			return true
		}
	}
	return false
}
