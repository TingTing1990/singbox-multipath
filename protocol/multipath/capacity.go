package multipath

import (
	"sync"
	"time"
)

const (
	preferredCapacityDebtChunks    = 2
	preferredCapacitySurplusChunks = 4
)

// preferredCapacityController belongs to exactly one configured multipath
// inbound/outbound instance and is shared by every logical TCP core created by
// that instance. The target is therefore aggregate path capacity, never a
// per-session target.
//
// The controller never owns a core and never acquires a core lock. Core code may
// call it while holding stateMu, so the only permitted lock direction is
// core.stateMu -> preferredCapacityController.mu.
type preferredCapacityController struct {
	targetBytesPS uint64
	window        time.Duration
	chunkSize     int

	mu sync.Mutex

	// Peer-confirmed normal first-transmission leg0 delivery aggregated across
	// all logical sessions. Repair/reinjection bytes are excluded before they
	// reach this controller.
	delivered uint64

	// Completed-window delivery sampler. This is used only as activation
	// authority; local transport Write completion is deliberately excluded.
	deliveryStart time.Time
	deliveryBase  uint64
	deliveryRate  uint64
	deliveryReady bool

	// Shared normal-DATA assignment reservation. Positive credit means preferred
	// is owed assignment; negative credit is a bounded natural surplus.
	creditLast time.Time
	credit     float64
}

type preferredCapacityReservation struct {
	controller *preferredCapacityController
	bytes      int
}

type preferredCapacitySnapshot struct {
	TargetBytesPS  uint64
	DeliveredBytes uint64
	DeliveryRate   uint64
	DeliveryReady  bool
	CreditBytes    int64
}

// preferredCapacityPathRange is per-leg receipt bookkeeping, not capacity
// ownership. It records only normal first-transmission DATA ranges in the leg0
// path sequence so peer receipts can exclude reinjection/repair bytes.
type preferredCapacityPathRange struct {
	next uint64
	end  uint64
}

func newPreferredCapacityController(mbps uint32, window time.Duration, chunkSize int) *preferredCapacityController {
	if mbps == 0 {
		return nil
	}
	if window <= 0 {
		window = time.Second
	}
	if chunkSize <= 0 {
		chunkSize = 64 << 10
	}
	return &preferredCapacityController{
		targetBytesPS: uint64(mbps) * 1_000_000 / 8,
		window:        window,
		chunkSize:     chunkSize,
	}
}

func (c *preferredCapacityController) target() uint64 {
	if c == nil {
		return 0
	}
	return c.targetBytesPS
}

func (c *preferredCapacityController) targetMbps() uint64 {
	if c == nil {
		return 0
	}
	return c.targetBytesPS * 8 / 1_000_000
}

// observePreferredDelivery records peer-confirmed normal first-transmission
// delivery. Calls from different cores may carry timestamps captured before lock
// acquisition, so shared time is never allowed to move backwards.
func (c *preferredCapacityController) observePreferredDelivery(bytes uint64, now time.Time) {
	if c == nil || bytes == 0 {
		return
	}
	c.mu.Lock()
	if now.IsZero() {
		now = time.Now()
	}
	if !c.deliveryStart.IsZero() && now.Before(c.deliveryStart) {
		now = c.deliveryStart
	}
	c.delivered += bytes
	c.rollDeliveryLocked(now)
	c.mu.Unlock()
}

// capacityState returns readiness from a completed aggregate delivery window.
// It is intentionally instance-wide. If an instance has been idle for a whole
// window, readiness expires rather than remaining latched for new connections.
func (c *preferredCapacityController) capacityState(now time.Time) preferredCapacitySnapshot {
	if c == nil {
		return preferredCapacitySnapshot{DeliveryReady: true}
	}
	c.mu.Lock()
	if now.IsZero() {
		now = time.Now()
	}
	if !c.deliveryStart.IsZero() && now.Before(c.deliveryStart) {
		now = c.deliveryStart
	}
	c.rollDeliveryLocked(now)
	snapshot := c.snapshotLocked()
	c.mu.Unlock()
	return snapshot
}

func (c *preferredCapacityController) capacityReady(now time.Time) bool {
	return c.capacityState(now).DeliveryReady
}

func (c *preferredCapacityController) rollDeliveryLocked(now time.Time) {
	if c.deliveryStart.IsZero() {
		c.deliveryStart = now
		c.deliveryBase = c.delivered
		c.deliveryRate = 0
		c.deliveryReady = false
		return
	}
	elapsed := now.Sub(c.deliveryStart)
	if elapsed < c.window {
		return
	}
	if c.delivered < c.deliveryBase || elapsed <= 0 {
		c.deliveryRate = 0
		c.deliveryReady = false
	} else {
		c.deliveryRate = uint64(float64(c.delivered-c.deliveryBase) / elapsed.Seconds())
		c.deliveryReady = c.deliveryRate >= c.targetBytesPS
	}
	c.deliveryStart = now
	c.deliveryBase = c.delivered
}

func (c *preferredCapacityController) refillCreditLocked(now time.Time, length int) {
	if now.IsZero() {
		now = time.Now()
	}
	if c.creditLast.IsZero() {
		c.creditLast = now
		// The first normal DATA assignment should not be gifted to booster after
		// capacity mode starts accounting. Natural preferred assignment consumes
		// this immediately; an already-active booster candidate can be overridden.
		c.credit = float64(max(length, 1))
		return
	}
	if now.Before(c.creditLast) {
		// Cross-core timestamps can be captured out of lock order. Never move the
		// shared clock backwards, which would mint duplicate credit later.
		now = c.creditLast
	}
	elapsed := now.Sub(c.creditLast)
	c.creditLast = now
	earned := elapsed.Seconds() * float64(c.targetBytesPS)
	// Historical high-rate preferred surplus must not leak booster DATA after
	// demand falls to/below the target. Once elapsed time alone owes one current
	// DATA chunk, discard negative surplus before adding newly earned credit.
	if c.credit < 0 && earned+1 >= float64(max(length, 1)) {
		c.credit = 0
	}
	c.credit += earned
	c.clampCreditLocked()
}

func (c *preferredCapacityController) clampCreditLocked() {
	chunk := max(c.chunkSize, 1)
	debtCap := float64(chunk * preferredCapacityDebtChunks)
	if c.credit > debtCap {
		c.credit = debtCap
	}
	surplusCap := float64(chunk * preferredCapacitySurplusChunks)
	if c.credit < -surplusCap {
		c.credit = -surplusCap
	}
}

// reserveAssignment accounts a prospective first-transmission DATA assignment.
// Natural preferred assignments from both preferred-only and aggregated cores
// consume the same shared budget. A booster candidate may be overridden only
// when preferred is immediately eligible in that same core.
func (c *preferredCapacityController) reserveAssignment(now time.Time, length int, naturalPreferred, canForce bool) (forcePreferred bool, reservation *preferredCapacityReservation) {
	if c == nil || length <= 0 {
		return false, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.refillCreditLocked(now, length)

	if naturalPreferred {
		c.credit -= float64(length)
		c.clampCreditLocked()
		return false, &preferredCapacityReservation{controller: c, bytes: length}
	}
	if !canForce || c.credit+1e-9 < float64(length) {
		return false, nil
	}
	c.credit -= float64(length)
	c.clampCreditLocked()
	return true, &preferredCapacityReservation{controller: c, bytes: length}
}

func (r *preferredCapacityReservation) refund() {
	if r == nil || r.controller == nil || r.bytes <= 0 {
		return
	}
	c := r.controller
	c.mu.Lock()
	c.credit += float64(r.bytes)
	c.clampCreditLocked()
	c.mu.Unlock()
}

func (c *preferredCapacityController) snapshotLocked() preferredCapacitySnapshot {
	return preferredCapacitySnapshot{
		TargetBytesPS:  c.targetBytesPS,
		DeliveredBytes: c.delivered,
		DeliveryRate:   c.deliveryRate,
		DeliveryReady:  c.deliveryReady,
		CreditBytes:    int64(c.credit),
	}
}

func (c *preferredCapacityController) snapshot() preferredCapacitySnapshot {
	if c == nil {
		return preferredCapacitySnapshot{}
	}
	c.mu.Lock()
	snapshot := c.snapshotLocked()
	c.mu.Unlock()
	return snapshot
}

// recordPreferredCapacityRange is called under core.stateMu after a successful
// logical first-transmission mapping is created for leg0. Repair path sequence
// ranges are intentionally absent.
func (l *mpLeg) recordPreferredCapacityRange(start uint64, length int) {
	if l == nil || length <= 0 {
		return
	}
	end := start + uint64(length)
	l.preferredCapacityRanges = append(l.preferredCapacityRanges, preferredCapacityPathRange{next: start, end: end})
}

// confirmPreferredCapacityDelivery consumes peer receipt progress through the
// recorded normal-data ranges. Path sequence gaps created by repair/reinjection
// are skipped, so duplicate repair bytes cannot satisfy capacity readiness.
// Caller holds core.stateMu.
func (l *mpLeg) confirmPreferredCapacityDelivery(received uint64) uint64 {
	if l == nil || l.preferredCapacityRangeHead >= len(l.preferredCapacityRanges) {
		return 0
	}
	var confirmed uint64
	for l.preferredCapacityRangeHead < len(l.preferredCapacityRanges) {
		rangeRef := &l.preferredCapacityRanges[l.preferredCapacityRangeHead]
		if received <= rangeRef.next {
			break
		}
		upper := min(received, rangeRef.end)
		if upper > rangeRef.next {
			confirmed += upper - rangeRef.next
			rangeRef.next = upper
		}
		if rangeRef.next < rangeRef.end {
			break
		}
		l.preferredCapacityRanges[l.preferredCapacityRangeHead] = preferredCapacityPathRange{}
		l.preferredCapacityRangeHead++
	}
	if l.preferredCapacityRangeHead == len(l.preferredCapacityRanges) {
		l.preferredCapacityRanges = l.preferredCapacityRanges[:0]
		l.preferredCapacityRangeHead = 0
	} else if l.preferredCapacityRangeHead >= 256 && l.preferredCapacityRangeHead*2 >= len(l.preferredCapacityRanges) {
		n := copy(l.preferredCapacityRanges, l.preferredCapacityRanges[l.preferredCapacityRangeHead:])
		clear(l.preferredCapacityRanges[n:])
		l.preferredCapacityRanges = l.preferredCapacityRanges[:n]
		l.preferredCapacityRangeHead = 0
	}
	return confirmed
}
