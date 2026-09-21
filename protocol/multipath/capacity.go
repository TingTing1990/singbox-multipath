package multipath

import (
	"sync"
	"time"
)

const (
	preferredCapacityDebtChunks     = 2
	preferredCapacitySurplusChunks  = 4
	preferredCapacityDegradeWindows = 3
)

// preferredCapacityController belongs to exactly one configured multipath
// inbound/outbound instance and is shared by every logical TCP core created by
// that instance. The configured target is therefore aggregate path capacity,
// never a per-session target.
//
// The controller has two distinct rates:
//  1. targetBytesPS is the configured minimum admission threshold.
//  2. protectedBytesPS is the currently proven sustainable preferred rate.
//
// Once booster DATA is admitted, reservation credit accrues at the protected
// rate, not merely the configured target. This is the core additive invariant:
// a booster may add excess capacity, but scheduler competition must not displace
// preferred throughput that peer-confirmed normal delivery has already proved.
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

	// Successful normal first-transmission assignments to leg0, aggregated across
	// every logical session. This is not an authority for capacity; it is only
	// used to distinguish real preferred degradation from low offered demand or
	// scheduler under-assignment when deciding whether a previously proven rate
	// may decay.
	assigned uint64

	// Completed-window sampler. Delivery remains the authority for admission and
	// for learning a sustainable preferred rate. Assignment is diagnostic input
	// used only by the conservative degradation rule.
	deliveryStart  time.Time
	deliveryBase   uint64
	assignmentBase uint64
	deliveryRate   uint64
	assignmentRate uint64
	deliveryReady  bool

	// protectedBytesPS is a delivery-proven aggregate preferred rate. Before the
	// first booster activation it tracks peer-confirmed delivery at/above target.
	// After activation it may rise immediately when preferred proves more capacity,
	// but it may fall only after several full windows in which preferred was
	// actually assigned enough work and still delivered materially less. This
	// prevents the controller from mistaking its own under-assignment for physical
	// capacity loss.
	protectedBytesPS uint64
	protectionValid  bool
	protectionActive bool
	degradeWindows   int

	// Shared normal-DATA assignment reservation. Positive credit means preferred
	// is owed assignment; negative credit is a bounded natural surplus. Once
	// protection is active, credit accrues at the peer-delivery-proven protected
	// rate. The configured target remains an admission threshold, not a permanent
	// post-activation floor after genuine physical degradation.
	creditLast time.Time
	credit     float64
}

type preferredCapacityReservation struct {
	controller *preferredCapacityController
	bytes      int
}

type preferredCapacitySnapshot struct {
	TargetBytesPS    uint64
	DeliveredBytes   uint64
	AssignedBytes    uint64
	DeliveryRate     uint64
	AssignmentRate   uint64
	DeliveryReady    bool
	ProtectedBytesPS uint64
	ProtectionValid  bool
	ProtectionActive bool
	DegradeWindows   int
	CreditBytes      int64
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
	target := uint64(mbps) * 1_000_000 / 8
	return &preferredCapacityController{
		targetBytesPS:    target,
		protectedBytesPS: target,
		window:           window,
		chunkSize:        chunkSize,
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
// It is intentionally instance-wide. Readiness itself expires after an idle
// window, but an already active additive-protection baseline is not silently
// discarded: beta6 activation is one-way, so dropping protection during an idle
// period would let an already-active core resume by sending normal DATA to the
// booster without a fresh gate.
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
		c.assignmentBase = c.assigned
		c.deliveryRate = 0
		c.assignmentRate = 0
		c.deliveryReady = false
		return
	}
	elapsed := now.Sub(c.deliveryStart)
	if elapsed < c.window {
		return
	}
	if elapsed <= 0 || c.delivered < c.deliveryBase || c.assigned < c.assignmentBase {
		c.deliveryRate = 0
		c.assignmentRate = 0
		c.deliveryReady = false
	} else {
		seconds := elapsed.Seconds()
		c.deliveryRate = uint64(float64(c.delivered-c.deliveryBase) / seconds)
		c.assignmentRate = uint64(float64(c.assigned-c.assignmentBase) / seconds)
		c.deliveryReady = c.deliveryRate >= c.targetBytesPS
		c.updateProtectedRateLocked(now)
	}
	c.deliveryStart = now
	c.deliveryBase = c.delivered
	c.assignmentBase = c.assigned
}

func (c *preferredCapacityController) updateProtectedRateLocked(now time.Time) {
	// Delivery, never assignment, establishes or raises the protected baseline.
	// Before the first normal booster activation a rate must reach the configured
	// admission target. After protection exists, any higher peer-confirmed normal
	// delivery is new evidence of sustainable preferred capacity and may raise the
	// protected rate even while recovering from a real degradation below target.
	if (!c.protectionValid && c.deliveryRate >= c.targetBytesPS) ||
		(c.protectionValid && c.deliveryRate > c.protectedBytesPS) {
		c.setProtectedRateLocked(now, c.deliveryRate, true)
	}

	if !c.protectionActive || !c.protectionValid {
		c.degradeWindows = 0
		return
	}

	protected := c.protectedBytesPS
	if protected == 0 {
		c.degradeWindows = 0
		return
	}
	healthyDeliveryFloor := protected - protected/10
	// Assignment is exact successful normal DATA accounting, so use only a small
	// chunk-quantization tolerance here. A broad percentage tolerance would allow
	// the scheduler to under-assign preferred and then ratify its own displacement
	// as apparent physical degradation.
	assignmentTolerance := uint64(0)
	if c.window > 0 {
		assignmentTolerance = uint64(float64(max(c.chunkSize, 1)*2) / c.window.Seconds())
	}

	// Do not lower protection unless preferred was actually offered approximately
	// its full protected rate. A low delivery sample after lower assignment says
	// nothing about physical path capability and may be caused by the scheduler
	// competition this feature exists to prevent.
	if c.assignmentRate+assignmentTolerance < protected {
		c.degradeWindows = 0
		return
	}
	if c.deliveryRate >= healthyDeliveryFloor {
		c.degradeWindows = 0
		return
	}

	c.degradeWindows++
	if c.degradeWindows < preferredCapacityDegradeWindows {
		return
	}

	// Multiple complete windows of sufficient assignment but materially lower
	// peer-confirmed delivery are evidence of a real capacity decline. The
	// configured value is an admission threshold, not a permanent post-activation
	// floor, so a genuinely degraded path may fall below it rather than queueing
	// impossible preferred work forever.
	c.setProtectedRateLocked(now, c.deliveryRate, true)
	c.degradeWindows = 0
}

func (c *preferredCapacityController) setProtectedRateLocked(now time.Time, rate uint64, valid bool) {
	oldRate := c.reservationRateLocked()
	c.protectedBytesPS = rate
	c.protectionValid = valid
	newRate := c.reservationRateLocked()
	if newRate == oldRate {
		return
	}

	// Do not carry credit earned at one rate into another rate epoch. When the
	// proven rate increases, owe one chunk immediately so booster admission cannot
	// create a transition dip from the just-proven preferred throughput.
	c.creditLast = now
	if newRate > oldRate && c.protectionActive {
		c.credit = float64(max(c.chunkSize, 1))
	} else {
		c.credit = 0
	}
	c.clampCreditLocked()
}

// activateProtection is called only when a normal beta6 activation trigger and
// the aggregate peer-delivery capacity gate have both succeeded. The first such
// activation starts the additive reservation epoch and discards any pre-
// activation surplus: DATA sent while leg0 was the only ordinary path must never
// be treated as permission to displace leg0 immediately after booster joins.
func (c *preferredCapacityController) activateProtection(now time.Time, snapshot preferredCapacitySnapshot) {
	if c == nil || !snapshot.DeliveryReady {
		return
	}
	c.mu.Lock()
	if now.IsZero() {
		now = time.Now()
	}
	if !c.deliveryStart.IsZero() && now.Before(c.deliveryStart) {
		now = c.deliveryStart
	}
	if snapshot.DeliveryRate >= c.targetBytesPS && (!c.protectionValid || snapshot.DeliveryRate > c.protectedBytesPS) {
		c.setProtectedRateLocked(now, snapshot.DeliveryRate, true)
	}
	if !c.protectionValid {
		c.setProtectedRateLocked(now, c.targetBytesPS, true)
	}
	if !c.protectionActive {
		c.protectionActive = true
		c.creditLast = now
		c.credit = float64(max(c.chunkSize, 1))
		c.clampCreditLocked()
	}
	c.mu.Unlock()
}

func (c *preferredCapacityController) reservationRateLocked() uint64 {
	if c == nil {
		return 0
	}
	if c.protectionValid {
		return c.protectedBytesPS
	}
	return c.targetBytesPS
}

func (c *preferredCapacityController) refillCreditLocked(now time.Time, length int) {
	if !c.protectionActive {
		return
	}
	if now.IsZero() {
		now = time.Now()
	}
	if c.creditLast.IsZero() {
		c.creditLast = now
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
	earned := elapsed.Seconds() * float64(c.reservationRateLocked())
	// Historical high-rate preferred surplus must not leak booster DATA after
	// demand falls below the protected rate. Once elapsed time alone owes one
	// current DATA chunk, discard negative surplus before adding newly earned
	// credit.
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
// Before the first normal booster activation it records successful preferred
// assignments but deliberately does not create/consume reservation credit. After
// protection is active, natural preferred assignments from both preferred-only
// and aggregated cores consume the same shared protected-rate budget. A booster
// candidate may be overridden only when preferred is immediately eligible in
// that same core.
func (c *preferredCapacityController) reserveAssignment(now time.Time, length int, naturalPreferred, canForce bool) (forcePreferred bool, reservation *preferredCapacityReservation) {
	if c == nil || length <= 0 {
		return false, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if naturalPreferred {
		if c.protectionActive {
			c.refillCreditLocked(now, length)
			c.credit -= float64(length)
			c.clampCreditLocked()
		}
		return false, &preferredCapacityReservation{controller: c, bytes: length}
	}
	if !c.protectionActive || !canForce {
		return false, nil
	}
	c.refillCreditLocked(now, length)
	if c.credit+1e-9 < float64(length) {
		return false, nil
	}
	c.credit -= float64(length)
	c.clampCreditLocked()
	return true, &preferredCapacityReservation{controller: c, bytes: length}
}

func (r *preferredCapacityReservation) commit() {
	if r == nil || r.controller == nil || r.bytes <= 0 {
		return
	}
	c := r.controller
	c.mu.Lock()
	c.assigned += uint64(r.bytes)
	c.mu.Unlock()
}

func (r *preferredCapacityReservation) refund() {
	if r == nil || r.controller == nil || r.bytes <= 0 {
		return
	}
	c := r.controller
	c.mu.Lock()
	if c.protectionActive {
		c.credit += float64(r.bytes)
		c.clampCreditLocked()
	}
	c.mu.Unlock()
}

func (c *preferredCapacityController) snapshotLocked() preferredCapacitySnapshot {
	protected := uint64(0)
	if c.protectionValid {
		protected = c.reservationRateLocked()
	}
	return preferredCapacitySnapshot{
		TargetBytesPS:    c.targetBytesPS,
		DeliveredBytes:   c.delivered,
		AssignedBytes:    c.assigned,
		DeliveryRate:     c.deliveryRate,
		AssignmentRate:   c.assignmentRate,
		DeliveryReady:    c.deliveryReady,
		ProtectedBytesPS: protected,
		ProtectionValid:  c.protectionValid,
		ProtectionActive: c.protectionActive,
		DegradeWindows:   c.degradeWindows,
		CreditBytes:      int64(c.credit),
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
