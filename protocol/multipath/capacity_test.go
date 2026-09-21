package multipath

import (
	"context"
	"testing"
	"time"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/multipath/stream"
	M "github.com/sagernet/sing/common/metadata"
)

func capacitySchedulerCore(controller *preferredCapacityController) (*mpCore, *mpLeg, *mpLeg) {
	core := &mpCore{
		cfg: coreConfig{
			PreferredCapacity:  controller,
			AggregationEnabled: true,
			ChunkSize:          64 << 10,
			QueueFrames:        256,
			QueueBytes:         16 << 20,
			ReplayBytes:        112 << 20,
		},
		legs:   make(map[uint8]*mpLeg),
		memory: newMemoryBudget(256<<20, false),
	}
	core.active.Store(true)
	preferred := &mpLeg{id: 0, path: streamPathForCapacityTest(1, 10_000_000, 40*time.Millisecond)}
	booster := &mpLeg{id: 1, path: streamPathForCapacityTest(2, 10_000_000, 100*time.Millisecond)}
	preferred.ready.Store(true)
	booster.ready.Store(true)
	core.legs[0], core.legs[1] = preferred, booster
	return core, preferred, booster
}

// Keep stream.Path construction in one helper so tests set only observable path
// state and do not duplicate scheduler implementation logic.
func streamPathForCapacityTest(generation uint64, rate float64, srtt time.Duration) stream.Path {
	return stream.Path{Generation: generation, Rate: rate, SRTT: srtt}
}

func preferBoosterForCapacityTest(preferred, booster *mpLeg, chunk uint64) {
	preferred.path.Sent = 4 * chunk
	preferred.path.Received = 0
	booster.path.Sent = 0
	booster.path.Received = 0
}

func primePreferredProtection(t *testing.T, controller *preferredCapacityController, start time.Time, deliveredBytesPS uint64) preferredCapacitySnapshot {
	t.Helper()
	if controller.capacityReady(start) {
		t.Fatal("capacity unexpectedly ready before any delivery")
	}
	controller.observePreferredDelivery(deliveredBytesPS, start.Add(time.Second))
	snapshot := controller.capacityState(start.Add(time.Second))
	if !snapshot.DeliveryReady {
		t.Fatalf("delivery did not satisfy target: %+v", snapshot)
	}
	controller.activateProtection(start.Add(time.Second), snapshot)
	snapshot = controller.snapshot()
	if !snapshot.ProtectionActive || !snapshot.ProtectionValid {
		t.Fatalf("additive protection did not activate: %+v", snapshot)
	}
	return snapshot
}

func runBoosterBiasedAssignments(t *testing.T, controller *preferredCapacityController, cores []*mpCore, offeredBytesPS uint64, duration time.Duration, start time.Time) (preferredBytes, boosterBytes uint64) {
	t.Helper()
	if len(cores) == 0 {
		t.Fatal("no cores")
	}
	chunk := cores[0].cfg.ChunkSize
	step := time.Duration(float64(time.Second) * float64(chunk) / float64(offeredBytesPS))
	if step <= 0 {
		t.Fatal("invalid assignment step")
	}
	index := 0
	for now, end := start, start.Add(duration); now.Before(end); now = now.Add(step) {
		selection := cores[index%len(cores)].choosePathForSubmitLocked(chunk, now)
		if selection.leg == nil {
			t.Fatal("scheduler returned nil with both paths eligible")
		}
		if selection.leg.id == 0 {
			preferredBytes += uint64(chunk)
		} else {
			boosterBytes += uint64(chunk)
		}
		selection.finish(nil)
		index++
	}
	return
}

func TestPreferredCapacityAggregateDeliveryReadiness(t *testing.T) {
	controller := newPreferredCapacityController(70, time.Second, 64<<10)
	t0 := time.Unix(100, 0)
	if controller.capacityReady(t0) {
		t.Fatal("capacity became ready without peer-confirmed preferred delivery")
	}
	// Two independent logical sessions each contribute 35 Mbps. Readiness must
	// use their aggregate 70 Mbps rather than requiring either session to reach 70.
	controller.observePreferredDelivery(4_375_000, t0.Add(400*time.Millisecond))
	controller.observePreferredDelivery(4_375_000, t0.Add(800*time.Millisecond))
	if !controller.capacityReady(t0.Add(time.Second)) {
		t.Fatalf("aggregate 70 Mbps preferred delivery did not satisfy shared target: %+v", controller.snapshot())
	}
	if controller.capacityReady(t0.Add(2 * time.Second)) {
		t.Fatal("stale readiness survived a full window with no preferred delivery")
	}
}

func TestPreferredCapacityProtectsProven90AgainstBoosterDisplacement(t *testing.T) {
	const (
		preferredBytesPS = 90_000_000 / 8
		offeredBytesPS   = 150_000_000 / 8
	)
	controller := newPreferredCapacityController(70, time.Second, 64<<10)
	t0 := time.Unix(200, 0)
	snapshot := primePreferredProtection(t, controller, t0, preferredBytesPS)
	if snapshot.ProtectedBytesPS != preferredBytesPS {
		t.Fatalf("protected rate=%d, want delivery-proven %d", snapshot.ProtectedBytesPS, preferredBytesPS)
	}

	cores := make([]*mpCore, 4)
	for i := range cores {
		core, preferred, booster := capacitySchedulerCore(controller)
		// The unchanged beta6 scheduler is deliberately biased toward booster.
		// The additive controller must therefore be the thing that preserves the
		// already-proven preferred contribution.
		preferBoosterForCapacityTest(preferred, booster, uint64(core.cfg.ChunkSize))
		cores[i] = core
	}
	preferredBytes, boosterBytes := runBoosterBiasedAssignments(t, controller, cores, offeredBytesPS, 4*time.Second, t0.Add(time.Second))
	preferredRate := float64(preferredBytes) / 4
	boosterRate := float64(boosterBytes) / 4
	tolerance := float64(cores[0].cfg.ChunkSize) * 4 / 4
	if diff := preferredRate - preferredBytesPS; diff < -tolerance || diff > tolerance {
		t.Fatalf("booster displaced proven preferred rate: preferred=%.0f B/s want~%d; booster=%.0f B/s", preferredRate, preferredBytesPS, boosterRate)
	}
	if boosterBytes == 0 {
		t.Fatal("excess demand never reached booster")
	}
}

func TestPreferredCapacityTarget50DoesNotPullProven75Down(t *testing.T) {
	const (
		preferredBytesPS = 75_000_000 / 8
		offeredBytesPS   = 120_000_000 / 8
	)
	controller := newPreferredCapacityController(50, time.Second, 64<<10)
	t0 := time.Unix(300, 0)
	primePreferredProtection(t, controller, t0, preferredBytesPS)
	core, preferred, booster := capacitySchedulerCore(controller)
	preferBoosterForCapacityTest(preferred, booster, uint64(core.cfg.ChunkSize))
	preferredBytes, boosterBytes := runBoosterBiasedAssignments(t, controller, []*mpCore{core}, offeredBytesPS, 4*time.Second, t0.Add(time.Second))
	preferredRate := float64(preferredBytes) / 4
	if preferredRate < float64(preferredBytesPS)-float64(core.cfg.ChunkSize) {
		t.Fatalf("target=50 pulled proven 75 Mbps preferred down: %.0f B/s", preferredRate)
	}
	if boosterBytes == 0 {
		t.Fatal("target=50 prevented additive booster use")
	}
}

func TestPreferredCapacitySharedProtectionAcrossCores(t *testing.T) {
	const (
		protectedBytesPS = 80_000_000 / 8
		offeredBytesPS   = 160_000_000 / 8
	)
	controller := newPreferredCapacityController(70, time.Second, 64<<10)
	t0 := time.Unix(400, 0)
	primePreferredProtection(t, controller, t0, protectedBytesPS)
	cores := make([]*mpCore, 8)
	for i := range cores {
		core, preferred, booster := capacitySchedulerCore(controller)
		preferBoosterForCapacityTest(preferred, booster, uint64(core.cfg.ChunkSize))
		cores[i] = core
	}
	preferredBytes, boosterBytes := runBoosterBiasedAssignments(t, controller, cores, offeredBytesPS, 4*time.Second, t0.Add(time.Second))
	preferredRate := float64(preferredBytes) / 4
	tolerance := float64(cores[0].cfg.ChunkSize)
	if diff := preferredRate - protectedBytesPS; diff < -tolerance || diff > tolerance {
		t.Fatalf("shared aggregate protected rate %.0f B/s, want ~%d B/s; booster=%d", preferredRate, protectedBytesPS, boosterBytes)
	}
	if boosterBytes == 0 {
		t.Fatal("shared protection multiplied target by flow count and starved booster")
	}
}

func TestPreferredCapacityPreActivationSurplusCannotLicenseImmediateDisplacement(t *testing.T) {
	controller := newPreferredCapacityController(70, time.Second, 64<<10)
	t0 := time.Unix(500, 0)
	// Record many preferred-only assignments before first booster activation.
	for i := 0; i < 64; i++ {
		_, r := controller.reserveAssignment(t0.Add(time.Duration(i)*time.Millisecond), 64<<10, true, false)
		if r == nil {
			t.Fatal("preferred assignment reservation missing")
		}
		r.commit()
	}
	controller.capacityState(t0)
	controller.observePreferredDelivery(90_000_000/8, t0.Add(time.Second))
	snapshot := controller.capacityState(t0.Add(time.Second))
	controller.activateProtection(t0.Add(time.Second), snapshot)
	core, preferred, booster := capacitySchedulerCore(controller)
	preferBoosterForCapacityTest(preferred, booster, uint64(core.cfg.ChunkSize))
	selection := core.choosePathForSubmitLocked(core.cfg.ChunkSize, t0.Add(time.Second))
	if selection.leg != preferred {
		t.Fatalf("pre-activation preferred traffic licensed immediate booster displacement: got leg %v", legIDForCapacityTest(selection.leg))
	}
	selection.finish(nil)
}

func TestPreferredCapacityBusyPreferredDoesNotBlockBooster(t *testing.T) {
	controller := newPreferredCapacityController(70, time.Second, 64<<10)
	t0 := time.Unix(600, 0)
	primePreferredProtection(t, controller, t0, 90_000_000/8)
	core, preferred, booster := capacitySchedulerCore(controller)
	preferBoosterForCapacityTest(preferred, booster, uint64(core.cfg.ChunkSize))
	preferred.busy = true
	selection := core.choosePathForSubmitLocked(core.cfg.ChunkSize, t0.Add(time.Second))
	if selection.leg != booster {
		t.Fatalf("busy preferred blocked/replaced original booster candidate: got leg %v", legIDForCapacityTest(selection.leg))
	}
}

func TestPreferredCapacityFullPreferredDoesNotBlockBooster(t *testing.T) {
	controller := newPreferredCapacityController(70, time.Second, 64<<10)
	t0 := time.Unix(700, 0)
	primePreferredProtection(t, controller, t0, 90_000_000/8)
	core, preferred, booster := capacitySchedulerCore(controller)
	preferBoosterForCapacityTest(preferred, booster, uint64(core.cfg.ChunkSize))
	preferred.path.Sent = uint64(core.cfg.ReplayBytes)
	preferred.path.Received = 0
	selection := core.choosePathForSubmitLocked(core.cfg.ChunkSize, t0.Add(time.Second))
	if selection.leg != booster {
		t.Fatalf("pipeline-full preferred blocked/replaced original booster candidate: got leg %v", legIDForCapacityTest(selection.leg))
	}
}

func TestPreferredCapacityDisabledReturnsOriginalDecision(t *testing.T) {
	core, preferred, booster := capacitySchedulerCore(nil)
	preferBoosterForCapacityTest(preferred, booster, uint64(core.cfg.ChunkSize))
	want := core.choosePathLocked(core.cfg.ChunkSize)
	got := core.choosePathForSubmitLocked(core.cfg.ChunkSize, time.Unix(800, 0)).leg
	if got != want || got != booster {
		t.Fatalf("disabled capacity changed beta6 decision: got=%v want=%v", legIDForCapacityTest(got), legIDForCapacityTest(want))
	}
}

func TestPreferredCapacityNaturalPreferredIsNotCapped(t *testing.T) {
	controller := newPreferredCapacityController(50, time.Second, 64<<10)
	t0 := time.Unix(900, 0)
	primePreferredProtection(t, controller, t0, 75_000_000/8)
	core, preferred, booster := capacitySchedulerCore(controller)
	// Make booster slower by outstanding work so original beta6 naturally picks preferred.
	booster.path.Sent = 16 * uint64(core.cfg.ChunkSize)
	booster.path.Received = 0
	preferred.path.Sent = 0
	preferred.path.Received = 0
	for i := 0; i < 32; i++ {
		selection := core.choosePathForSubmitLocked(core.cfg.ChunkSize, t0.Add(time.Second+time.Duration(i)*time.Millisecond))
		if selection.leg != preferred {
			t.Fatalf("target/protection acted as a cap on natural preferred at iteration %d: leg=%v", i, legIDForCapacityTest(selection.leg))
		}
		selection.finish(nil)
	}
}

func TestPreferredCapacityLowDemandDoesNotDecayProvenRate(t *testing.T) {
	controller := newPreferredCapacityController(70, time.Second, 64<<10)
	t0 := time.Unix(1000, 0)
	primePreferredProtection(t, controller, t0, 90_000_000/8)
	for window := 1; window <= preferredCapacityDegradeWindows+1; window++ {
		start := t0.Add(time.Duration(window) * time.Second)
		// Only 60 Mbps is assigned and delivered. That is low demand, not evidence
		// that the path lost its previously proven 90 Mbps capability.
		_, r := controller.reserveAssignment(start.Add(100*time.Millisecond), 60_000_000/8, true, false)
		r.commit()
		controller.observePreferredDelivery(60_000_000/8, start.Add(time.Second))
		controller.capacityState(start.Add(time.Second))
	}
	if got := controller.snapshot().ProtectedBytesPS; got != 90_000_000/8 {
		t.Fatalf("low demand decayed protected rate: got=%d want=%d", got, 90_000_000/8)
	}
}

func TestPreferredCapacitySchedulerUnderAssignmentCannotDecayProvenRate(t *testing.T) {
	controller := newPreferredCapacityController(70, time.Second, 64<<10)
	t0 := time.Unix(1100, 0)
	primePreferredProtection(t, controller, t0, 90_000_000/8)
	for window := 1; window <= preferredCapacityDegradeWindows+1; window++ {
		start := t0.Add(time.Duration(window) * time.Second)
		// A hypothetical buggy scheduler only gives preferred 70 Mbps and it
		// delivers all 70. This must NOT be interpreted as physical degradation;
		// otherwise the controller would ratify the scheduler's own displacement.
		_, r := controller.reserveAssignment(start.Add(100*time.Millisecond), 70_000_000/8, true, false)
		r.commit()
		controller.observePreferredDelivery(70_000_000/8, start.Add(time.Second))
		controller.capacityState(start.Add(time.Second))
	}
	if got := controller.snapshot().ProtectedBytesPS; got != 90_000_000/8 {
		t.Fatalf("scheduler under-assignment ratified its own displacement: got=%d want=%d", got, 90_000_000/8)
	}
}

func TestPreferredCapacityRealDeliveryDegradationDecaysConservatively(t *testing.T) {
	controller := newPreferredCapacityController(70, time.Second, 64<<10)
	t0 := time.Unix(1200, 0)
	primePreferredProtection(t, controller, t0, 90_000_000/8)
	for window := 1; window <= preferredCapacityDegradeWindows; window++ {
		start := t0.Add(time.Duration(window) * time.Second)
		_, r := controller.reserveAssignment(start.Add(100*time.Millisecond), 90_000_000/8, true, false)
		r.commit()
		controller.observePreferredDelivery(80_000_000/8, start.Add(time.Second))
		controller.capacityState(start.Add(time.Second))
		if window < preferredCapacityDegradeWindows && controller.snapshot().ProtectedBytesPS != 90_000_000/8 {
			t.Fatalf("protected rate decayed before %d consecutive evidence windows", preferredCapacityDegradeWindows)
		}
	}
	if got := controller.snapshot().ProtectedBytesPS; got != 80_000_000/8 {
		t.Fatalf("real sustained degradation did not decay protected rate: got=%d want=%d", got, 80_000_000/8)
	}
}

func TestPreferredCapacityRealDegradationMayFallBelowAdmissionTarget(t *testing.T) {
	controller := newPreferredCapacityController(70, time.Second, 64<<10)
	t0 := time.Unix(1250, 0)
	primePreferredProtection(t, controller, t0, 90_000_000/8)
	for window := 1; window <= preferredCapacityDegradeWindows; window++ {
		start := t0.Add(time.Duration(window) * time.Second)
		_, r := controller.reserveAssignment(start.Add(100*time.Millisecond), 90_000_000/8, true, false)
		r.commit()
		controller.observePreferredDelivery(60_000_000/8, start.Add(time.Second))
		controller.capacityState(start.Add(time.Second))
	}
	if got := controller.snapshot().ProtectedBytesPS; got != 60_000_000/8 {
		t.Fatalf("configured admission target became an impossible post-activation floor: got=%d want=%d", got, 60_000_000/8)
	}
}

func TestPreferredCapacityHigherPeerDeliveryRaisesProtection(t *testing.T) {
	controller := newPreferredCapacityController(70, time.Second, 64<<10)
	t0 := time.Unix(1300, 0)
	primePreferredProtection(t, controller, t0, 80_000_000/8)
	start := t0.Add(time.Second)
	_, r := controller.reserveAssignment(start.Add(100*time.Millisecond), 95_000_000/8, true, false)
	r.commit()
	controller.observePreferredDelivery(95_000_000/8, start.Add(time.Second))
	controller.capacityState(start.Add(time.Second))
	if got := controller.snapshot().ProtectedBytesPS; got != 95_000_000/8 {
		t.Fatalf("higher peer delivery did not raise protected rate: got=%d want=%d", got, 95_000_000/8)
	}
}

func TestPreferredCapacityRecoveryFailoverBypassesGateAndProtection(t *testing.T) {
	controller := newPreferredCapacityController(70, time.Second, 64<<10)
	policy := &recoveryPolicy{}
	policy.update(1, 0b10, 1) // leg0 unavailable, leg1 allowed.
	core, _, booster := capacitySchedulerCore(controller)
	core.cfg.Recovery = policy
	if !core.preferredCapacityActivationReady(time.Unix(1400, 0)) {
		t.Fatal("capacity gate blocked true recovery failover")
	}
	selection := core.choosePathForSubmitLocked(core.cfg.ChunkSize, time.Unix(1400, 0))
	if selection.leg != booster {
		t.Fatalf("capacity protection overrode recovery leg1: got leg %v", legIDForCapacityTest(selection.leg))
	}
}

func TestPreferredCapacityControllerOwnershipIsInstanceWide(t *testing.T) {
	options := option.MultipathOutboundOptions{
		PreferredCapacityMbps: 70,
		Outbounds:             []string{"preferred", "booster"},
		Server:                "127.0.0.1",
		ServerPort:            39000,
	}
	created, err := NewOutbound(context.Background(), nil, log.NewNOPFactory().Logger(), "mp", options)
	if err != nil {
		t.Fatal(err)
	}
	outbound := created.(*Outbound)
	if outbound.cfg.PreferredCapacity == nil {
		t.Fatal("outbound instance did not own capacity controller")
	}
	a := outbound.connectionCoreConfig(context.Background(), M.ParseSocksaddr("1.1.1.1:443"), [16]byte{1})
	b := outbound.connectionCoreConfig(context.Background(), M.ParseSocksaddr("1.0.0.1:443"), [16]byte{2})
	if a.PreferredCapacity != outbound.cfg.PreferredCapacity || b.PreferredCapacity != outbound.cfg.PreferredCapacity || a.PreferredCapacity != b.PreferredCapacity {
		t.Fatal("logical sessions did not inherit one shared outbound capacity controller")
	}
}

func legIDForCapacityTest(leg *mpLeg) any {
	if leg == nil {
		return nil
	}
	return leg.id
}

func TestPreferredCapacityRepairRangesExcludedFromDelivery(t *testing.T) {
	leg := &mpLeg{}
	const chunk = 64 << 10
	leg.recordPreferredCapacityRange(0, chunk)
	// [chunk, 2*chunk) represents a repair/reinjection range and is deliberately
	// not recorded as normal first-transmission DATA.
	leg.recordPreferredCapacityRange(2*chunk, chunk)
	if got := leg.confirmPreferredCapacityDelivery(3 * chunk); got != 2*chunk {
		t.Fatalf("peer receipt counted repair bytes: got=%d want=%d", got, 2*chunk)
	}
	if len(leg.preferredCapacityRanges) != 0 || leg.preferredCapacityRangeHead != 0 {
		t.Fatalf("confirmed range bookkeeping not compacted: len=%d head=%d", len(leg.preferredCapacityRanges), leg.preferredCapacityRangeHead)
	}
}

func TestPreferredCapacityCrossCoreTimestampDoesNotMintCredit(t *testing.T) {
	const chunk = 64 << 10
	controller := newPreferredCapacityController(70, time.Second, chunk)
	t0 := time.Unix(1500, 0)
	primePreferredProtection(t, controller, t0, 90_000_000/8)
	// Consume the initial post-activation preferred debt.
	_, r := controller.reserveAssignment(t0.Add(time.Second), chunk, true, false)
	r.commit()

	controller.mu.Lock()
	controller.refillCreditLocked(t0.Add(time.Second+20*time.Millisecond), chunk)
	before := int64(controller.credit)
	// Another core captured an older timestamp but enters the controller later.
	controller.refillCreditLocked(t0.Add(time.Second+10*time.Millisecond), chunk)
	afterOld := int64(controller.credit)
	controller.refillCreditLocked(t0.Add(time.Second+21*time.Millisecond), chunk)
	afterNext := int64(controller.credit)
	protected := controller.reservationRateLocked()
	controller.mu.Unlock()
	if afterOld != before {
		t.Fatalf("out-of-order timestamp changed shared credit: before=%d after=%d", before, afterOld)
	}
	maxOneMillisecondEarned := int64(protected / 1000)
	if delta := afterNext - afterOld; delta < 0 || delta > maxOneMillisecondEarned+2 {
		t.Fatalf("out-of-order timestamp minted duplicate credit: delta=%d max~%d", delta, maxOneMillisecondEarned)
	}
}

func TestPreferredCapacityHistoricalSurplusDoesNotLeakBelowProtectedRate(t *testing.T) {
	const chunk = 64 << 10
	controller := newPreferredCapacityController(70, time.Second, chunk)
	t0 := time.Unix(1600, 0)
	primePreferredProtection(t, controller, t0, 90_000_000/8)
	controller.mu.Lock()
	controller.creditLast = t0.Add(time.Second)
	controller.credit = -float64(4 * chunk)
	controller.mu.Unlock()
	// 40 Mbps offered load spaces 64 KiB chunks longer than the time needed for
	// a 90 Mbps protected rate to owe one chunk. Historical surplus must therefore
	// be cleared and the next booster candidate forced preferred.
	step := time.Duration(float64(time.Second) * float64(chunk) / 5_000_000)
	force, reservation := controller.reserveAssignment(t0.Add(time.Second).Add(step), chunk, false, true)
	if !force || reservation == nil {
		t.Fatalf("historical preferred surplus leaked booster DATA below protected rate: force=%v reservation=%v snapshot=%+v", force, reservation, controller.snapshot())
	}
}

func TestPreferredCapacityDeliveryTimeDoesNotMoveBackward(t *testing.T) {
	controller := newPreferredCapacityController(70, time.Second, 64<<10)
	t0 := time.Unix(1700, 0)
	if controller.capacityReady(t0) {
		t.Fatal("unexpected initial readiness")
	}
	controller.observePreferredDelivery(4_375_000, t0.Add(700*time.Millisecond))
	// An older cross-core timestamp is processed later. It must contribute bytes
	// without resetting the aggregate window backwards.
	controller.observePreferredDelivery(4_375_000, t0.Add(600*time.Millisecond))
	if !controller.capacityReady(t0.Add(time.Second)) {
		t.Fatalf("out-of-order delivery timestamp corrupted aggregate window: %+v", controller.snapshot())
	}
}

func TestPreferredCapacityModerateSchedulerUnderAssignmentCannotDecayProvenRate(t *testing.T) {
	controller := newPreferredCapacityController(70, time.Second, 64<<10)
	t0 := time.Unix(1800, 0)
	primePreferredProtection(t, controller, t0, 90_000_000/8)
	for window := 1; window <= preferredCapacityDegradeWindows+1; window++ {
		start := t0.Add(time.Duration(window) * time.Second)
		// 85 Mbps is materially below the proven 90 Mbps protected rate, but still
		// close enough that a broad percentage threshold could incorrectly treat it
		// as sufficient offered work and ratify scheduler displacement.
		_, r := controller.reserveAssignment(start.Add(100*time.Millisecond), 85_000_000/8, true, false)
		r.commit()
		controller.observePreferredDelivery(80_000_000/8, start.Add(time.Second))
		controller.capacityState(start.Add(time.Second))
	}
	if got := controller.snapshot().ProtectedBytesPS; got != 90_000_000/8 {
		t.Fatalf("moderate scheduler under-assignment ratified physical degradation: got=%d want=%d", got, 90_000_000/8)
	}
}

func TestPreferredCapacityBelowTargetDegradationAndRecoveryRemainDeliveryDriven(t *testing.T) {
	controller := newPreferredCapacityController(70, time.Second, 64<<10)
	t0 := time.Unix(1900, 0)
	primePreferredProtection(t, controller, t0, 90_000_000/8)

	// First prove a real decline below the admission target: assignment remains at
	// the protected 90 Mbps, while peer-confirmed normal delivery sustains only 60.
	for window := 1; window <= preferredCapacityDegradeWindows; window++ {
		start := t0.Add(time.Duration(window) * time.Second)
		_, r := controller.reserveAssignment(start.Add(100*time.Millisecond), 90_000_000/8, true, false)
		r.commit()
		controller.observePreferredDelivery(60_000_000/8, start.Add(time.Second))
		controller.capacityState(start.Add(time.Second))
	}
	if got := controller.snapshot().ProtectedBytesPS; got != 60_000_000/8 {
		t.Fatalf("first below-target physical decline not learned: got=%d want=%d", got, 60_000_000/8)
	}

	// A further real decline must be judged against the current protected 60 Mbps,
	// not the original admission target 70 Mbps.
	for window := 1; window <= preferredCapacityDegradeWindows; window++ {
		start := t0.Add(time.Duration(preferredCapacityDegradeWindows+window) * time.Second)
		_, r := controller.reserveAssignment(start.Add(100*time.Millisecond), 60_000_000/8, true, false)
		r.commit()
		controller.observePreferredDelivery(50_000_000/8, start.Add(time.Second))
		controller.capacityState(start.Add(time.Second))
	}
	if got := controller.snapshot().ProtectedBytesPS; got != 50_000_000/8 {
		t.Fatalf("second below-target physical decline not learned: got=%d want=%d", got, 50_000_000/8)
	}

	// Existing active traffic may recover while still below the configured 70 Mbps
	// admission threshold. Higher peer-confirmed normal delivery must raise the
	// protection immediately; target is not a post-activation relearning floor.
	start := t0.Add(time.Duration(preferredCapacityDegradeWindows*2+1) * time.Second)
	_, r := controller.reserveAssignment(start.Add(100*time.Millisecond), 65_000_000/8, true, false)
	r.commit()
	controller.observePreferredDelivery(65_000_000/8, start.Add(time.Second))
	controller.capacityState(start.Add(time.Second))
	if got := controller.snapshot().ProtectedBytesPS; got != 65_000_000/8 {
		t.Fatalf("below-target recovery did not raise delivery-proven protection: got=%d want=%d", got, 65_000_000/8)
	}
}

func TestPreferredCapacityAdjacentDeliveryRangesCoalesce(t *testing.T) {
	leg := &mpLeg{}
	leg.recordPreferredCapacityRange(0, 64<<10)
	leg.recordPreferredCapacityRange(64<<10, 64<<10)
	leg.recordPreferredCapacityRange(128<<10, 64<<10)
	if len(leg.preferredCapacityRanges) != 1 {
		t.Fatalf("adjacent preferred ranges not coalesced: len=%d", len(leg.preferredCapacityRanges))
	}
	if got := leg.confirmPreferredCapacityDelivery(96 << 10); got != 96<<10 {
		t.Fatalf("partial coalesced delivery=%d, want %d", got, 96<<10)
	}
	if got := leg.confirmPreferredCapacityDelivery(192 << 10); got != 96<<10 {
		t.Fatalf("remaining coalesced delivery=%d, want %d", got, 96<<10)
	}
	if len(leg.preferredCapacityRanges) != 0 || leg.preferredCapacityRangeHead != 0 {
		t.Fatalf("coalesced range bookkeeping not drained: len=%d head=%d", len(leg.preferredCapacityRanges), leg.preferredCapacityRangeHead)
	}
}
