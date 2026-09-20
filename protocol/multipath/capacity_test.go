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

func TestPreferredCapacitySharedAssignmentAcrossCores(t *testing.T) {
	const (
		targetBytesPS  = 8_000_000
		offeredBytesPS = 16_000_000
	)
	controller := &preferredCapacityController{targetBytesPS: targetBytesPS, window: time.Second, chunkSize: 64 << 10}
	cores := make([]*mpCore, 4)
	for i := range cores {
		core, preferred, booster := capacitySchedulerCore(controller)
		preferBoosterForCapacityTest(preferred, booster, uint64(core.cfg.ChunkSize))
		cores[i] = core
	}
	chunk := cores[0].cfg.ChunkSize
	step := time.Duration(float64(time.Second) * float64(chunk) / offeredBytesPS)
	t0 := time.Unix(200, 0)
	end := t0.Add(4 * time.Second)
	var preferredBytes, boosterBytes uint64
	index := 0
	for now := t0; now.Before(end); now = now.Add(step) {
		selection := cores[index%len(cores)].choosePathForSubmitLocked(chunk, now)
		if selection.leg == nil {
			t.Fatal("shared scheduler returned nil with both paths eligible")
		}
		if selection.leg.id == 0 {
			preferredBytes += uint64(chunk)
		} else {
			boosterBytes += uint64(chunk)
		}
		selection.finish(nil)
		index++
	}
	elapsed := end.Sub(t0).Seconds()
	preferredRate := float64(preferredBytes) / elapsed
	boosterRate := float64(boosterBytes) / elapsed
	chunkTolerance := float64(chunk) * 3 / elapsed
	if diff := preferredRate - targetBytesPS; diff < -chunkTolerance || diff > chunkTolerance {
		t.Fatalf("aggregate preferred rate %.0f B/s, want ~%d B/s; booster %.0f B/s", preferredRate, targetBytesPS, boosterRate)
	}
	if boosterBytes == 0 {
		t.Fatal("excess demand never reached booster")
	}
}

func TestPreferredCapacityBusyPreferredDoesNotBlockBooster(t *testing.T) {
	controller := newPreferredCapacityController(70, time.Second, 64<<10)
	core, preferred, booster := capacitySchedulerCore(controller)
	preferBoosterForCapacityTest(preferred, booster, uint64(core.cfg.ChunkSize))
	preferred.busy = true
	selection := core.choosePathForSubmitLocked(core.cfg.ChunkSize, time.Unix(300, 0))
	if selection.leg != booster {
		t.Fatalf("busy preferred blocked/replaced original booster candidate: got leg %v", legIDForCapacityTest(selection.leg))
	}
}

func TestPreferredCapacityFullPreferredDoesNotBlockBooster(t *testing.T) {
	controller := newPreferredCapacityController(70, time.Second, 64<<10)
	core, preferred, booster := capacitySchedulerCore(controller)
	preferBoosterForCapacityTest(preferred, booster, uint64(core.cfg.ChunkSize))
	preferred.path.Sent = uint64(core.cfg.ReplayBytes)
	preferred.path.Received = 0
	selection := core.choosePathForSubmitLocked(core.cfg.ChunkSize, time.Unix(400, 0))
	if selection.leg != booster {
		t.Fatalf("pipeline-full preferred blocked/replaced original booster candidate: got leg %v", legIDForCapacityTest(selection.leg))
	}
}

func TestPreferredCapacityDisabledReturnsOriginalDecision(t *testing.T) {
	core, preferred, booster := capacitySchedulerCore(nil)
	preferBoosterForCapacityTest(preferred, booster, uint64(core.cfg.ChunkSize))
	want := core.choosePathLocked(core.cfg.ChunkSize)
	got := core.choosePathForSubmitLocked(core.cfg.ChunkSize, time.Unix(500, 0)).leg
	if got != want || got != booster {
		t.Fatalf("disabled capacity changed beta6 decision: got=%v want=%v", legIDForCapacityTest(got), legIDForCapacityTest(want))
	}
}

func TestPreferredCapacityNaturalPreferredIsNotCapped(t *testing.T) {
	controller := newPreferredCapacityController(50, time.Second, 64<<10)
	core, preferred, booster := capacitySchedulerCore(controller)
	// Make booster slower by outstanding work so original beta6 naturally picks preferred.
	booster.path.Sent = 16 * uint64(core.cfg.ChunkSize)
	booster.path.Received = 0
	preferred.path.Sent = 0
	preferred.path.Received = 0
	for i := 0; i < 32; i++ {
		selection := core.choosePathForSubmitLocked(core.cfg.ChunkSize, time.Unix(600, int64(i)*1_000_000))
		if selection.leg != preferred {
			t.Fatalf("target acted as a cap on natural preferred at iteration %d: leg=%v", i, legIDForCapacityTest(selection.leg))
		}
		selection.finish(nil)
	}
}

func TestPreferredCapacityRecoveryFailoverBypassesGateAndFloor(t *testing.T) {
	controller := newPreferredCapacityController(70, time.Second, 64<<10)
	policy := &recoveryPolicy{}
	policy.update(1, 0b10, 1) // leg0 unavailable, leg1 allowed.
	core, _, booster := capacitySchedulerCore(controller)
	core.cfg.Recovery = policy
	if !core.preferredCapacityActivationReady(time.Unix(700, 0)) {
		t.Fatal("capacity gate blocked true recovery failover")
	}
	selection := core.choosePathForSubmitLocked(core.cfg.ChunkSize, time.Unix(700, 0))
	if selection.leg != booster {
		t.Fatalf("capacity floor overrode recovery leg1: got leg %v", legIDForCapacityTest(selection.leg))
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
	t0 := time.Unix(800, 0)
	// Initialize at t0 and consume the initial preferred assignment.
	_, r := controller.reserveAssignment(t0, chunk, true, false)
	if r == nil {
		t.Fatal("initial preferred reservation missing")
	}
	// A later core advances the shared clock.
	_, _ = controller.reserveAssignment(t0.Add(20*time.Millisecond), chunk, true, false)
	before := controller.snapshot().CreditBytes
	// Another core captured an older timestamp but enters the controller later.
	_, _ = controller.reserveAssignment(t0.Add(10*time.Millisecond), chunk, false, false)
	afterOld := controller.snapshot().CreditBytes
	// No backwards movement may create extra elapsed credit.
	_, _ = controller.reserveAssignment(t0.Add(21*time.Millisecond), chunk, false, false)
	afterNext := controller.snapshot().CreditBytes
	if afterOld != before {
		t.Fatalf("out-of-order timestamp changed shared credit: before=%d after=%d", before, afterOld)
	}
	maxOneMillisecondEarned := int64(controller.targetBytesPS / 1000)
	if delta := afterNext - afterOld; delta < 0 || delta > maxOneMillisecondEarned+2 {
		t.Fatalf("out-of-order timestamp minted duplicate credit: delta=%d max~%d", delta, maxOneMillisecondEarned)
	}
}

func TestPreferredCapacityHistoricalSurplusDoesNotLeakBelowTarget(t *testing.T) {
	const chunk = 64 << 10
	controller := newPreferredCapacityController(70, time.Second, chunk)
	t0 := time.Unix(900, 0)
	controller.mu.Lock()
	controller.creditLast = t0
	controller.credit = -float64(4 * chunk)
	controller.mu.Unlock()
	// 40 Mbps offered load spaces 64 KiB chunks by ~13 ms, longer than the
	// ~7.5 ms needed for a 70 Mbps target to owe one chunk. Historical surplus
	// must therefore be cleared and the next booster candidate forced preferred.
	step := time.Duration(float64(time.Second) * float64(chunk) / 5_000_000)
	force, reservation := controller.reserveAssignment(t0.Add(step), chunk, false, true)
	if !force || reservation == nil {
		t.Fatalf("historical preferred surplus leaked booster DATA below target: force=%v reservation=%v snapshot=%+v", force, reservation, controller.snapshot())
	}
}

func TestPreferredCapacityDeliveryTimeDoesNotMoveBackward(t *testing.T) {
	controller := newPreferredCapacityController(70, time.Second, 64<<10)
	t0 := time.Unix(1000, 0)
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
