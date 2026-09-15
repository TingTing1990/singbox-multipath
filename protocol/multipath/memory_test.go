package multipath

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestAutomaticMemoryLimit(t *testing.T) {
	if limit := automaticMemoryLimit(256 << 20); limit != 128<<20 {
		t.Fatalf("unexpected limit for small host: %d", limit)
	}
	if limit := automaticMemoryLimit(4 << 30); limit != 512<<20 {
		t.Fatalf("automatic limit was not capped: %d", limit)
	}
}

func TestMemoryStartupCreditAndSessionShares(t *testing.T) {
	cfg := testCoreConfig()
	cfg.ChunkSize, cfg.QueueFrames, cfg.QueueBytes = 65536, 256, 16<<20
	budget := newMemoryBudget(512<<20, false)
	cfg.Memory = budget
	var cores []*mpCore
	defer func() {
		for _, core := range cores {
			core.Close()
		}
		for _, core := range cores {
			<-core.released
		}
		if budget.sessions.Load() != 0 {
			t.Error("session share leaked")
		}
	}()
	for range 32 {
		core, _, err := newCoreWithError(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		cores = append(cores, core)
	}
	if budget.sessions.Load() != 32 {
		t.Fatal("incorrect live session count")
	}
	if budget.snapshot().Pressure {
		t.Fatal("idle startup grants exhausted the budget")
	}
	slot := cores[0].receiveSlotBytes()
	if int64(budget.receiveShareSlots(slot))*slot*32 > (budget.boosterLimit-budget.cacheLimit)/2 {
		t.Fatal("receive growth targets exceed shared allocation")
	}
}

func TestMemoryBudgetBoosterBackpressurePreservesPrimaryReserve(t *testing.T) {
	budget := newMemoryBudget(1024, false)
	first, err := budget.acquire(context.Background(), 800, memoryClassPrimary)
	if err != nil {
		t.Fatal(err)
	}
	second, err := budget.acquire(context.Background(), 100, memoryClassPrimary)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot := budget.snapshot(); !snapshot.Pressure || snapshot.BoosterLimitBytes != 896 || snapshot.BoosterResumeBytes != 768 {
		t.Fatalf("unexpected pressure snapshot: %+v", snapshot)
	}

	boosterResult := make(chan []byte, 1)
	boosterError := make(chan error, 1)
	go func() {
		buffer, acquireErr := budget.acquire(context.Background(), 64, memoryClassBooster)
		if acquireErr != nil {
			boosterError <- acquireErr
			return
		}
		boosterResult <- buffer
	}()
	select {
	case <-boosterResult:
		t.Fatal("booster allocation passed the high watermark")
	case err = <-boosterError:
		t.Fatal(err)
	case <-time.After(20 * time.Millisecond):
	}

	primary, err := budget.acquire(context.Background(), 100, memoryClassPrimary)
	if err != nil {
		t.Fatalf("primary could not use reserved memory: %v", err)
	}
	budget.release(primary)
	budget.release(second)
	select {
	case <-boosterResult:
		t.Fatal("booster resumed above the low watermark")
	case err = <-boosterError:
		t.Fatal(err)
	case <-time.After(20 * time.Millisecond):
	}

	budget.release(first)
	select {
	case booster := <-boosterResult:
		budget.release(booster)
	case err = <-boosterError:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("booster did not resume below the low watermark")
	}
	snapshot := budget.snapshot()
	if snapshot.Pressure || snapshot.PressureEvents != 1 || snapshot.BackpressureEvents != 1 {
		t.Fatalf("unexpected final memory snapshot: %+v", snapshot)
	}
	if snapshot.PeakUsedBytes < 1000 || snapshot.PeakCachedBytes < 64 {
		t.Fatalf("memory peaks were not retained: %+v", snapshot)
	}
}

func TestMemoryBudgetSessionAdmissionIsReleased(t *testing.T) {
	cfg := testCoreConfig()
	reservation := sessionMemoryReservation(cfg) + int64(cfg.ChunkSize) + receiveFrameOverhead
	budget := newMemoryBudget(reservation+1, false)
	cfg.Memory = budget
	first, _, err := newCoreWithError(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = newCoreWithError(context.Background(), cfg); !errors.Is(err, errMemoryLimit) {
		t.Fatalf("expected session admission failure, got %v", err)
	}
	first.Close()
	deadline := time.Now().Add(time.Second)
	for budget.snapshot().UsedBytes != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	second, _, err := newCoreWithError(context.Background(), cfg)
	if err != nil {
		t.Fatalf("released session budget was not reusable: %v", err)
	}
	second.Close()
}

func TestCoreMemoryPressureKeepsLeg0Available(t *testing.T) {
	cfg := testCoreConfig()
	budget := newMemoryBudget(4<<20, false)
	cfg.Memory = budget
	core, _, err := newCoreWithError(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer core.Close()
	core.activate(activationInfo{Reason: activationReasonBytes})

	leg0Core, leg0Peer := net.Pipe()
	defer leg0Peer.Close()
	leg1Core, leg1Peer := net.Pipe()
	defer leg1Peer.Close()
	if _, err = core.addLeg(0, leg0Core, nil); err != nil {
		t.Fatal(err)
	}
	if _, err = core.addLeg(1, leg1Core, nil); err != nil {
		t.Fatal(err)
	}

	snapshot := budget.snapshot()
	pressureBytes := int(snapshot.BoosterLimitBytes - snapshot.UsedBytes)
	pressureBuffer, err := budget.acquire(context.Background(), pressureBytes, memoryClassPrimary)
	if err != nil {
		t.Fatal(err)
	}
	defer budget.release(pressureBuffer)
	if selected := core.chooseLeg(cfg.ChunkSize); selected == nil || selected.id != 0 {
		t.Fatalf("memory pressure selected leg %v instead of leg0", selected)
	}
}
