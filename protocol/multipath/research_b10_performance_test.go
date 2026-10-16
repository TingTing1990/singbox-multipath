package multipath

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"testing/synctest"
	"time"
)

// Reuse the existing Golden pacedLink / newCore / connectTestLeg fixture,
// without changing the frozen TestPerformanceHealthyLinks or its >=98% Gate.
// This pair explicitly runs BOTH the server B10 + shared Capacity V2 code
// path and the corresponding Golden selector scope under the same test fixture.
// This is a protocol-level pre-FIELD gate, not SS2022/kernel FIELD evidence.
func TestResearchB10ServerDownloadCapacityHealthyAB(t *testing.T) {
	cases := []struct{ flows, preferred, booster, rtt0, rtt1 int }{
		{1, 160, 600, 65, 110}, {8, 160, 600, 65, 110},
		{1, 700, 500, 65, 110}, {1, 750, 550, 65, 110},
		{1, 800, 600, 65, 110}, {1, 160, 600, 65, 400},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%d_%d+%d_%d+%d", tc.flows, tc.preferred, tc.booster, tc.rtt0, tc.rtt1), func(t *testing.T) {
			baseline := researchB10LiveThroughput(t, tc.flows, tc.preferred, tc.booster, tc.rtt0, tc.rtt1, false, true, 128<<20)
			candidate := researchB10LiveThroughput(t, tc.flows, tc.preferred, tc.booster, tc.rtt0, tc.rtt1, true, true, 128<<20)
			t.Logf("paired scope-off %.2f Mbps; server B10 + Capacity V2 %.2f Mbps", baseline, candidate)
			if baseline <= 0 || candidate < baseline*0.98 {
				t.Fatalf("server B10 + Capacity V2 fell below existing 98%% healthy gate: %.2f vs %.2f Mbps", candidate, baseline)
			}
		})
	}
}

func researchB10LiveThroughput(t *testing.T, flows, rate0, rate1, rtt0, rtt1 int, serverScope, twoLeg bool, total int) (mbps float64) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) {
		cfg := testCoreConfig()
		cfg.ChunkSize, cfg.QueueFrames, cfg.QueueBytes = 65536, 256, 16<<20
		cfg.MaxReorderFrames, cfg.MaxReorderBytes = 2048, 64<<20
		cfg.ReplayBytes, cfg.ReplayTimeout = 64<<20, time.Second
		// A single instance-wide controller is genuinely shared across
		// ALL sender connections, rather than a stub per session.
		capacity := newPreferredCapacityController(60, time.Second, cfg.ChunkSize)
		if twoLeg {
			primePreferredProtection(t, capacity, time.Now().Add(-time.Second), uint64(rate0*1_000_000/8))
		}
		sendMemory := newMemoryBudget(512<<20, false)
		receiveMemory := newMemoryBudget(512<<20, false)
		senders, receivers := make([]*mpCore, 0, flows), make([]*mpCore, 0, flows)
		sources, targets := make([]net.Conn, 0, flows), make([]net.Conn, 0, flows)
		defer func() {
			for _, c := range senders {
				c.Close()
			}
			for _, c := range receivers {
				c.Close()
			}
			for _, c := range senders {
				c.workerGroup.Wait()
			}
			for _, c := range receivers {
				c.workerGroup.Wait()
			}
		}()
		for i := 0; i < flows; i++ {
			cfg.Memory, cfg.PreferredCapacity, cfg.ServerDownload = sendMemory, capacity, serverScope
			left, app := newCore(context.Background(), cfg)
			cfg.Memory, cfg.PreferredCapacity, cfg.ServerDownload = receiveMemory, nil, false
			right, peer := newCore(context.Background(), cfg)
			senders, receivers = append(senders, left), append(receivers, right)
			sources, targets = append(sources, app), append(targets, peer)
			if twoLeg {
				left.activate(activationInfo{Reason: activationReasonBytes})
				right.activate(activationInfo{Reason: activationReasonBytes})
			}
			for id := uint8(0); id < 2; id++ {
				if !twoLeg && id == 1 {
					break
				}
				a, b := net.Pipe()
				rate, delay := int64(rate0*1_000_000/8/flows), time.Duration(rtt0)*time.Millisecond/2
				if id == 1 {
					rate, delay = int64(rate1*1_000_000/8/flows), time.Duration(rtt1)*time.Millisecond/2
				}
				connectTestLeg(t, left, right, id, newPacedLink(a, rate, delay), newPacedLink(b, rate, delay))
			}
			left.scheduleProbes(time.Now())
			right.scheduleProbes(time.Now())
		}
		time.Sleep(250 * time.Millisecond)
		for _, c := range senders {
			if c.active.Load() != twoLeg || c.cfg.PreferredCapacity != capacity || c.cfg.ServerDownload != serverScope {
				t.Fatal("test failed to enter intended active/server/Capacity code scope")
			}
		}
		perConnection := total / flows
		results := make(chan error, 2*flows)
		started := time.Now()
		for i := range sources {
			i := i
			go func() {
				payload := make([]byte, 65536)
				for sent := 0; sent < perConnection; sent += len(payload) {
					if _, err := sources[i].Write(payload); err != nil {
						results <- err
						return
					}
				}
				results <- sources[i].(closeWriter).CloseWrite()
			}()
			go func() {
				_ = targets[i].SetReadDeadline(time.Now().Add(90 * time.Second))
				n, err := io.Copy(io.Discard, targets[i])
				if err == nil && n != int64(perConnection) {
					err = fmt.Errorf("short logical stream %d", n)
				}
				results <- err
			}()
		}
		for i := 0; i < 2*flows; i++ {
			if err := <-results; err != nil {
				t.Fatal(err)
			}
		}
		elapsed := time.Since(started)
		if elapsed <= 0 {
			t.Fatal("nonpositive protocol runtime")
		}
		mbps = float64(total) * 8 / elapsed.Seconds() / 1e6
		var boosterBytes uint64
		for _, c := range senders {
			boosterBytes += c.legCounters[1].txBytes.Load()
		}
		if twoLeg && boosterBytes == 0 {
			t.Fatal("active healthy pair did not actually use the booster")
		}
		if twoLeg && (!capacity.snapshot().ProtectionActive || capacity.snapshot().AssignedBytes == 0) {
			t.Fatal("real shared Capacity V2 controller was not exercised")
		}
		if sendMemory.snapshot().UsedBytes > sendMemory.limit || receiveMemory.snapshot().UsedBytes > receiveMemory.limit {
			t.Fatal("memory exceeded charged budget")
		}
	})
	return
}

// Single preferred-only regression uses the identical one-leg child path with
// and without S1 headroom. It does not claim to localize SS2022/socket tail.
func TestResearchB10SingleLegServerHeadroomAB(t *testing.T) {
	for _, rate := range []int{600, 700} {
		t.Run(fmt.Sprint(rate), func(t *testing.T) {
			baseline := researchB10LiveThroughput(t, 1, rate, 0, 65, 110, false, false, 128<<20)
			candidate := researchB10LiveThroughput(t, 1, rate, 0, 65, 110, true, false, 128<<20)
			t.Logf("single leg scope-off %.2f Mbps; server S1 %.2f Mbps", baseline, candidate)
			if baseline <= 0 || candidate < baseline*0.98 {
				t.Fatalf("single-leg server throughput below 98%% of scope-off control: %.2f vs %.2f", candidate, baseline)
			}
		})
	}
}

// Extreme asymmetric-path A/B reuses the exact live core, actual link delay,
// Capacity V2 and receipt/repair plumbing rather than an isolated selector.
// The target is no regression versus the Golden selector scope; no simulated
// Mbps threshold is invented to stand in for the 309/309 FIELD replay.
func TestResearchB10WeakBoosterAB(t *testing.T) {
	baseline := researchB10LiveThroughput(t, 1, 1000, 1, 65, 110, false, true, 8<<20)
	candidate := researchB10LiveThroughput(t, 1, 1000, 1, 65, 110, true, true, 8<<20)
	t.Logf("weak booster scope-off %.2f Mbps; B10 server %.2f Mbps", baseline, candidate)
	if baseline <= 0 || candidate < baseline*0.98 {
		t.Fatalf("weak booster degradation: candidate %.2f vs %.2f Mbps", candidate, baseline)
	}
}
