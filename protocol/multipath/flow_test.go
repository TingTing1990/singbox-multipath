package multipath

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// A complete DATA header and part of its payload arrive, then the original
// writer retains the caller's slice while leg0 is free to deliver a duplicate.
type partialBoosterConn struct {
	net.Conn
	started, release, stopped chan struct{}
	closeOnce                 sync.Once
	payload, blocked          bool
}

func newPartialBooster(conn net.Conn) *partialBoosterConn {
	return &partialBoosterConn{Conn: conn, started: make(chan struct{}), release: make(chan struct{}), stopped: make(chan struct{})}
}

func (c *partialBoosterConn) Write(data []byte) (int, error) {
	if len(data) == dataFrameHeaderSize && data[0] == frameTypeData {
		c.payload = true
		return c.Conn.Write(data)
	}
	if c.payload {
		c.payload = false
		if !c.blocked {
			c.blocked = true
			first := min(3, len(data))
			n, err := c.Conn.Write(data[:first])
			if err != nil {
				return n, err
			}
			close(c.started)
			select {
			case <-c.stopped:
				return n, net.ErrClosed
			case <-c.release:
			}
			more, err := c.Conn.Write(data[first:])
			return n + more, err
		}
	}
	return c.Conn.Write(data)
}

func (c *partialBoosterConn) Close() error {
	c.closeOnce.Do(func() { close(c.stopped) })
	return c.Conn.Close()
}

func flowTestConfig() coreConfig {
	cfg := testCoreConfig()
	cfg.ChunkSize, cfg.QueueFrames, cfg.QueueBytes = 1024, 64, 64<<10
	cfg.MaxReorderFrames, cfg.MaxReorderBytes = 64, 8<<10
	cfg.ReplayTimeout = 200 * time.Millisecond
	cfg.Memory = newMemoryBudget(1<<20, false)
	return cfg
}

func flowPayload(size int) []byte {
	data := make([]byte, size)
	for index := range data {
		data[index] = byte(index % 251)
	}
	return data
}

func flowSend(conn net.Conn, data []byte, closeWrite bool) <-chan error {
	result := make(chan error, 1)
	go func() {
		_, err := conn.Write(data)
		if err == nil && closeWrite {
			err = conn.(closeWriter).CloseWrite()
		}
		result <- err
	}()
	return result
}

func assertFlowAlive(t *testing.T, cores ...*mpCore) {
	t.Helper()
	for _, core := range cores {
		if core.isDone() {
			core.failureMu.Lock()
			reason := core.failure
			core.failureMu.Unlock()
			t.Fatalf("healthy leg0 session closed: %s", reason)
		}
		if core.memory.snapshot().PeakUsedBytes > core.memory.snapshot().LimitBytes {
			t.Fatal("memory budget exceeded")
		}
		if core.reorderPeak.Load() > core.cfg.MaxReorderBytes || core.reorderFPeak.Load() > int64(core.cfg.MaxReorderFrames) {
			t.Fatal("reorder limits exceeded")
		}
	}
}

func closeFlowCores(cores ...*mpCore) {
	for _, core := range cores {
		core.Close()
	}
	// Keep the synctest bubble alive while graceful-close timers drain a
	// deliberately stalled writer. Production timers advance independently.
	for _, core := range cores {
		core.workerGroup.Wait()
	}
}

func TestFlowTinyWindowSurvivesPartialBooster(t *testing.T) {
	for _, late := range []bool{false, true} {
		t.Run(map[bool]string{false: "permanent_stall", true: "late_duplicate"}[late], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				left, app := newCore(context.Background(), flowTestConfig())
				right, peerApp := newCore(context.Background(), flowTestConfig())
				defer closeFlowCores(left, right)
				left.activate(activationInfo{Reason: activationReasonBytes})
				a, b := net.Pipe()
				connectTestLeg(t, left, right, 0, a, b)
				a, b = net.Pipe()
				fault := newPartialBooster(a)
				connectTestLeg(t, left, right, 1, fault, b)
				if late {
					go func() { <-fault.started; time.Sleep(350 * time.Millisecond); close(fault.release) }()
				}
				payload := flowPayload(2 << 20)
				result := flowSend(app, payload, true)
				_ = peerApp.SetReadDeadline(time.Now().Add(5 * time.Second))
				got, err := io.ReadAll(peerApp)
				if err != nil {
					t.Fatal(err)
				}
				if err = <-result; err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got, payload) {
					t.Fatal("recovered stream was corrupted")
				}
				if left.fallbackF.Load() == 0 {
					t.Fatal("test did not exercise replay")
				}
				assertFlowAlive(t, left, right)
				// Recovery does not wait for the stalled writer or detach the leg.
				if left.getLeg(1) == nil {
					t.Fatal("single recovery detached leg1")
				}
				if late {
					time.Sleep(400 * time.Millisecond)
					synctest.Wait()
					assertFlowAlive(t, left, right)
				}
			})
		})
	}
}

func TestFlowBidirectionalStallAndMemoryPressure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		leftCfg, rightCfg := flowTestConfig(), flowTestConfig()
		leftCfg.MaxReorderBytes, rightCfg.MaxReorderBytes = 64<<10, 128<<10
		leftCfg.Memory, rightCfg.Memory = newMemoryBudget(512<<10, false), newMemoryBudget(512<<10, false)
		left, app := newCore(context.Background(), leftCfg)
		right, peerApp := newCore(context.Background(), rightCfg)
		defer closeFlowCores(left, right)
		left.activate(activationInfo{Reason: activationReasonBytes})
		right.activate(activationInfo{Reason: activationReasonBytes})
		a, b := net.Pipe()
		connectTestLeg(t, left, right, 0, a, b)
		a, b = net.Pipe()
		connectTestLeg(t, left, right, 1, newPartialBooster(a), newPartialBooster(b))
		payload := flowPayload(1 << 20)
		leftDone, rightDone := flowSend(app, payload, true), flowSend(peerApp, payload, true)
		readDone := make(chan error, 2)
		for _, conn := range []net.Conn{app, peerApp} {
			go func(conn net.Conn) {
				_ = conn.SetReadDeadline(time.Now().Add(8 * time.Second))
				got, err := io.ReadAll(conn)
				if err == nil && !bytes.Equal(got, payload) {
					err = io.ErrUnexpectedEOF
				}
				readDone <- err
			}(conn)
		}
		for range 2 {
			if err := <-readDone; err != nil {
				t.Fatal(err)
			}
		}
		if err := <-leftDone; err != nil {
			t.Fatal(err)
		}
		if err := <-rightDone; err != nil {
			t.Fatal(err)
		}
		if left.fallbackF.Load()+right.fallbackF.Load() == 0 {
			t.Fatal("no fallback exercised")
		}
		assertFlowAlive(t, left, right)
	})
}

func TestFlowSlowReaderDoesNotBlockControlOrReverseData(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		left, app := newCore(context.Background(), flowTestConfig())
		right, peerApp := newCore(context.Background(), flowTestConfig())
		defer closeFlowCores(left, right)
		left.activate(activationInfo{Reason: activationReasonBytes})
		a, b := net.Pipe()
		connectTestLeg(t, left, right, 0, a, b)
		a, b = net.Pipe()
		connectTestLeg(t, left, right, 1, a, b)
		payload := flowPayload(128 << 10)
		written := flowSend(app, payload, false)
		time.Sleep(time.Second)
		synctest.Wait()
		if left.ackedNext.Load() == 0 {
			t.Fatal("receipt ACK blocked behind application delivery")
		}
		if left.fallbackF.Load() != 0 {
			t.Fatal("slow application was treated as network loss")
		}
		if left.rttSnapshot()[0].Samples == 0 {
			t.Fatal("control probe blocked behind full receive window")
		}
		reply := []byte("reverse direction still works")
		replied := flowSend(peerApp, reply, false)
		_ = app.SetReadDeadline(time.Now().Add(time.Second))
		gotReply := make([]byte, len(reply))
		if _, err := io.ReadFull(app, gotReply); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(gotReply, reply) {
			t.Fatal("reverse payload corrupted")
		}
		if err := <-replied; err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len(payload))
		_ = peerApp.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := io.ReadFull(peerApp, got); err != nil {
			t.Fatal(err)
		}
		if err := <-written; err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatal("slow-reader payload corrupted")
		}
		assertFlowAlive(t, left, right)
	})
}

func TestFlowIdleCreditReturnAndReactivation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := flowTestConfig()
		cfg.MaxReorderBytes = 128 << 10
		left, app := newCore(context.Background(), cfg)
		cfg.Memory = newMemoryBudget(1<<20, false)
		right, peerApp := newCore(context.Background(), cfg)
		defer closeFlowCores(left, right)
		a, b := net.Pipe()
		connectTestLeg(t, left, right, 0, a, b)
		for range 3 {
			payload := flowPayload(32 << 10)
			written := flowSend(app, payload, false)
			got := make([]byte, len(payload))
			if _, err := io.ReadFull(peerApp, got); err != nil {
				t.Fatal(err)
			}
			if err := <-written; err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, payload) {
				t.Fatal("payload mismatch after epoch change")
			}
			time.Sleep(750 * time.Millisecond)
			synctest.Wait()
			right.flow.rxMu.Lock()
			held := right.flow.rxLimit - right.flow.rxConsumed
			right.flow.rxMu.Unlock()
			if held != 1 {
				t.Fatalf("idle connection retained %d credits", held)
			}
			assertFlowAlive(t, left, right)
		}
	})
}

func TestFlowMessageCodec(t *testing.T) {
	message := flowMessage{Epoch: 3, Next: 5, Limit: 64, GapAge: 12345, Leg1Bytes: 67890, Flags: flowFlagGap | flowFlagPressure}
	for _, typ := range []byte{frameTypeWindow, frameTypeWindowRequest} {
		a, b := net.Pipe()
		go func() { defer a.Close(); _ = writeWireFrame(a, wireFrame{typ: typ, flow: message}) }()
		core, _ := newCore(context.Background(), testCoreConfig())
		frame, err := readWireFrame(b, core)
		b.Close()
		core.Close()
		if err != nil || frame.typ != typ || frame.flow != message {
			t.Fatalf("flow codec: %+v %v", frame, err)
		}
	}
}

func TestRecoveryTimeoutDefaultsAndBounds(t *testing.T) {
	cfg := flowTestConfig()
	cfg.ReplayTimeout = 0
	core, _ := newCore(context.Background(), cfg)
	defer core.Close()
	if got := core.recoveryTimeout(); got != time.Second {
		t.Fatalf("default timeout %s", got)
	}
	core.probeMu.Lock()
	core.probeRTT[0] = legRTTSnapshot{Samples: 1, EWMA: 65 * time.Millisecond}
	core.probeRTT[1] = legRTTSnapshot{Samples: 1, EWMA: 80 * time.Millisecond, Jitter: 10 * time.Millisecond}
	core.probeMu.Unlock()
	if got := core.recoveryTimeout(); got != 200*time.Millisecond {
		t.Fatalf("adaptive timeout %s", got)
	}
}

func TestMemoryLargeLimitWatermarks(t *testing.T) {
	budget := newMemoryBudget(1<<62, false)
	if budget.boosterLimit <= 0 || budget.boosterResume <= 0 || !budget.boosterAllowed() {
		t.Fatal("large memory watermark overflow")
	}
}

func TestFlowSharedBudgetPreservesEveryLeg0(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		budget := newMemoryBudget(4<<20, false)
		var cores []*mpCore
		var apps, peers []net.Conn
		for range 4 {
			cfg := flowTestConfig()
			cfg.Memory = budget
			left, app := newCore(context.Background(), cfg)
			right, peer := newCore(context.Background(), cfg)
			left.activate(activationInfo{Reason: activationReasonBytes})
			cores = append(cores, left, right)
			apps = append(apps, app)
			peers = append(peers, peer)
			a, b := net.Pipe()
			connectTestLeg(t, left, right, 0, a, b)
		}
		defer closeFlowCores(cores...)
		synctest.Wait()
		snapshot := budget.snapshot()
		held, err := budget.acquire(context.Background(), int(snapshot.BoosterLimitBytes-snapshot.UsedBytes), memoryClassPrimary)
		if err != nil {
			t.Fatal(err)
		}
		defer budget.release(held)
		payload := flowPayload(64 << 10)
		results := make(chan error, len(apps))
		for index, app := range apps {
			written := flowSend(app, payload, false)
			go func(peer net.Conn, written <-chan error) {
				_ = peer.SetReadDeadline(time.Now().Add(5 * time.Second))
				got := make([]byte, len(payload))
				_, readErr := io.ReadFull(peer, got)
				if readErr == nil {
					readErr = <-written
				}
				if readErr == nil && !bytes.Equal(got, payload) {
					readErr = io.ErrUnexpectedEOF
				}
				results <- readErr
			}(peers[index], written)
		}
		for range apps {
			if err = <-results; err != nil {
				t.Fatal(err)
			}
		}
		if !budget.snapshot().Pressure {
			t.Fatal("test did not maintain memory pressure")
		}
		assertFlowAlive(t, cores...)
	})
}

func TestFlowCreditReturnValidation(t *testing.T) {
	core, _ := newCore(context.Background(), flowTestConfig())
	defer core.Close()
	for _, message := range []flowMessage{
		{Next: 2, Limit: 3}, // No corresponding credit was granted.
		{Next: 1, Limit: 0},
		{Epoch: 1, Next: 1, Limit: 2}, // Cannot "return" credit by expanding it.
		{Flags: flowFlagGap, Limit: 1},
	} {
		if core.handleWindowRequest(message) == nil {
			t.Fatalf("accepted invalid request: %+v", message)
		}
	}
}

func TestFlowMinimumBudgetStillTransmits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := flowTestConfig()
		minimum := sessionMemoryReservation(cfg) + int64(cfg.ChunkSize) + receiveFrameOverhead
		cfg.Memory = newMemoryBudget(minimum, false)
		left, app := newCore(context.Background(), cfg)
		cfg.Memory = newMemoryBudget(minimum, false)
		right, peer := newCore(context.Background(), cfg)
		defer closeFlowCores(left, right)
		left.activate(activationInfo{Reason: activationReasonBytes})
		a, b := net.Pipe()
		connectTestLeg(t, left, right, 0, a, b)
		payload := flowPayload(32 << 10)
		written := flowSend(app, payload, false)
		_ = peer.SetReadDeadline(time.Now().Add(5 * time.Second))
		got := make([]byte, len(payload))
		if _, err := io.ReadFull(peer, got); err != nil {
			t.Fatal(err)
		}
		if err := <-written; err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatal("minimum-budget payload corrupted")
		}
		assertFlowAlive(t, left, right)
	})
}
