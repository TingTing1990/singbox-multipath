package multipath

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/sagernet/sing-box/protocol/multipath/stream"
)

// Preserve the 20,000-record high water and 17000 -> 19000 -> 19999 ACKs.
// This is the metadata ownership contract. Drive the real submission/feedback
// APIs synchronously, draining only the test transport queue, so constructing
// the high water does not repeatedly scan all unacknowledged mappings in the
// asynchronous repair scheduler (quadratic work under the race detector).
// TestIndependentRetentionContract and TestF01RetainedCapacityAcrossIdleSessions
// retain the real net.Pipe/app/pump coverage; no production path is bypassed by
// this fixture outside this test.
func TestIndependentRetentionContractStagedACK(t *testing.T) {
	for _, capacity := range []bool{false, true} {
		t.Run(fmt.Sprintf("capacity=%v", capacity), func(t *testing.T) {
			cfg := flowTestConfig()
			cfg.AggregationEnabled = false
			cfg.Memory = newMemoryBudget(64<<20, false)
			if capacity {
				cfg.PreferredCapacity = newPreferredCapacityController(60, time.Second, cfg.ChunkSize)
			}
			const count = 20000
			reservation := minimumSessionMemory(cfg)
			if !cfg.Memory.reserveSession(reservation) {
				t.Fatal("session reserve failed")
			}
			core := &mpCore{cfg: cfg, memory: cfg.Memory, tx: stream.NewSender(count+1, cfg.Memory), rx: stream.NewReceiver(1<<20, nil),
				legs: make(map[uint8]*mpLeg), pumpWake: make(chan struct{}, 1), txWake: make(chan struct{}, 1)}
			leg := &mpLeg{id: 0, send: make(chan wireFrame, 1), recordMemory: cfg.Memory,
				path: stream.Path{Generation: 1, RecordMemory: cfg.Memory}}
			leg.ready.Store(true)
			leg.path.ReleaseFlight = func(prepaid bool) {
				if prepaid {
					leg.prepaidFlights--
				} else {
					cfg.Memory.releaseSession(128)
				}
			}
			core.legs[0] = leg
			defer func() {
				leg.path.Close()
				leg.closePreferredCapacityRanges()
				core.tx.Close()
				core.rx.Close()
				stream.CloseRecords(&core.mappings, &core.mappingHead, &core.mappingBytes, core.mappingReserve[:], cfg.Memory)
				cfg.Memory.releaseSession(reservation)
				if s := cfg.Memory.snapshot(); s.UsedBytes != s.CachedBytes {
					t.Errorf("fixture leaked charge: %+v", s)
				}
			}()
			now := time.Unix(1000, 0)
			for n := 0; n < count; n++ {
				payload, wait := cfg.Memory.tryAcquirePrimary(1)
				if payload == nil || wait != nil {
					t.Fatal("fixture payload allocation failed")
				}
				payload[0] = byte(n)
				buffer := stream.NewBuffer(payload, func() { cfg.Memory.release(payload) })
				if err := core.tx.Append(buffer); err != nil {
					buffer.Release()
					t.Fatal(err)
				}
				segment, ok := core.tx.NextRange(1)
				if !ok {
					t.Fatal("no next test segment")
				}
				if err := core.submitLocked(leg, segment, false, now); err != nil {
					t.Fatal(err)
				}
				// Independent byte/generation oracle for the submitted frame, not a mock
				// of accounting or ACK processing. Match the writer's ownership release.
				frame := <-leg.send
				if frame.seq != uint64(n) || frame.pathSeq != uint64(n) || frame.generation != 1 || len(frame.data) != 1 || frame.data[0] != byte(n) {
					t.Fatal("submitted frame mismatch")
				}
				frame.buffer.Release()
				leg.queuedBytes.Add(-1)
				leg.busy = false
			}
			capacities := func() [4]int64 {
				var held [4]int64
				for i, item := range []struct {
					value any
					field string
				}{{core.tx, "segments"}, {&leg.path, "flights"}, {core, "mappings"}, {leg, "preferredCapacityRanges"}} {
					f := reflect.ValueOf(item.value).Elem().FieldByName(item.field)
					held[i] = int64(f.Cap()) * int64(f.Type().Elem().Size())
				}
				return held
			}
			high := capacities()
			for i, held := range high {
				if (i < 3 || capacity) && held < 20000 {
					t.Fatalf("high water absent array=%d bytes=%d", i, held)
				}
			}
			for _, ack := range []uint64{17000, 19000, 19999} {
				if err := core.handleWindow(flowMessage{Next: ack, Limit: count + 1, Paths: [2]stream.Receipt{{Generation: 1, Next: ack, ReceivedAt: count}}}); err != nil {
					t.Fatal(err)
				}
				remaining := uint64(count) - ack
				if core.tx.Buffered() != remaining || leg.path.Outstanding() != remaining {
					t.Fatal("incorrect staged frontier")
				}
				sizes := capacities()
				var held int64
				for _, size := range sizes {
					held += size
				}
				s := cfg.Memory.snapshot()
				if held > s.UsedBytes-s.CachedBytes {
					t.Fatalf("ACK=%d retained arrays=%d exceed charges=%d", ack, held, s.UsedBytes-s.CachedBytes)
				}
				t.Logf("capacity=%v ACK=%d remaining=%d arrays=%v held=%d charges=%d", capacity, ack, remaining, sizes, held, s.UsedBytes-s.CachedBytes)
			}
			if sizes := capacities(); sizes[0] >= high[0] || sizes[1] >= high[1] || sizes[2] >= high[2] || (capacity && sizes[3] >= high[3]) {
				t.Fatal("staged tail retained high-water capacity")
			}
		})
	}
}
