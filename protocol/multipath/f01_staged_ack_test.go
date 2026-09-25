package multipath

import (
	"context"
	"net"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sagernet/sing-box/protocol/multipath/stream"
)

// Regression for the confirmed partial-ACK blind spot in the chat candidate.
// The sender is acknowledged in stages 17000 -> 19000 -> 19999, leaving one
// live byte after a previous compaction. The oracle reads real cap*element-size,
// not implementation charge counters.
func TestIndependentRetentionContractStagedACK(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := flowTestConfig()
		cfg.AggregationEnabled = false
		cfg.Memory = newMemoryBudget(64<<20, false)
		v := reflect.ValueOf(&cfg).Elem()
		for _, field := range []string{"ReplayTimeout", "PathStallTimeoutMin"} {
			if f := v.FieldByName(field); f.IsValid() {
				f.SetInt(int64(time.Hour))
			}
		}
		frameSize := 1024
		for _, field := range []string{"ChunkSize", "FrameSize"} {
			if f := v.FieldByName(field); f.IsValid() {
				frameSize = int(f.Int())
			}
		}
		core, app := newCore(context.Background(), cfg)
		defer func() { core.Close(); <-core.released }()
		transport, peer := net.Pipe()
		defer peer.Close()
		leg, err := core.addLeg(0, transport, nil)
		if err != nil {
			t.Fatal(err)
		}

		const count = 20000
		received := make(chan error, 1)
		go func() {
			scratch := make([]byte, frameSize)
			for n := 0; ; {
				frame, err := readFrame(peer, scratch)
				if err != nil {
					if n < count {
						received <- err
					}
					return
				}
				if frame.typ != frameTypeData {
					continue
				}
				if frame.seq != uint64(n) || len(frame.data) != 1 || frame.data[0] != byte(n) {
					received <- errStagedACKFixture
					return
				}
				n++
				if n == count {
					received <- nil
				}
			}
		}()
		if err = writeWireFrame(peer, wireFrame{typ: frameTypeWindow, flow: flowMessage{Limit: 1 << 20}}); err != nil {
			t.Fatal(err)
		}
		for n := 0; n < count; n++ {
			if _, err = app.Write([]byte{byte(n)}); err != nil {
				t.Fatal(err)
			}
		}
		if err = <-received; err != nil {
			t.Fatal(err)
		}
		for _, ack := range []uint64{17000, 19000, 19999} {
			if err = writeWireFrame(peer, wireFrame{typ: frameTypeWindow, flow: flowMessage{Next: ack, Limit: 1 << 20, Paths: [2]stream.Receipt{{Generation: leg.path.Generation, Next: ack, ReceivedAt: count}}}}); err != nil {
				t.Fatal(err)
			}
			synctest.Wait()
		}

		core.stateMu.Lock()
		defer core.stateMu.Unlock()
		var held int64
		for _, item := range []struct {
			value any
			field string
		}{{core.tx, "segments"}, {&leg.path, "flights"}, {core, "mappings"}, {leg, "preferredCapacityRanges"}} {
			f := reflect.ValueOf(item.value).Elem().FieldByName(item.field)
			if f.IsValid() {
				held += int64(f.Cap()) * int64(f.Type().Elem().Size())
			}
		}
		if core.tx.Buffered() != 1 || leg.path.Outstanding() != 1 {
			t.Fatalf("staged ACK did not leave one live byte: buffered=%d outstanding=%d", core.tx.Buffered(), leg.path.Outstanding())
		}
		s := cfg.Memory.snapshot()
		if held > s.UsedBytes-s.CachedBytes {
			t.Fatalf("staged ACK retained arrays %d exceed all non-cache charges %d", held, s.UsedBytes-s.CachedBytes)
		}
	})
}

var errStagedACKFixture = &stagedACKFixtureError{}

type stagedACKFixtureError struct{}

func (*stagedACKFixtureError) Error() string { return "staged ACK byte-stream fixture mismatch" }
