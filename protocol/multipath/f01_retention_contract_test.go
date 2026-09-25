package multipath

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sagernet/sing-box/protocol/multipath/stream"
)

// The same external byte-stream experiment runs against beta6, beta9 and the
// F01 candidate. Its oracle measures reachable arrays, never charge counters.
func TestIndependentRetentionContract(t *testing.T) {
	for _, remaining := range []uint64{0, 1, 17} {
		t.Run(fmt.Sprintf("remaining=%d", remaining), func(t *testing.T) {
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
				// 4097 one-byte writes exceed the retained-array budget on both
				// baselines while keeping the race-instrumented fixture bounded.
				const count = 4097
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
							received <- fmt.Errorf("byte-stream mismatch at %d", n)
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
				ack := uint64(count) - remaining
				if err = writeWireFrame(peer, wireFrame{typ: frameTypeWindow, flow: flowMessage{Next: ack, Limit: 1 << 20, Paths: [2]stream.Receipt{{Generation: leg.path.Generation, Next: ack, ReceivedAt: count}}}}); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
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
				if core.tx.Buffered() != remaining || leg.path.Outstanding() != remaining {
					t.Fatal("feedback did not settle expected byte frontier")
				}
				s := cfg.Memory.snapshot()
				t.Logf("remaining=%d retained_arrays=%d all_noncache_charges=%d", remaining, held, s.UsedBytes-s.CachedBytes)
				if held > s.UsedBytes-s.CachedBytes {
					t.Fatalf("live arrays exceed even ALL non-cache budget charges by %d bytes", held-(s.UsedBytes-s.CachedBytes))
				}
			})
		})
	}
}
