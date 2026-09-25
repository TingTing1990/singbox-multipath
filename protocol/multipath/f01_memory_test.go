package multipath

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"testing"
	"testing/synctest"
	"time"
	"unsafe"

	"github.com/sagernet/sing-box/protocol/multipath/stream"
)

// This oracle observes the real backing-array capacities, not the implementation's
// record charge counters. It also compiles against the frozen baseline.
func f01RetainedBytes(c *mpCore, leg *mpLeg) int64 {
	sender := reflect.ValueOf(c.tx).Elem().FieldByName("segments")
	flights := reflect.ValueOf(&leg.path).Elem().FieldByName("flights")
	return int64(sender.Cap())*int64(sender.Type().Elem().Size()) +
		int64(flights.Cap())*int64(flights.Type().Elem().Size()) +
		int64(cap(c.mappings))*int64(unsafe.Sizeof(dataMapping{})) +
		int64(cap(leg.preferredCapacityRanges))*int64(unsafe.Sizeof(preferredCapacityPathRange{}))
}

func TestF01RetainedCapacityAcrossIdleSessions(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("capacity=%v", enabled), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := flowTestConfig()
				cfg.AggregationEnabled = false
				cfg.ReplayTimeout = time.Hour
				cfg.Memory = newMemoryBudget(64<<20, false)
				if enabled {
					cfg.PreferredCapacity = newPreferredCapacityController(60, time.Second, cfg.ChunkSize)
				}
				var cores []*mpCore
				defer func() {
					for _, c := range cores {
						c.Close()
					}
					for _, c := range cores {
						<-c.released
					}
					if s := cfg.Memory.snapshot(); s.UsedBytes != s.CachedBytes {
						t.Errorf("shutdown leaked budget: %+v", s)
					}
				}()
				var retainedTotal int64
				for session := 0; session < 3; session++ {
					c, app := newCore(context.Background(), cfg)
					cores = append(cores, c)
					a, peer := net.Pipe()
					defer peer.Close()
					leg, err := c.addLeg(0, a, nil)
					if err != nil {
						t.Fatal(err)
					}
					// Cross the old compaction thresholds and several growth steps
					// without quadratic tiny-frame scans dominating race runs.
					const count = 4097
					received := make(chan error, 1)
					go func() {
						scratch := make([]byte, cfg.ChunkSize)
						n := 0
						for {
							frame, readErr := readFrame(peer, scratch)
							if readErr != nil {
								if n < count {
									received <- readErr
								}
								return
							}
							if frame.typ != frameTypeData {
								continue
							}
							if frame.seq != uint64(n) || len(frame.data) != 1 || frame.data[0] != byte(n) {
								received <- fmt.Errorf("unexpected byte at %d", n)
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
					c.stateMu.Lock()
					burstBytes := f01RetainedBytes(c, leg)
					c.stateMu.Unlock()
					if burstBytes < 256<<10 {
						t.Fatal("fixture did not grow the metadata arrays")
					}
					ack := flowMessage{Next: count, Limit: 1 << 20}
					ack.Paths[0] = stream.Receipt{Generation: leg.path.Generation, Next: count, ReceivedAt: count}
					if err = writeWireFrame(peer, wireFrame{typ: frameTypeWindow, flow: ack}); err != nil {
						t.Fatal(err)
					}
					synctest.Wait()
					c.stateMu.Lock()
					buffered, outstanding := c.tx.Buffered(), leg.path.Outstanding()
					retained := f01RetainedBytes(c, leg)
					c.stateMu.Unlock()
					if buffered != 0 || outstanding != 0 {
						t.Fatal("fixture ACK did not drain sender/path")
					}
					retainedTotal += retained
					s := cfg.Memory.snapshot()
					t.Logf("session=%d burst_arrays=%d idle_arrays=%d idle_total=%d used=%d cached=%d", session, burstBytes, retained, retainedTotal, s.UsedBytes, s.CachedBytes)
					if retainedTotal > s.UsedBytes-s.CachedBytes {
						t.Fatalf("F01: retained arrays %d exceed ALL non-cache charges %d", retainedTotal, s.UsedBytes-s.CachedBytes)
					}
					if c.isDone() {
						t.Fatal("idle test session was closed to release its storage")
					}
				}
			})
		})
	}
}

func TestF01PendingWriteSurvivesRecordPressure(t *testing.T) {
	for _, closePending := range []bool{false, true} {
		t.Run(fmt.Sprintf("close=%v", closePending), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := flowTestConfig()
				cfg.AggregationEnabled = false
				c, app := newCore(context.Background(), cfg)
				defer c.Close()
				for n := 0; n < 16; n++ {
					if _, err := app.Write([]byte{byte(n)}); err != nil {
						t.Fatal(err)
					}
				}
				synctest.Wait()
				s := cfg.Memory.snapshot()
				fill := s.LimitBytes - s.UsedBytes
				if !cfg.Memory.reserveSession(fill) {
					t.Fatal("could not hold remaining budget")
				}
				if _, err := app.Write([]byte{16}); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				c.stateMu.Lock()
				queued := c.tx.WriteNext
				c.stateMu.Unlock()
				if queued != 16 || c.isDone() {
					t.Fatal("record pressure did not preserve a pending accepted write")
				}
				if closePending {
					c.Close()
					<-c.released
					cfg.Memory.releaseSession(fill)
				} else {
					cfg.Memory.releaseSession(fill)
					synctest.Wait()
					c.stateMu.Lock()
					for n := 0; n < 17; n++ {
						segment, ok := c.tx.Range(uint64(n), 1)
						if !ok || segment.Data()[0] != byte(n) {
							t.Errorf("accepted byte %d lost or corrupted", n)
						}
					}
					c.stateMu.Unlock()
					c.Close()
					<-c.released
				}
				if s := cfg.Memory.snapshot(); s.UsedBytes != s.CachedBytes {
					t.Fatalf("pending write shutdown leaked: %+v", s)
				}
			})
		})
	}
}

// A checked-out payload must no longer be reachable from the cache index,
// including slots beyond len. Exercise both acquisition paths.
func TestF01CacheDropsCheckedOutReferences(t *testing.T) {
	for _, reserved := range []bool{false, true} {
		t.Run(fmt.Sprintf("reserved=%v", reserved), func(t *testing.T) {
			b := newMemoryBudget(8<<20, false)
			const size = 1024
			for i := 0; i < 128; i++ {
				b.putReservedBuffer(make([]byte, size))
			}
			var taken [][]byte
			for i := 0; i < 124; i++ {
				var buffer []byte
				if reserved {
					buffer = b.takeReservedBuffer(size)
				} else {
					buffer, _ = b.tryAcquirePrimary(size)
				}
				if buffer == nil {
					t.Fatal("cached payload unavailable")
				}
				taken = append(taken, buffer)
				b.access.Lock()
				index := b.cache[size]
				for _, unused := range index[len(index):cap(index)] {
					if unused != nil {
						t.Error("cache checkout retains payload beyond len")
						break
					}
				}
				b.access.Unlock()
			}
			b.access.Lock()
			index := b.cache[size]
			if len(index) != 4 || cap(index) > 16 {
				t.Errorf("sparse cache index retained peak: len=%d cap=%d", len(index), cap(index))
			}
			for n, buffer := range index[:cap(index)] {
				if n >= len(index) && buffer != nil {
					t.Error("unused cache slot retains checked-out payload")
				}
			}
			b.access.Unlock()
			snapshot := b.snapshot()
			wantUsed := int64(128 * size)
			if reserved {
				wantUsed = 4 * size
			}
			if snapshot.CachedBytes != 4*size || snapshot.UsedBytes != wantUsed {
				t.Fatalf("cache ownership charges changed: %+v", snapshot)
			}
			if !reserved {
				for _, buffer := range taken {
					b.release(buffer)
				}
			}
			b.access.Lock()
			b.dropCacheLocked()
			b.access.Unlock()
			if s := b.snapshot(); s.UsedBytes != 0 || s.CachedBytes != 0 {
				t.Fatalf("cache drain leaked charges: %+v", s)
			}
		})
	}
}
