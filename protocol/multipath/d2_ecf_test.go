package multipath

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing-box/protocol/multipath/stream"
)

// Exercise the actual Golden Capacity submission wrapper, with only the
// server-side research scope switched on. No stubbed path-selection function.
func d2ECFTestCore() (*mpCore, *mpLeg, *mpLeg) {
	c, p, b := capacitySchedulerCore(nil)
	c.cfg.ServerDownload = true
	c.tx = stream.NewSender(8 << 20)
	c.writerWaiting.Store(true)
	p.path.MinimumRTT = 20 * time.Millisecond
	b.path.MinimumRTT = 100 * time.Millisecond
	return c, p, b
}

func TestD2ECFWeakBusyPreferredReject(t *testing.T) {
	c, p, b := d2ECFTestCore()
	p.busy = true
	p.path.Rate = 100_000_000
	b.path.Rate = 100_000
	p.path.Sent, b.path.Sent = 500_000, 0
	if sel := c.choosePathForSubmitLocked(c.cfg.ChunkSize, time.Now()); sel.leg != nil {
		t.Fatal("weak booster displaced faster temporarily busy preferred")
	}
}

func TestD2ECFHealthyBoosterRemainsEligible(t *testing.T) {
	c, p, b := d2ECFTestCore()
	p.busy = true
	p.path.Rate = 20_000_000 // 160 Mbps preferred
	b.path.Rate = 75_000_000 // 600 Mbps booster
	p.path.MinimumRTT = 65 * time.Millisecond
	b.path.MinimumRTT = 110 * time.Millisecond
	p.path.Sent = 1 << 20
	if sel := c.choosePathForSubmitLocked(c.cfg.ChunkSize, time.Now()); sel.leg != b {
		t.Fatal("healthy 160+600 booster starved")
	}
}

func TestD2ECFLegacyUploadAndInactive(t *testing.T) {
	c, p, b := d2ECFTestCore()
	p.busy = true
	p.path.Rate = 100_000_000
	b.path.Rate = 100_000
	c.cfg.ServerDownload = false
	if sel := c.choosePathForSubmitLocked(c.cfg.ChunkSize, time.Now()); sel.leg != b {
		t.Fatal("upload did not retain Golden choice")
	}
	c.cfg.ServerDownload = true
	c.active.Store(false)
	if sel := c.choosePathForSubmitLocked(c.cfg.ChunkSize, time.Now()); sel.leg != nil {
		t.Fatal("inactive was allowed to use secondary")
	}
}

func TestD2ECFBLESTWindowWait(t *testing.T) {
	c, p, b := d2ECFTestCore()
	p.busy = true
	p.path.Rate = 100_000_000
	b.path.Rate = 10_000_000
	p.path.Sent = 4 << 20
	// Enough outstanding to keep preferred theoretically best, even as its
	// physical writer is busy; a minimal window must block slower new DATA.
	c.tx.WindowEnd = uint64(c.cfg.ChunkSize)
	if sel := c.choosePathForSubmitLocked(c.cfg.ChunkSize, time.Now()); sel.leg != nil {
		t.Fatal("BLEST receiver-window rejection missing")
	}
}

// A healthy server Recovery group is not a failover state; its explicit
// two-path mask must retain normal beta10 aggregate selection. If a path is
// disallowed, the Golden recovery semantics continue to own selection.
func TestD2ECFHealthyRecoveryGroupUsesAggregateSelection(t *testing.T) {
	c, preferred, booster := d2ECFTestCore()
	policy := new(recoveryPolicy)
	policy.mask.Store(3)
	c.cfg.Recovery = policy
	preferred.busy = true
	preferred.path.Rate = 100_000_000
	booster.path.Rate = 100_000
	preferred.path.Sent = 500_000
	if sel := c.choosePathForSubmitLocked(c.cfg.ChunkSize, time.Now()); sel.leg != nil {
		t.Fatal("weak booster was assigned despite healthy two-path recovery group")
	}
	policy.mask.Store(2)
	if sel := c.choosePathForSubmitLocked(c.cfg.ChunkSize, time.Now()); sel.leg != booster {
		t.Fatal("single-path recovery must retain Golden emergency selection")
	}
}

// Regression for the previously untested B10 tail case: leg0 is actually
// eligible, while the theoretical fastest booster cannot accept a frame.
// Golden can keep sending preferred; a WAIT here would bypass Capacity V2.
func TestD2ECFReadyPreferredNotBlockedByBusyBooster(t *testing.T) {
	c, preferred, booster := d2ECFTestCore()
	c.writerWaiting.Store(false)
	preferred.path.Rate = 20_000_000
	preferred.path.MinimumRTT = 100 * time.Millisecond
	booster.path.Rate = 75_000_000
	booster.path.MinimumRTT = 10 * time.Millisecond
	booster.busy = true
	selection := c.choosePathForSubmitLocked(c.cfg.ChunkSize, time.Now())
	if selection.leg != preferred {
		t.Fatalf("eligible preferred was held by busy booster: leg=%v", legIDForCapacityTest(selection.leg))
	}
	selection.finish(nil)
}

// The same bug must not bypass the shared controller's natural preferred
// assignment accounting. Both sessions use the real Capacity V2 controller.
func TestD2ECFPreferredCapacityReservationOnBusyBooster(t *testing.T) {
	ctrl := newPreferredCapacityController(60, time.Second, 64<<10)
	now := time.Now()
	primePreferredProtection(t, ctrl, now.Add(-2*time.Second), 90_000_000/8)
	cores := make([]*mpCore, 2)
	for i := range cores {
		c, preferred, booster := d2ECFTestCore()
		c.cfg.PreferredCapacity = ctrl
		c.writerWaiting.Store(false)
		preferred.path.Rate, preferred.path.MinimumRTT = 20_000_000, 100*time.Millisecond
		booster.path.Rate, booster.path.MinimumRTT = 75_000_000, 10*time.Millisecond
		booster.busy = true
		cores[i] = c
	}
	before := ctrl.snapshot().AssignedBytes
	for _, c := range cores {
		sel := c.choosePathForSubmitLocked(c.cfg.ChunkSize, now)
		if sel.leg == nil || sel.leg.id != 0 || sel.reservation == nil {
			t.Fatalf("natural preferred Capacity reservation lost: selected=%v reservation=%v", legIDForCapacityTest(sel.leg), sel.reservation)
		}
		sel.finish(nil)
	}
	after := ctrl.snapshot().AssignedBytes
	if after-before != 2*uint64(cores[0].cfg.ChunkSize) {
		t.Fatalf("shared Capacity commit mismatch: before=%d after=%d", before, after)
	}
	// Refund must never count a failed assignment as peer delivery or commit.
	sel := cores[0].choosePathForSubmitLocked(cores[0].cfg.ChunkSize, now.Add(time.Millisecond))
	if sel.reservation == nil {
		t.Fatal("missing failure-path reservation")
	}
	sel.finish(errMemoryLimit)
	if ctrl.snapshot().AssignedBytes != after {
		t.Fatal("failed submit was committed")
	}
}

// A theoretically faster booster must not make the currently eligible
// preferred leg wait when the booster cannot accept this frame because its
// original startup pipeline is full. This uses the actual Path.Pipeline and
// the shared Capacity V2 controller; it is not a mocked eligibility result.
func TestD2ECFReadyPreferredNotBlockedByFullBooster(t *testing.T) {
	ctrl := newPreferredCapacityController(60, time.Second, 64<<10)
	now := time.Now()
	primePreferredProtection(t, ctrl, now.Add(-2*time.Second), 90_000_000/8)
	c, preferred, booster := d2ECFTestCore()
	c.cfg.PreferredCapacity = ctrl
	c.writerWaiting.Store(false)
	preferred.path.Rate = 1_000_000
	preferred.path.SRTT = 400 * time.Millisecond
	preferred.path.MinimumRTT = 400 * time.Millisecond
	preferred.path.Sent, preferred.path.Received = 0, 0
	booster.path.Rate = 100_000_000
	booster.path.SRTT = 0 // keeps the real Path in its startup pipeline
	booster.path.MinimumRTT = 0
	initial := min(uint64(c.cfg.QueueBytes), uint64(c.cfg.ChunkSize)*4)
	booster.path.Sent, booster.path.Received = initial, 0
	c.tx.WindowEnd = 8 << 20
	if booster.path.Outstanding()+uint64(c.cfg.ChunkSize) <= booster.path.Pipeline(initial, uint64(c.cfg.ReplayBytes)) {
		t.Fatal("test precondition invalid: booster must be pipeline-full")
	}
	preferredCost := preferred.path.CompletionTime(c.cfg.ChunkSize, 100_000_000, 400*time.Millisecond, now)
	boosterCost := booster.path.CompletionTime(c.cfg.ChunkSize, 100_000_000, 400*time.Millisecond, now)
	if boosterCost >= preferredCost {
		t.Fatalf("test precondition invalid: booster must be theoretically faster: preferred=%v booster=%v", preferredCost, boosterCost)
	}
	before := ctrl.snapshot().AssignedBytes
	selection := c.choosePathForSubmitLocked(c.cfg.ChunkSize, now)
	if selection.leg != preferred || selection.reservation == nil {
		t.Fatalf("pipeline-full booster blocked preferred/Capacity: leg=%v reservation=%v", legIDForCapacityTest(selection.leg), selection.reservation)
	}
	selection.finish(nil)
	if got := ctrl.snapshot().AssignedBytes - before; got != uint64(c.cfg.ChunkSize) {
		t.Fatalf("preferred Capacity commit mismatch: got=%d want=%d", got, c.cfg.ChunkSize)
	}
}

// Exercise the actual DATA writer rather than only stream.Buffer's borrow API.
// This covers full-frame headroom, sliced/reinjected fallback, header identity,
// child write errors and headroom-release on failure.
func TestS1WireFrameBytesAndFallback(t *testing.T) {
	for _, offset := range []int{0, 1} {
		raw := make([]byte, 256)
		for i := 0; i < 100; i++ {
			raw[32+i] = byte((i * 7) % 251)
		}
		buffer := stream.NewBufferWithHeadroom(raw, 32, 100, nil)
		data := buffer.Data[offset:]
		golden := append([]byte(nil), data...)
		a, b := net.Pipe()
		gotCh := make(chan wireFrame, 1)
		errCh := make(chan error, 1)
		go func() {
			frame, err := readFrame(b, make([]byte, 1024))
			gotCh <- frame
			errCh <- err
		}()
		want := wireFrame{typ: frameTypeData, seq: 0x10203, pathSeq: 0x40506, generation: 7, data: data, buffer: buffer}
		if err := writeWireFrame(a, want); err != nil {
			t.Fatalf("offset=%d write: %v", offset, err)
		}
		got := <-gotCh
		if err := <-errCh; err != nil {
			t.Fatalf("offset=%d read: %v", offset, err)
		}
		a.Close()
		b.Close()
		if got.seq != want.seq || got.pathSeq != want.pathSeq || got.generation != want.generation || !bytes.Equal(got.data, golden) {
			t.Fatalf("offset=%d DATA wire bytes diverged", offset)
		}
		buffer.Release()
	}
}

type s1FailingWriter struct{ net.Conn }

func (*s1FailingWriter) Write(p []byte) (int, error) { return min(len(p), 1), io.ErrUnexpectedEOF }
func TestS1WireFrameHeadroomReturnedOnChildError(t *testing.T) {
	raw := make([]byte, 256)
	buffer := stream.NewBufferWithHeadroom(raw, 32, 100, nil)
	frame := wireFrame{typ: frameTypeData, seq: 1, pathSeq: 1, generation: 1, data: buffer.Data, buffer: buffer}
	if err := writeWireFrame(&s1FailingWriter{}, frame); err == nil {
		t.Fatal("child failure swallowed")
	}
	if _, ok := buffer.TakeHeadroom(buffer.Data, dataFrameHeaderSize); !ok {
		t.Fatal("headroom held after child failure")
	}
	buffer.ReturnHeadroom()
	buffer.Release()
}

// Golden's single-leg staging research is scoped to inactive server TCP
// downloads. Active aggregate traffic and upload must not allocate S1 headroom.
func TestS1InactiveServerHeadroomScope(t *testing.T) {
	c := &mpCore{cfg: coreConfig{ServerDownload: true, ChunkSize: 4096}, memory: newMemoryBudget(32<<20, false), done: make(chan struct{}), txReserve: make(chan []byte, 1)}
	buf, err := c.nextTXBuffer()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := buf.TakeHeadroom(buf.Data, dataFrameHeaderSize); !ok {
		t.Fatal("inactive server did not get S1 headroom")
	}
	buf.ReturnHeadroom()
	buf.Release()
	c.active.Store(true)
	buf, err = c.nextTXBuffer()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := buf.TakeHeadroom(buf.Data, dataFrameHeaderSize); ok {
		t.Fatal("active dual-leg frame received unauthorized S1 headroom")
	}
	buf.Release()
	c.active.Store(false)
	c.cfg.ServerDownload = false
	buf, err = c.nextTXBuffer()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := buf.TakeHeadroom(buf.Data, dataFrameHeaderSize); ok {
		t.Fatal("client-upload frame received unauthorized S1 headroom")
	}
	buf.Release()
}
