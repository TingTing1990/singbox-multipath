package multipath

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/sagernet/sing-box/protocol/multipath/audit"
	D "github.com/sagernet/sing-box/protocol/multipath/downloadevidence"
	"github.com/sagernet/sing-box/protocol/multipath/stream"
)

func TestDownloadProducerToOffline(t *testing.T) {
	var mu sync.Mutex
	var output bytes.Buffer
	cfg := flowTestConfig()
	cfg.DownloadSession = "0123456789abcdef0123456789abcdef"
	observer, err := newDownloadAudit("mp-in", cfg, func(s string) { mu.Lock(); fmt.Fprintln(&output, s); mu.Unlock() })
	if err != nil {
		t.Fatal(err)
	}
	observer.start()
	cfg.DownloadAudit = observer
	left, app := newCore(context.Background(), cfg)
	right, peer := newCore(context.Background(), flowTestConfig())
	defer func() { left.Close(); right.Close() }()
	left.activate(activationInfo{Reason: activationReasonBytes})
	a, b := net.Pipe()
	connectTestLeg(t, left, right, 0, a, b)
	a, b = net.Pipe()
	connectTestLeg(t, left, right, 1, a, b)
	data := flowPayload(32 << 10)
	app.SetDeadline(time.Now().Add(5 * time.Second))
	peer.SetDeadline(time.Now().Add(5 * time.Second))
	// Exercise the reverse direction independently; server TX evidence must
	// not count this client upload as download delivery.
	uploadData := flowPayload(12345)
	uploaded := flowSend(peer, uploadData, false)
	uploadGot := make([]byte, len(uploadData))
	if _, err := io.ReadFull(app, uploadGot); err != nil || !bytes.Equal(uploadGot, uploadData) {
		t.Fatalf("upload fixture: %v", err)
	}
	if err := <-uploaded; err != nil {
		t.Fatal(err)
	}
	sent := flowSend(app, data, true)
	got, err := io.ReadAll(peer)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("download failed: bytes=%d err=%v", len(got), err)
	}
	if err = <-sent; err != nil {
		t.Fatal(err)
	}
	// Wait for the ACK including FIN. Only DATA must enter the logical counter.
	until := time.Now().Add(3 * time.Second)
	for !left.ackedFIN.Load() && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if !left.ackedFIN.Load() {
		t.Fatal("FIN ACK missing")
	}
	left.Close()
	right.Close()
	<-left.released
	<-right.released
	observer.snapshot("WINDOW")
	observer.close()
	<-observer.done
	mu.Lock()
	raw := output.String()
	mu.Unlock()
	if dir := os.Getenv("DOWNLOAD_AUDIT_EVIDENCE_DIR"); dir != "" {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "producer.log"), []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
	}
	report, err := audit.AnalyzeServerDownload(strings.NewReader(raw), "mp-in", "")
	if err != nil {
		t.Fatalf("producer replay failed: %v", err)
	}
	var logical, written, assigned uint64
	generations := map[int]uint64{}
	for _, e := range report.Events {
		if e.Kind == "WRITE" {
			var v D.Transfer
			if err = json.Unmarshal(e.Data, &v); err != nil {
				t.Fatal(err)
			}
			if v.Outcome == "written" {
				written += v.Bytes
				generations[v.Leg] = v.Generation
			}
		}
		if e.Kind == "DECISION" {
			var v D.Decision
			json.Unmarshal(e.Data, &v)
			if v.Outcome == "submitted" {
				assigned += uint64(v.Length)
				if v.Selected < 0 || v.Selected > 1 {
					t.Fatal("bad selection")
				}
			}
		}
	}
	for _, w := range report.Windows {
		logical += w.Delta.ConfirmedLogical
	}
	if logical != uint64(len(data)) || written != uint64(len(data)) || assigned != uint64(len(data)) {
		t.Fatalf("independent application byte oracle: logical=%d written=%d assigned=%d want=%d", logical, written, assigned, len(data))
	}
	if len(generations) != 2 {
		t.Fatal("two physical legs not exercised")
	}
	// Fault injection against real producer output; unrelated journal lines do not
	// create completeness, and one omitted producer event must reject the report.
	lines := strings.Split(strings.TrimSpace(raw), "\n")
	for i, line := range lines {
		if strings.Contains(line, `"kind":"DECISION"`) {
			mutant := append([]string{}, lines[:i]...)
			mutant = append(mutant, lines[i+1:]...)
			r, e := audit.AnalyzeServerDownload(strings.NewReader(strings.Join(mutant, "\n")), "mp-in", "")
			if e == nil || r.Complete {
				t.Fatal("dropped decision accepted")
			}
			break
		}
	}
}

func TestDownloadQueueBoundedAndDropVisible(t *testing.T) {
	cfg := flowTestConfig()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var output bytes.Buffer
	a, err := newDownloadAudit("mp-in", cfg, func(s string) { once.Do(func() { close(entered); <-release }); fmt.Fprintln(&output, s) })
	if err != nil {
		t.Fatal(err)
	}
	a.start()
	<-entered
	for i := 0; i < downloadAuditQueue*4; i++ {
		a.record(downloadRecord{header: D.Header{Kind: "WAIT", Session: "x"}, decision: D.Decision{Reason: "test", Outcome: "waiting"}})
	}
	if len(a.queue) != downloadAuditQueue {
		t.Fatal("queue not bounded")
	}
	if unsafe.Sizeof(downloadRecord{})*downloadAuditQueue > 2<<20 {
		t.Fatal("observation queue exceeds fixed 2 MiB bound")
	}
	a.mu.Lock()
	drops := a.dropped
	a.mu.Unlock()
	if drops == 0 {
		t.Fatal("overflow not counted")
	}
	close(release)
	deadline := time.Now().Add(time.Second)
	for len(a.queue) > 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	a.snapshot("WINDOW")
	a.close()
	<-a.done
	r, e := audit.AnalyzeServerDownload(strings.NewReader(output.String()), "mp-in", "")
	if e == nil || r.Complete || !strings.Contains(e.Error(), "dropped") {
		t.Fatalf("overflow accepted: %v", e)
	}
}

func TestDownloadFeedbackGenerationDuplicateAndFIN(t *testing.T) {
	cfg := flowTestConfig()
	a, _ := newDownloadAudit("test", cfg, func(string) {})
	cfg.DownloadAudit = a
	c := &mpCore{cfg: cfg, memory: cfg.Memory, tx: stream.NewSender(100), rx: stream.NewReceiver(100, nil), legs: map[uint8]*mpLeg{}, txWake: make(chan struct{}, 1), pumpWake: make(chan struct{}, 1)}
	c.tx.Append(stream.NewBuffer(make([]byte, 100), func() {}))
	seg, _ := c.tx.NextRange(100)
	c.tx.Sent(seg)
	leg := &mpLeg{id: 0, path: stream.Path{Generation: 2, Sent: 100}}
	c.legs[0] = leg
	msg := flowMessage{Next: 100, Limit: 1000}
	msg.Paths[0] = stream.Receipt{Generation: 1, Next: 100, ReceivedAt: 1}
	if err := c.handleWindow(msg); err != nil {
		t.Fatal(err)
	}
	msg.Paths[0].Generation = 2
	if err := c.handleWindow(msg); err != nil {
		t.Fatal(err)
	}
	if err := c.handleWindow(msg); err != nil {
		t.Fatal(err)
	}
	c.tx.CloseWrite()
	if !c.tx.SendFIN() {
		t.Fatal("no FIN")
	}
	msg.Next = 101
	if err := c.handleWindow(msg); err != nil {
		t.Fatal(err)
	}
	if a.totals.ConfirmedLogical != 100 || a.totals.ConfirmedPath[0] != 100 {
		t.Fatalf("duplicate/generation/FIN accounting: %+v", a.totals)
	}
}

// This function is a byte-for-byte copy of the frozen choice body (only its
// name changed). It is an independent parity oracle, never production code.
func (c *mpCore) frozenDownloadChoiceLocked(length int) *mpLeg {
	legs := c.availableLegs()
	provisionalRate := float64(0)
	for _, leg := range legs {
		provisionalRate = max(provisionalRate, leg.path.Rate)
	}
	var chosen *mpLeg
	score := math.Inf(1)
	for _, leg := range legs {
		if c.cfg.Recovery != nil && !c.cfg.Recovery.allows(leg.id) {
			continue
		}
		if leg.path.Stale || leg.busy {
			continue
		}
		emergency := c.cfg.Recovery != nil && c.controlLeg() == leg && leg.id == 1
		if (!c.active.Load() && leg.id == 0) || emergency {
			return leg
		}
		if leg.id == 1 && (!c.active.Load() || !leg.ready.Load() || c.peerPressure || !c.memory.boosterAllowed()) {
			continue
		}
		// Startup sampling is bounded. Thereafter the connection-level byte
		// window and memory, not an extra per-path cwnd, bound lookahead.
		initial := min(uint64(c.cfg.QueueBytes), uint64(c.cfg.ChunkSize)*4)
		pipeline := leg.path.Pipeline(initial, uint64(c.cfg.ReplayBytes))
		if leg.path.Outstanding()+uint64(length) > pipeline {
			continue
		}
		next := leg.path.DrainTime(provisionalRate)
		if next < score {
			chosen, score = leg, next
		}
	}
	return chosen
}

func TestDownloadChoiceParity(t *testing.T) {
	for mask := 0; mask < 512; mask++ {
		cfg := flowTestConfig()
		c := &mpCore{cfg: cfg, memory: cfg.Memory, legs: map[uint8]*mpLeg{}}
		for i := 0; i < 2; i++ {
			l := &mpLeg{id: uint8(i), path: stream.Path{Generation: uint64(i + 1), Rate: float64(1000 + i*1000), Sent: uint64(i * 100)}}
			l.ready.Store(mask&(1<<i) == 0)
			l.busy = mask&(1<<(i+2)) != 0
			l.path.Stale = mask&(1<<(i+4)) != 0
			c.legs[uint8(i)] = l
		}
		c.active.Store(mask&64 != 0)
		c.peerPressure = mask&128 != 0
		if mask&256 != 0 {
			c.legs[0].path.Sent = 1 << 30
		}
		want := c.frozenDownloadChoiceLocked(1024)
		trace := c.downloadDecision(1024)
		got := c.choosePathObservedLocked(1024, &trace)
		if got != want {
			t.Fatalf("scheduler changed for state mask=%d", mask)
		}
	}
}

func (c *mpCore) frozenDownloadSubmitChoiceLocked(length int, now time.Time) pathSelection {
	chosen := c.frozenDownloadChoiceLocked(length)
	controller := c.cfg.PreferredCapacity
	if controller == nil || chosen == nil {
		return pathSelection{leg: chosen}
	}

	// Every normal preferred assignment, including preferred-only connections,
	// is accounted by the one shared instance controller. Once additive
	// protection is active, these assignments consume the shared protected-rate
	// reservation. This prevents N logical sessions from each receiving an
	// independent N x target/protection reservation.
	if chosen.id == 0 {
		_, reservation := controller.reserveAssignment(now, length, true, false)
		return pathSelection{leg: chosen, reservation: reservation}
	}

	// Before this connection's original beta6 activation condition fires, the
	// capacity feature must not create a second path decision of its own. A leg1
	// candidate here can only be an existing recovery/failover decision.
	if !c.active.Load() {
		return pathSelection{leg: chosen}
	}
	if c.cfg.Recovery != nil && !c.cfg.Recovery.allows(0) {
		return pathSelection{leg: chosen}
	}

	preferred := c.getLeg(0)
	if preferred == nil || preferred.path.Stale || preferred.busy || !preferred.ready.Load() {
		return pathSelection{leg: chosen}
	}
	initial := min(uint64(c.cfg.QueueBytes), uint64(c.cfg.ChunkSize)*4)
	pipeline := preferred.path.Pipeline(initial, uint64(c.cfg.ReplayBytes))
	if preferred.path.Outstanding()+uint64(length) > pipeline {
		return pathSelection{leg: chosen}
	}

	force, reservation := controller.reserveAssignment(now, length, false, true)
	if force {
		return pathSelection{leg: preferred, reservation: reservation}
	}
	return pathSelection{leg: chosen}
}

func TestDownloadCapacityOverrideParity(t *testing.T) {
	for mask := 0; mask < 64; mask++ {
		t0 := time.Unix(100, 0)
		makeCore := func(observed bool) *mpCore {
			controller := newPreferredCapacityController(60, time.Second, 64<<10)
			if mask&1 != 0 {
				primePreferredProtection(t, controller, t0, 90_000_000/8)
			}
			c, p, b := capacitySchedulerCore(controller)
			preferBoosterForCapacityTest(p, b, uint64(c.cfg.ChunkSize))
			p.busy = mask&2 != 0
			p.ready.Store(mask&4 == 0)
			c.active.Store(mask&8 == 0)
			if mask&16 != 0 {
				p.path.Sent = uint64(c.cfg.ReplayBytes)
			}
			if observed {
				c.cfg.DownloadAudit, _ = newDownloadAudit("parity", c.cfg, func(string) {})
			}
			return c
		}
		plain, observed := makeCore(false), makeCore(true)
		for i := 0; i < 8; i++ {
			now := t0.Add(time.Second + time.Duration(i)*time.Millisecond)
			a := plain.frozenDownloadSubmitChoiceLocked(65536, now)
			b := observed.choosePathForSubmitLocked(65536, now)
			id := func(l *mpLeg) int {
				if l == nil {
					return -1
				}
				return int(l.id)
			}
			if id(a.leg) != id(b.leg) || b.trace == nil || b.trace.Selected != id(b.leg) {
				t.Fatalf("choice/trace mismatch mask=%d", mask)
			}
			var submitErr error
			if mask&32 != 0 {
				submitErr = errMemoryLimit
			}
			a.finish(submitErr)
			b.finish(submitErr)
			if plain.cfg.PreferredCapacity.snapshot() != observed.cfg.PreferredCapacity.snapshot() {
				t.Fatalf("Capacity mutated by observer mask=%d", mask)
			}
		}
	}
}

func TestDownloadConcurrentStopIsFinal(t *testing.T) {
	var output bytes.Buffer
	a, _ := newDownloadAudit("stop", flowTestConfig(), func(s string) { fmt.Fprintln(&output, s) })
	a.start()
	var group sync.WaitGroup
	for i := 0; i < 8; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			a.record(downloadRecord{header: D.Header{Kind: "WAIT", Session: "s"}, decision: D.Decision{Reason: "test", Outcome: "waiting"}})
			a.close()
		}()
	}
	group.Wait()
	<-a.done
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if !strings.Contains(lines[len(lines)-1], `"kind":"STOP"`) {
		t.Fatal("event emitted after STOP")
	}
}

// A reader must not acknowledge pressure, update peaks, close a waiter channel,
// or emit pressure events. Deliberately seed a pending transition to expose
// calls to the mutating memoryBudget.snapshot/boosterAllowed APIs.
func TestDownloadObserverCannotAdvanceMemoryControl(t *testing.T) {
	cfg := flowTestConfig()
	b := cfg.Memory
	b.access.Lock()
	b.used = 100
	b.pressure = true
	b.pressureSince = time.Unix(10, 0)
	b.peakUsed = 0
	changed := b.changed
	b.access.Unlock()
	a, err := newDownloadAudit("read-only", cfg, func(string) {})
	if err != nil {
		t.Fatal(err)
	}
	c := &mpCore{cfg: cfg, memory: b, legs: make(map[uint8]*mpLeg)}
	c.cfg.DownloadAudit = a
	for i := 0; i < 10; i++ {
		a.snapshot("WINDOW")
		_ = c.downloadDecision(1024)
	}
	b.access.Lock()
	defer b.access.Unlock()
	if !b.pressure || b.pressureSince != time.Unix(10, 0) || b.peakUsed != 0 || b.changed != changed || len(b.events) != 0 {
		t.Fatalf("observer advanced memory control: pressure=%v since=%v peak=%d changed=%v events=%d", b.pressure, b.pressureSince, b.peakUsed, b.changed != changed, len(b.events))
	}
}
