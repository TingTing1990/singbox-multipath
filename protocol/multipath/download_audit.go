package multipath

import (
	"container/list"
	"crypto/rand"
	"encoding/hex"
	"strings"
	"sync"
	"time"

	D "github.com/sagernet/sing-box/protocol/multipath/downloadevidence"
)

type downloadRecord struct {
	header   D.Header
	decision D.Decision
	transfer D.Transfer
	feedback D.Feedback
	session  D.Session
	snapshot D.Snapshot
}

type downloadAudit struct {
	mu              sync.Mutex
	queue           list.List
	wake            chan struct{}
	done            chan struct{}
	closeOnce       sync.Once
	startOnce       sync.Once
	started         time.Time
	epoch, instance string
	seq, dropped    uint64
	closed          bool
	totals          D.Totals
	cfg             D.Config
	memory          *memoryBudget
	capacity        *preferredCapacityController
	write           func(string)
}

func newDownloadAudit(instance string, cfg coreConfig, write func(string)) (*downloadAudit, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, err
	}
	a := &downloadAudit{wake: make(chan struct{}, 1), done: make(chan struct{}), started: time.Now(), epoch: hex.EncodeToString(id[:]), instance: instance, memory: cfg.Memory, capacity: cfg.PreferredCapacity, write: write, cfg: downloadConfig(cfg)}
	return a, nil
}

func downloadConfig(c coreConfig) D.Config {
	x := D.Config{Aggregation: c.AggregationEnabled, ActivationOnQueue: c.ActivationOnQueue, ChunkSize: c.ChunkSize, QueueFrames: c.QueueFrames, QueueBytes: c.QueueBytes, ThresholdBytesPS: c.ThresholdBytesPS, ActivationAfterBytes: c.ActivationAfterBytes, ActivationAfterBytesMinBytesPS: c.ActivationAfterBytesMinBytesPS, ActivationWindowNS: int64(c.ActivationWindow), MaxReorderFrames: c.MaxReorderFrames, MaxReorderBytes: c.MaxReorderBytes, ReplayBytes: c.ReplayBytes, ReplayTimeoutNS: int64(c.ReplayTimeout)}
	if c.PreferredCapacity != nil {
		x.CapacityTargetBytesPS = c.PreferredCapacity.targetBytesPS
	}
	return x
}

// mu is held only while copying fixed-size observation records; never during
// JSON serialization, logging, memory snapshotting or acquisition of core locks.
func (a *downloadAudit) record(r downloadRecord) {
	if a == nil {
		return
	}
	if len(r.session.Reason) > 512 {
		r.session.Reason = strings.Clone(r.session.Reason[:512]) + " [truncated]"
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return
	}
	switch r.header.Kind {
	case "DECISION":
		a.totals.Decisions++
		if r.decision.Outcome == "submitted" {
			if r.decision.Repair {
				a.totals.AssignedRepair[r.decision.Selected] += uint64(r.decision.Length)
			} else {
				a.totals.AssignedNormal[r.decision.Selected] += uint64(r.decision.Length)
			}
		} else {
			a.totals.SubmitFailures++
		}
	case "WAIT":
		a.totals.Waits++
	case "WRITE":
		if r.transfer.Outcome == "written" {
			if r.transfer.Repair {
				a.totals.WrittenRepair[r.transfer.Leg] += r.transfer.Bytes
			} else {
				a.totals.WrittenNormal[r.transfer.Leg] += r.transfer.Bytes
			}
		} else {
			a.totals.WriteFailures++
		}
	case "FEEDBACK":
		a.totals.ConfirmedLogical += r.feedback.LogicalBytes
		for i := range a.totals.ConfirmedPath {
			a.totals.ConfirmedPath[i] += r.feedback.PathBytes[i]
		}
	case "SESSION_OPEN":
		a.totals.SessionsStarted++
	case "SESSION_CLOSE":
		a.totals.SessionsClosed++
	}
	a.enqueueLocked(r)
}

func (a *downloadAudit) enqueueLocked(r downloadRecord) {
	a.seq++
	now := time.Now()
	r.header.Schema = D.Version
	r.header.Epoch = a.epoch
	r.header.Instance = a.instance
	r.header.Side = "server"
	r.header.Direction = "download"
	r.header.Seq = a.seq
	r.header.At = now
	r.header.MonoNS = now.Sub(a.started).Nanoseconds()
	r.header.Dropped = a.dropped
	a.queue.PushBack(r)
	select {
	case a.wake <- struct{}{}:
	default:
	}
}

func (a *downloadAudit) dequeue() (downloadRecord, bool, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	front := a.queue.Front()
	if front != nil {
		r := front.Value.(downloadRecord)
		a.queue.Remove(front)
		return r, true, false
	}
	return downloadRecord{}, false, a.closed
}

func (a *downloadAudit) snapshot(kind string) {
	a.snapshotFinal(kind, false)
}

func (a *downloadAudit) snapshotFinal(kind string, final bool) {
	if a == nil {
		return
	}
	m, _ := downloadMemory(a.memory)
	capacity := downloadCapacity(a.capacity)
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.closed {
		a.enqueueLocked(downloadRecord{header: D.Header{Kind: kind}, snapshot: D.Snapshot{Totals: a.totals, Memory: m, Config: a.cfg, Capacity: capacity}})
		if final {
			a.closed = true
			select {
			case a.wake <- struct{}{}:
			default:
			}
		}
	}
}

func (a *downloadAudit) start() {
	if a == nil {
		return
	}
	a.startOnce.Do(func() {
		a.snapshot("START")
		go func() {
			defer close(a.done)
			timer := time.NewTicker(time.Second)
			defer timer.Stop()
			for {
				select {
				case <-timer.C:
					a.snapshot("WINDOW")
				default:
				}
				r, ok, closed := a.dequeue()
				if !ok {
					if closed {
						return
					}
					select {
					case <-a.wake:
					case <-timer.C:
						a.snapshot("WINDOW")
					}
					continue
				}
				var data any
				switch r.header.Kind {
				case "START", "WINDOW", "STOP":
					data = r.snapshot
				case "SESSION_OPEN", "SESSION_CLOSE", "ACTIVATION", "SESSION_ESTABLISHED", "PATH_ATTACHED", "PATH_FAILED":
					data = r.session
				case "DECISION", "WAIT":
					data = r.decision
				case "WRITE":
					data = r.transfer
				case "FEEDBACK":
					data = r.feedback
				}
				encoded, err := D.Encode(r.header, data)
				if err != nil {
					a.mu.Lock()
					a.dropped++
					a.mu.Unlock()
					continue
				}
				a.write(D.Marker + string(encoded))
			}
		}()
	})
}

func (a *downloadAudit) close() {
	if a == nil {
		return
	}
	a.closeOnce.Do(func() { a.snapshotFinal("STOP", true) })
}

func (c *mpCore) downloadRecord(kind string) downloadRecord {
	return downloadRecord{header: D.Header{Kind: kind, Session: c.cfg.DownloadSession, Destination: c.cfg.CapacityAuditDestination}}
}

// Caller holds stateMu. This never rolls Capacity windows or reserves credit.
func (c *mpCore) downloadDecision(length int) D.Decision {
	_, allowed := downloadMemory(c.memory)
	d := D.Decision{Length: length, Candidate: -1, Selected: -1, Active: c.active.Load(), PeerPressure: c.peerPressure, MemoryAllowed: allowed}
	for _, leg := range c.availableLegs() {
		writing, blocked := leg.writingSnapshot(time.Now())
		d.Paths[leg.id] = D.Path{Present: true, Ready: leg.ready.Load(), Busy: leg.busy, Stale: leg.path.Stale, Generation: leg.path.Generation, RateBytesPS: leg.path.Rate, SRTTNS: int64(leg.path.SRTT), RTTVarNS: int64(leg.path.RTTVar), MinRTTNS: int64(leg.path.MinimumRTT), Outstanding: leg.path.Outstanding(), Pipeline: leg.path.Pipeline(min(uint64(c.cfg.QueueBytes), uint64(c.cfg.ChunkSize)*4), uint64(c.cfg.ReplayBytes)), Backlog: leg.backlogBytes(), Writing: writing, WriteBlockedNS: int64(blocked)}
	}
	d.Capacity = downloadCapacity(c.cfg.PreferredCapacity)
	return d
}

func (c *mpCore) auditWait(reason string) {
	if c.cfg.DownloadAudit == nil {
		return
	}
	// A bounded periodic/transition record preserves stalls without a busy-loop
	// log flood. It is not advertised as a trace of every unsuccessful poll.
	now := time.Now()
	if reason == c.downloadWaitReason && now.Sub(c.downloadWaitAt) < time.Second {
		return
	}
	c.downloadWaitReason, c.downloadWaitAt = reason, now
	r := c.downloadRecord("WAIT")
	r.decision = c.downloadDecision(0)
	r.decision.Reason = reason
	r.decision.Outcome = "waiting"
	c.cfg.DownloadAudit.record(r)
}

// Shared-controller observation only; no capacityState/roll/reserve side effect.
// Other sessions may transact after this snapshot. Actual override is recorded
// from the original reserveAssignment return, not inferred from this snapshot.
func downloadCapacity(p *preferredCapacityController) D.Capacity {
	if p == nil {
		return D.Capacity{}
	}
	p.mu.Lock()
	s := p.snapshotLocked()
	p.mu.Unlock()
	return D.Capacity{Enabled: true, TargetBytesPS: s.TargetBytesPS, DeliveryBytesPS: s.DeliveryRate, ProtectedBytesPS: s.ProtectedBytesPS, Ready: s.DeliveryReady, ProtectionActive: s.ProtectionActive, ProtectionValid: s.ProtectionValid, CreditBytes: s.CreditBytes, WindowSeq: s.WindowSequence}
}

// Read storage directly under its owner lock. The existing snapshot and
// boosterAllowed APIs update pressure/peaks and wake waiters; observers must
// never call them. "allowed" describes the stored state at observation time,
// not a second admission decision and not an atomic cross-session prediction.
func downloadMemory(b *memoryBudget) (D.Memory, bool) {
	if b == nil {
		return D.Memory{}, true
	}
	b.access.Lock()
	defer b.access.Unlock()
	return D.Memory{Limit: b.limit, Used: b.used, Cached: b.cached, BoosterLimit: b.boosterLimit,
		Resume: b.boosterResume, PeakUsed: b.peakUsed, PeakCached: b.peakCached, Pressure: b.pressure,
		PressureEvents: b.pressureCount, BackpressureEvents: b.waitCount}, !b.pressure && b.used < b.boosterLimit
}
