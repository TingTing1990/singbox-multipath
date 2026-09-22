package multipath

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter/inbound"
	"github.com/sagernet/sing-box/log"
)

func waitForAuditEvent(t *testing.T, controller *preferredCapacityController, kind preferredCapacityAuditEventKind, timeout time.Duration) preferredCapacityAuditEvent {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		events, _ := controller.drainAuditEvents()
		for _, event := range events {
			if event.Kind == kind {
				return event
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for audit event %s", kind)
	return preferredCapacityAuditEvent{}
}

func auditActivationCore(controller *preferredCapacityController, policy *recoveryPolicy, sessionID, destination string) *mpCore {
	core := &mpCore{
		cfg: coreConfig{
			Recovery:                 policy,
			PreferredCapacity:        controller,
			AggregationEnabled:       true,
			ActivationOnQueue:        false,
			ActivationAfterBytes:     2 << 20,
			ActivationWindow:         time.Second,
			CapacityAuditSessionID:   sessionID,
			CapacityAuditDestination: destination,
		},
		done:     make(chan struct{}),
		activeCh: make(chan struct{}),
	}
	return core
}

func findAuditEvent(events []preferredCapacityAuditEvent, kind preferredCapacityAuditEventKind) (preferredCapacityAuditEvent, bool) {
	for _, event := range events {
		if event.Kind == kind {
			return event, true
		}
	}
	return preferredCapacityAuditEvent{}, false
}

func TestPreferredCapacityServerAuditReadyAndDisabledIsolation(t *testing.T) {
	disabled := newPreferredCapacityController(70, time.Second, 64<<10)
	disabled.recordAuditReady(time.Unix(100, 0))
	disabled.observePreferredDelivery(70_000_000/8, time.Unix(101, 0))
	events, dropped := disabled.drainAuditEvents()
	if len(events) != 0 || dropped != 0 {
		t.Fatalf("audit-disabled controller emitted evidence: events=%+v dropped=%d", events, dropped)
	}

	enabled := newPreferredCapacityController(700, time.Second, 64<<10)
	enabled.enableAudit()
	enabled.recordAuditReady(time.Unix(200, 0))
	events, dropped = enabled.drainAuditEvents()
	if dropped != 0 || len(events) != 1 || events[0].Kind != preferredCapacityAuditReady {
		t.Fatalf("CAP_AUDIT_READY missing: events=%+v dropped=%d", events, dropped)
	}
	if events[0].TargetBytesPS != 700_000_000/8 || events[0].NormalBoosterAdmitted {
		t.Fatalf("CAP_AUDIT_READY has incorrect controller state: %+v", events[0])
	}
}

func TestPreferredCapacityServerAuditBlockedAfterOriginalBytesTrigger(t *testing.T) {
	controller := newPreferredCapacityController(700, time.Second, 64<<10)
	controller.enableAudit()
	t0 := time.Now().Add(-100 * time.Millisecond)
	controller.capacityState(t0)

	core := auditActivationCore(controller, nil, "a4bd1539", "speedtest.example:8080")
	core.ingressBytes.Store(3 << 20)
	go core.activationLoop()
	event := waitForAuditEvent(t, controller, preferredCapacityAuditGateBlocked, 750*time.Millisecond)
	close(core.done)

	if event.OriginalTrigger != activationReasonBytes || !event.OriginalTriggerOK {
		t.Fatalf("original trigger not proven: %+v", event)
	}
	if event.CurrentBytes < 3<<20 || event.ThresholdBytes != 2<<20 {
		t.Fatalf("bytes trigger evidence incomplete: %+v", event)
	}
	if event.DeliveryReady || event.NormalBoosterAdmitted || event.RecoveryBypass {
		t.Fatalf("below-target gate did not prove a normal block: %+v", event)
	}
	if event.TargetBytesPS != 700_000_000/8 {
		t.Fatalf("target=%d, want %d", event.TargetBytesPS, 700_000_000/8)
	}
	if event.SessionID != "a4bd1539" || event.Destination != "speedtest.example:8080" {
		t.Fatalf("session correlation missing: %+v", event)
	}
	if core.active.Load() {
		t.Fatal("blocked capacity gate still activated booster DATA")
	}
}

func TestPreferredCapacityServerAuditOpenedAndProtectionArmed(t *testing.T) {
	controller := newPreferredCapacityController(70, time.Second, 64<<10)
	controller.enableAudit()
	now := time.Now()
	controller.capacityState(now.Add(-time.Second))
	controller.observePreferredDelivery(90_000_000/8, now)
	if !controller.snapshot().DeliveryReady {
		t.Fatal("test setup failed to establish capacity readiness")
	}
	// Remove setup window/change events so this assertion concerns the admission decision.
	controller.drainAuditEvents()

	core := auditActivationCore(controller, nil, "11223344", "download.example:443")
	core.ingressBytes.Store(4 << 20)
	go core.activationLoop()
	deadline := time.Now().Add(750 * time.Millisecond)
	for !core.active.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !core.active.Load() {
		close(core.done)
		t.Fatal("ready capacity gate did not activate")
	}
	close(core.done)

	events, dropped := controller.drainAuditEvents()
	if dropped != 0 {
		t.Fatalf("unexpected audit evidence loss: %d", dropped)
	}
	opened, ok := findAuditEvent(events, preferredCapacityAuditGateOpened)
	if !ok {
		t.Fatalf("CAP_GATE_OPENED missing: %+v", events)
	}
	armed, ok := findAuditEvent(events, preferredCapacityAuditProtectionArmed)
	if !ok {
		t.Fatalf("CAP_PROTECTION_ARMED missing: %+v", events)
	}
	if !opened.DeliveryReady || !opened.NormalBoosterAdmitted || opened.RecoveryBypass {
		t.Fatalf("opened event is not a normal admission: %+v", opened)
	}
	if opened.SessionID != "11223344" || armed.SessionID != "11223344" {
		t.Fatalf("admission/protection events cannot be correlated to the session: opened=%+v armed=%+v", opened, armed)
	}
	if !armed.ProtectionActive || !armed.ProtectionValid || armed.ProtectedBytesPS != 90_000_000/8 {
		t.Fatalf("armed event does not freeze delivery-proven P: %+v", armed)
	}
	if armed.ProtectedBytesPS <= armed.TargetBytesPS {
		t.Fatalf("target was reported as the protected cap: %+v", armed)
	}
}

func TestPreferredCapacityRecoveryBypassIsNotReportedAsNormalAdmission(t *testing.T) {
	controller := newPreferredCapacityController(70, time.Second, 64<<10)
	controller.enableAudit()
	policy := &recoveryPolicy{}
	policy.update(1, 0b10, 1) // preferred unavailable; booster allowed.
	core := auditActivationCore(controller, policy, "55667788", "recovery.example:443")
	core.ingressBytes.Store(3 << 20)
	go core.activationLoop()

	event := waitForAuditEvent(t, controller, preferredCapacityAuditGateOpened, 750*time.Millisecond)
	close(core.done)
	if !event.RecoveryBypass || event.NormalBoosterAdmitted {
		t.Fatalf("recovery bypass was misreported as normal capacity admission: %+v", event)
	}
	if controller.snapshot().ProtectionActive {
		t.Fatal("recovery bypass incorrectly armed additive protection")
	}
	events, _ := controller.drainAuditEvents()
	if _, ok := findAuditEvent(events, preferredCapacityAuditProtectionArmed); ok {
		t.Fatalf("recovery bypass emitted CAP_PROTECTION_ARMED: %+v", events)
	}
}

func TestPreferredCapacityServerAuditWindowAndPhysicalDegradationReason(t *testing.T) {
	controller := newPreferredCapacityController(70, time.Second, 64<<10)
	controller.enableAudit()
	t0 := time.Unix(5000, 0)
	controller.capacityState(t0)
	controller.observePreferredDelivery(90_000_000/8, t0.Add(time.Second))
	snapshot := controller.capacityState(t0.Add(time.Second))
	if !controller.activateProtection(t0.Add(time.Second), snapshot) {
		t.Fatal("test setup failed to arm protection")
	}
	controller.drainAuditEvents()

	for window := 1; window <= preferredCapacityDegradeWindows; window++ {
		start := t0.Add(time.Duration(window) * time.Second)
		_, reservation := controller.reserveAssignment(start.Add(100*time.Millisecond), 90_000_000/8, true, false)
		if reservation == nil {
			t.Fatal("preferred assignment reservation missing")
		}
		reservation.commit()
		controller.observePreferredDelivery(60_000_000/8, start.Add(time.Second))
		controller.capacityState(start.Add(time.Second))
	}

	events, dropped := controller.drainAuditEvents()
	if dropped != 0 {
		t.Fatalf("unexpected audit evidence loss: %d", dropped)
	}
	windowCount := 0
	activeWindowSeen := false
	var changed preferredCapacityAuditEvent
	for _, event := range events {
		if event.Kind == preferredCapacityAuditWindow {
			windowCount++
			if event.ProtectionActive && event.NormalBoosterAdmitted {
				activeWindowSeen = true
			}
		}
		if event.Kind == preferredCapacityAuditProtectionChanged && event.NewProtectedBytesPS < event.OldProtectedBytesPS {
			changed = event
		}
	}
	if windowCount < preferredCapacityDegradeWindows {
		t.Fatalf("controller windows are not directly observable: count=%d events=%+v", windowCount, events)
	}
	if !activeWindowSeen {
		t.Fatalf("CAP_WINDOW does not expose normal-booster/protection state: %+v", events)
	}
	if changed.Kind == "" {
		t.Fatalf("physical degradation protection change missing: %+v", events)
	}
	if changed.ChangeReason != "physical_delivery_degradation" {
		t.Fatalf("protection decrease lacks causal reason: %+v", changed)
	}
	if changed.AssignmentRate+uint64((64<<10)*2) < changed.OldProtectedBytesPS {
		t.Fatalf("degradation was reported without sufficient preferred assignment: %+v", changed)
	}
	if changed.NewProtectedBytesPS != 60_000_000/8 {
		t.Fatalf("unexpected degraded protection: %+v", changed)
	}
}

func assertCapacityControlStateEqual(t *testing.T, a, b preferredCapacitySnapshot) {
	t.Helper()
	if a.TargetBytesPS != b.TargetBytesPS ||
		a.DeliveredBytes != b.DeliveredBytes ||
		a.AssignedBytes != b.AssignedBytes ||
		a.DeliveryRate != b.DeliveryRate ||
		a.AssignmentRate != b.AssignmentRate ||
		a.DeliveryReady != b.DeliveryReady ||
		a.ProtectedBytesPS != b.ProtectedBytesPS ||
		a.ProtectionValid != b.ProtectionValid ||
		a.ProtectionActive != b.ProtectionActive ||
		a.DegradeWindows != b.DegradeWindows ||
		a.CreditBytes != b.CreditBytes {
		t.Fatalf("audit changed controller state:\nwith audit: %+v\nwithout audit: %+v", a, b)
	}
}

func TestPreferredCapacityAuditIsDecisionEquivalent(t *testing.T) {
	withAudit := newPreferredCapacityController(70, time.Second, 64<<10)
	withoutAudit := newPreferredCapacityController(70, time.Second, 64<<10)
	withAudit.enableAudit()
	t0 := time.Unix(6000, 0)

	for _, controller := range []*preferredCapacityController{withAudit, withoutAudit} {
		controller.capacityState(t0)
		controller.observePreferredDelivery(90_000_000/8, t0.Add(time.Second))
		snapshot := controller.capacityState(t0.Add(time.Second))
		controller.activateProtection(t0.Add(time.Second), snapshot)
	}
	assertCapacityControlStateEqual(t, withAudit.snapshot(), withoutAudit.snapshot())

	coreAudit, preferredAudit, boosterAudit := capacitySchedulerCore(withAudit)
	corePlain, preferredPlain, boosterPlain := capacitySchedulerCore(withoutAudit)
	preferBoosterForCapacityTest(preferredAudit, boosterAudit, uint64(coreAudit.cfg.ChunkSize))
	preferBoosterForCapacityTest(preferredPlain, boosterPlain, uint64(corePlain.cfg.ChunkSize))

	for n := 0; n < 250; n++ {
		now := t0.Add(time.Second + time.Duration(n)*5*time.Millisecond)
		a := coreAudit.choosePathForSubmitLocked(coreAudit.cfg.ChunkSize, now)
		b := corePlain.choosePathForSubmitLocked(corePlain.cfg.ChunkSize, now)
		if legIDForCapacityTest(a.leg) != legIDForCapacityTest(b.leg) {
			t.Fatalf("audit changed scheduler decision at %d: audit=%v plain=%v", n, legIDForCapacityTest(a.leg), legIDForCapacityTest(b.leg))
		}
		a.finish(nil)
		b.finish(nil)
	}
	assertCapacityControlStateEqual(t, withAudit.snapshot(), withoutAudit.snapshot())
}

func TestPreferredCapacityAuditQueueIsBoundedAndLossIsExplicit(t *testing.T) {
	controller := newPreferredCapacityController(70, time.Second, 64<<10)
	controller.enableAudit()
	for n := 0; n < preferredCapacityAuditQueueLimit+17; n++ {
		controller.recordAuditReady(time.Unix(int64(7000+n), 0))
	}
	events, dropped := controller.drainAuditEvents()
	if len(events) != preferredCapacityAuditQueueLimit {
		t.Fatalf("audit queue is not bounded: len=%d", len(events))
	}
	if dropped != 17 {
		t.Fatalf("audit evidence loss is not explicit: dropped=%d", dropped)
	}
}

func TestPreferredCapacityAuditLogLineCarriesFieldAcceptanceEvidence(t *testing.T) {
	event := preferredCapacityAuditEvent{
		Sequence:              7,
		Kind:                  preferredCapacityAuditGateBlocked,
		At:                    time.Unix(8000, 0),
		SessionID:             "a4bd1539",
		Destination:           "speedtest.example:8080",
		OriginalTrigger:       activationReasonBytes,
		OriginalTriggerOK:     true,
		TargetBytesPS:         700_000_000 / 8,
		DeliveryRate:          550_000_000 / 8,
		DeliveryReady:         false,
		NormalBoosterAdmitted: false,
		CurrentBytes:          581_745_969,
		ThresholdBytes:        2 << 20,
	}
	line := event.logLine("mp-in-39001")
	for _, required := range []string{
		"event=CAP_GATE_BLOCKED",
		"side=server",
		"instance=\"mp-in-39001\"",
		"session_id=\"a4bd1539\"",
		"destination=\"speedtest.example:8080\"",
		"original_trigger=bytes",
		"original_trigger_satisfied=true",
		"target_mbps=700.00",
		"delivery_mbps=550.00",
		"delivery_ready=false",
		"normal_booster_admitted=false",
		"current_bytes=581745969",
		"threshold_bytes=2097152",
	} {
		if !strings.Contains(line, required) {
			t.Fatalf("audit log line missing %q: %s", required, line)
		}
	}
}

type capacityAuditCaptureLogger struct {
	log.ContextLogger
	lines []string
}

func (l *capacityAuditCaptureLogger) ErrorContext(_ context.Context, args ...any) {
	l.lines = append(l.lines, fmt.Sprint(args...))
}

func (l *capacityAuditCaptureLogger) InfoContext(_ context.Context, args ...any) {
	l.lines = append(l.lines, fmt.Sprint(args...))
}

func TestPreferredCapacityAuditDroppedSurvivesInstanceFilter(t *testing.T) {
	controller := newPreferredCapacityController(70, time.Second, 64<<10)
	controller.enableAudit()
	for n := 0; n < preferredCapacityAuditQueueLimit+1; n++ {
		controller.recordAuditReady(time.Unix(int64(9000+n), 0))
	}
	logger := &capacityAuditCaptureLogger{ContextLogger: log.NewNOPFactory().Logger()}
	h := &Inbound{
		Adapter: inbound.NewAdapter("multipath", "mp-in-39002"),
		ctx:     context.Background(),
		logger:  logger,
		cfg:     coreConfig{PreferredCapacity: controller},
	}
	h.flushPreferredCapacityAudit()

	var droppedLine string
	for _, line := range logger.lines {
		if strings.Contains(line, "event=CAP_AUDIT_DROPPED") {
			droppedLine = line
			break
		}
	}
	if droppedLine == "" {
		t.Fatal("CAP_AUDIT_DROPPED not emitted")
	}
	for _, required := range []string{`instance="mp-in-39002"`, "evidence_complete=false"} {
		if !strings.Contains(droppedLine, required) {
			t.Fatalf("DROPPED evidence does not survive instance filter requirement %q: %s", required, droppedLine)
		}
	}
}
