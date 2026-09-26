package audit

import (
	"errors"
	"strings"
	"testing"
	"time"
)

const capBlocked = `CAP_AUDIT schema_version=1 event_seq=7 event=CAP_GATE_BLOCKED side=server instance="mp-in-39002" session_id="a4bd1539" destination="speedtest.example:8080" at=2026-09-26T01:02:03.123456789Z original_trigger=bytes original_trigger_satisfied=true recovery_bypass=false target_mbps=700.00 delivery_mbps=640.25 delivery_ready=false protected_mbps=0.00 protection_valid=false protection_active=false preferred_assignment_mbps=630.00 degrade_windows=0 normal_booster_admitted=false current_bytes=3145728 threshold_bytes=2097152 trigger_window_bytes=0 trigger_rate_mbps=0.00 trigger_threshold_mbps=0.00 backlog_bytes=0 queue_bytes=0 old_protected_mbps=0.00 new_protected_mbps=0.00 change_reason= controller_window_seq=31`

const capOpened = `CAP_AUDIT schema_version=1 event_seq=8 event=CAP_GATE_OPENED side=server instance="mp-in-39002" session_id="11223344" destination="download.example:443" at=2026-09-26T01:02:04Z original_trigger=bytes original_trigger_satisfied=true recovery_bypass=false target_mbps=70.00 delivery_mbps=90.00 delivery_ready=true protected_mbps=90.00 protection_valid=true protection_active=true preferred_assignment_mbps=88.50 degrade_windows=0 normal_booster_admitted=true current_bytes=4194304 threshold_bytes=2097152 trigger_window_bytes=0 trigger_rate_mbps=0.00 trigger_threshold_mbps=0.00 backlog_bytes=0 queue_bytes=0 old_protected_mbps=0.00 new_protected_mbps=0.00 change_reason= controller_window_seq=32`

func TestParseCapacityBlocked(t *testing.T) {
	event, ok, err := ParseLine(capBlocked)
	if err != nil || !ok {
		t.Fatalf("parse: ok=%v err=%v", ok, err)
	}
	if event.Type != EventCapacityState || event.Capacity == nil {
		t.Fatalf("unexpected event: %+v", event)
	}
	state := event.Capacity
	if state.EventSeq != 7 || state.Event != "CAP_GATE_BLOCKED" || state.Instance != "mp-in-39002" || state.SessionID != "a4bd1539" {
		t.Fatalf("identity mismatch: %+v", state)
	}
	if state.TargetMbps != 700 || state.DeliveryMbps != 640.25 || state.DeliveryReady || state.NormalBoosterAdmitted {
		t.Fatalf("capacity evidence mismatch: %+v", state)
	}
	if state.CurrentBytes != 3145728 || state.ThresholdBytes != 2097152 || state.ControllerWindowSeq != 31 {
		t.Fatalf("trigger/window evidence mismatch: %+v", state)
	}
	if event.ObservedAt.Format(time.RFC3339Nano) != "2026-09-26T01:02:03.123456789Z" {
		t.Fatalf("timestamp mismatch: %s", event.ObservedAt)
	}
	if err := ValidateCapacity(*state); err != nil {
		t.Fatal(err)
	}
}

func TestParseCapacityQuotedFieldsWithSpaces(t *testing.T) {
	line := strings.Replace(capBlocked, `instance="mp-in-39002"`, `instance="mp inbound 39002"`, 1)
	line = strings.Replace(line, `destination="speedtest.example:8080"`, `destination="test destination:8080"`, 1)
	event, ok, err := ParseLine(line)
	if err != nil || !ok {
		t.Fatalf("parse: ok=%v err=%v", ok, err)
	}
	if event.Capacity.Instance != "mp inbound 39002" || event.Capacity.Destination != "test destination:8080" {
		t.Fatalf("quoted parsing failed: %+v", event.Capacity)
	}
}

func TestParseCapacityDropped(t *testing.T) {
	line := `CAP_AUDIT schema_version=1 event=CAP_AUDIT_DROPPED side=server instance="mp-in-39002" dropped_events=17 evidence_complete=false`
	event, ok, err := ParseLine(line)
	if err != nil || !ok {
		t.Fatalf("parse: ok=%v err=%v", ok, err)
	}
	if event.Capacity.DroppedEvents != 17 || event.Capacity.EvidenceComplete {
		t.Fatalf("drop evidence mismatch: %+v", event.Capacity)
	}
	if err := ValidateCapacity(*event.Capacity); err != nil {
		t.Fatal(err)
	}
}

func TestParseMemoryEvidence(t *testing.T) {
	tests := []struct {
		line  string
		kind  MemoryEventKind
		check func(*testing.T, MemoryState)
	}{
		{
			`Go runtime soft memory limit: limit=256 MiB source=configured scope=process (not RSS)`,
			MemoryRuntimeLimit,
			func(t *testing.T, state MemoryState) {
				if state.LimitBytes != 256<<20 || state.Source != "configured" || state.Scope != "process (not RSS)" {
					t.Fatalf("runtime limit mismatch: %+v", state)
				}
			},
		},
		{
			`multipath memory budget: side=server limit=256 MiB source=configured high=224 MiB resume=192 MiB cache_limit=32 MiB`,
			MemoryBudget,
			func(t *testing.T, state MemoryState) {
				if state.Side != "server" || state.LimitBytes != 256<<20 || state.HighBytes != 224<<20 || state.ResumeBytes != 192<<20 || state.CacheLimitBytes != 32<<20 {
					t.Fatalf("budget mismatch: %+v", state)
				}
			},
		},
		{
			`multipath memory pressure entered: side=server used=225 MiB high=224 MiB limit=256 MiB`,
			MemoryPressureEntered,
			func(t *testing.T, state MemoryState) {
				if !state.Pressure || state.UsedBytes != 225<<20 || state.HighBytes != 224<<20 {
					t.Fatalf("entered mismatch: %+v", state)
				}
			},
		},
		{
			`multipath memory pressure cleared: side=server used=190 MiB resume=192 MiB duration=1.25s`,
			MemoryPressureCleared,
			func(t *testing.T, state MemoryState) {
				if state.Pressure || state.UsedBytes != 190<<20 || state.ResumeBytes != 192<<20 || state.Duration != 1250*time.Millisecond {
					t.Fatalf("cleared mismatch: %+v", state)
				}
			},
		},
	}
	for _, test := range tests {
		event, ok, err := ParseLine(test.line)
		if err != nil || !ok || event.Memory == nil {
			t.Fatalf("parse %q: ok=%v err=%v event=%+v", test.line, ok, err, event)
		}
		if event.Type != EventMemoryState || event.Memory.Kind != test.kind {
			t.Fatalf("kind mismatch: %+v", event)
		}
		test.check(t, *event.Memory)
	}
}

func TestParseSessionAndPathEvidence(t *testing.T) {
	tests := []struct {
		line     string
		typeWant EventType
	}{
		{`multipath session established to example.com:443 on leg 0`, EventSessionLifecycle},
		{`multipath leg 1 joined session for example.com:443`, EventPathState},
		{`multipath leg1 joined data path: side=server destination=example.com:443 reconnect=false reason=bytes current_bytes=3145728 threshold_bytes=2097152`, EventPathState},
		{`multipath protocol error for example.com:443: unexpected frame`, EventSessionLifecycle},
	}
	for _, test := range tests {
		event, ok, err := ParseLine(test.line)
		if err != nil || !ok {
			t.Fatalf("parse %q: ok=%v err=%v", test.line, ok, err)
		}
		if event.Type != test.typeWant {
			t.Fatalf("type %q: got %s want %s", test.line, event.Type, test.typeWant)
		}
	}
}

func TestCoverageDoesNotClaimFrozenInternals(t *testing.T) {
	coverage := Coverage()
	if len(coverage) != 6 {
		t.Fatalf("coverage count=%d", len(coverage))
	}
	byType := make(map[EventType]Capability, len(coverage))
	for _, item := range coverage {
		byType[item.Type] = item
	}
	if byType[EventCapacityState].Support != SupportFull || byType[EventCapacityState].RequiresFrozenHook {
		t.Fatalf("capacity coverage mismatch: %+v", byType[EventCapacityState])
	}
	for _, eventType := range []EventType{EventSchedulerDecision, EventAggregateWindow} {
		item := byType[eventType]
		if item.Support != SupportOpen || !item.RequiresFrozenHook {
			t.Fatalf("%s must remain OPEN under frozen-source constraints: %+v", eventType, item)
		}
	}
}

func TestAnalyzerMarksSequenceGapAndDropIncomplete(t *testing.T) {
	analyzer := NewAnalyzer()
	first, _, err := ParseLine(capBlocked)
	if err != nil {
		t.Fatal(err)
	}
	secondLine := strings.Replace(capOpened, "event_seq=8", "event_seq=10", 1)
	second, _, err := ParseLine(secondLine)
	if err != nil {
		t.Fatal(err)
	}
	dropped, _, err := ParseLine(`CAP_AUDIT schema_version=1 event=CAP_AUDIT_DROPPED side=server instance="mp-in-39002" dropped_events=3 evidence_complete=false`)
	if err != nil {
		t.Fatal(err)
	}
	analyzer.Consume(first)
	analyzer.Consume(second)
	analyzer.Consume(dropped)
	report := analyzer.Report()
	if report.EvidenceComplete || report.SequenceGaps != 1 || report.DroppedEvents != 3 {
		t.Fatalf("report mismatch: %+v", report)
	}
	if report.CapacityEvents["CAP_GATE_BLOCKED"] != 1 || report.CapacityEvents["CAP_GATE_OPENED"] != 1 || report.CapacityEvents["CAP_AUDIT_DROPPED"] != 1 {
		t.Fatalf("capacity counts mismatch: %+v", report.CapacityEvents)
	}
}

func TestValidateCapacityRejectsSemanticDrift(t *testing.T) {
	bad := CapacityState{
		Event:                    "CAP_GATE_OPENED",
		OriginalTriggerSatisfied: true,
		DeliveryReady:            true,
		RecoveryBypass:           true,
		NormalBoosterAdmitted:    true,
	}
	if err := ValidateCapacity(bad); err == nil {
		t.Fatal("recovery bypass mislabeled as normal admission was accepted")
	}
	bad = CapacityState{Event: "CAP_AUDIT_DROPPED", DroppedEvents: 1, EvidenceComplete: true}
	if err := ValidateCapacity(bad); err == nil {
		t.Fatal("complete evidence accepted after dropped events")
	}
}

func TestScanRecognizedIgnoredAndConsumerFailure(t *testing.T) {
	input := "noise\n" + capBlocked + "\n" + `multipath session established to example.com:443 on leg 0` + "\n"
	var events []Event
	recognized, ignored, err := Scan(strings.NewReader(input), func(event Event) error {
		events = append(events, event)
		return nil
	})
	if err != nil || recognized != 2 || ignored != 1 || len(events) != 2 {
		t.Fatalf("scan mismatch: recognized=%d ignored=%d events=%d err=%v", recognized, ignored, len(events), err)
	}
	stop := errors.New("stop")
	_, _, err = Scan(strings.NewReader(capBlocked+"\n"), func(Event) error { return stop })
	if !errors.Is(err, stop) {
		t.Fatalf("consumer error not preserved: %v", err)
	}
}

func TestAnalyzerTreatsReadySequenceOneAsNewControllerEpoch(t *testing.T) {
	analyzer := NewAnalyzer()
	first, _, err := ParseLine(capBlocked)
	if err != nil {
		t.Fatal(err)
	}
	readyLine := strings.Replace(capOpened, "event_seq=8 event=CAP_GATE_OPENED", "event_seq=1 event=CAP_AUDIT_READY", 1)
	readyLine = strings.Replace(readyLine, "original_trigger=bytes", "original_trigger=none", 1)
	ready, _, err := ParseLine(readyLine)
	if err != nil {
		t.Fatal(err)
	}
	analyzer.Consume(first)
	analyzer.Consume(ready)
	report := analyzer.Report()
	if !report.EvidenceComplete || report.SequenceGaps != 0 || report.SequenceEpochs != 1 {
		t.Fatalf("controller restart was misclassified as evidence loss: %+v", report)
	}
}

func TestAnalyzerSemanticFailureMarksEvidenceIncomplete(t *testing.T) {
	analyzer := NewAnalyzer()
	event := Event{Type: EventCapacityState, Capacity: &CapacityState{
		Event:                    "CAP_GATE_OPENED",
		OriginalTriggerSatisfied: true,
		DeliveryReady:            true,
		RecoveryBypass:           true,
		NormalBoosterAdmitted:    true,
	}}
	analyzer.Consume(event)
	report := analyzer.Report()
	if report.EvidenceComplete || len(report.ValidationErrors) != 1 {
		t.Fatalf("semantic failure did not fail evidence closed: %+v", report)
	}
}

func TestMalformedRecognizedEvidenceFailsClosed(t *testing.T) {
	_, ok, err := ParseLine(`CAP_AUDIT schema_version=x event=CAP_GATE_BLOCKED`)
	if !ok || err == nil {
		t.Fatalf("recognized malformed evidence must fail: ok=%v err=%v", ok, err)
	}
	_, ok, err = ParseLine(`multipath memory pressure entered: side=server used=garbage high=224 MiB limit=256 MiB`)
	if !ok || err == nil {
		t.Fatalf("malformed memory evidence must fail: ok=%v err=%v", ok, err)
	}
}
