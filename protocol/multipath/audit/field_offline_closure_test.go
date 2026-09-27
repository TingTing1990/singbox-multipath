package audit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestOfflineClosureStatusParserPreservesFrozenProducerFields(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	snapshot := syntheticSnapshot(start, start.Add(time.Second), 1000, [2]uint64{600, 500}, [2]uint64{})
	snapshot.Node.Aggregation = "127.0.0.1:39002"
	snapshot.Node.UDPOutbound = "leg0"
	snapshot.Node.TCPFastOpen = true
	snapshot.Node.Parameters = ParametersStatus{
		PreferredCapacityMbps:       640,
		AggregationEnabled:          true,
		ActivationOnQueue:           true,
		ActivationThresholdMbps:     80,
		ActivationAfterBytes:        2 << 20,
		ActivationAfterBytesMinMbps: 50,
		ActivationWindowMS:          1000,
		ChunkSize:                   32768,
		QueueFrames:                 32,
		QueueBytes:                  4 << 20,
		MaxReorderFrames:            64,
		MaxReorderBytes:             8 << 20,
		Leg1ReplayBytes:             2 << 20,
		Leg1ReplayTimeoutMS:         5000,
		MemoryLimitBytes:            128 << 20,
		HandshakeTimeoutMS:          10000,
	}
	snapshot.Node.Memory = MemoryStatus{LimitBytes: 128 << 20, UsedBytes: 16 << 20, CachedBytes: 4 << 20, BoosterLimitBytes: 96 << 20, BoosterResumeBytes: 80 << 20, Automatic: true, Pressure: true, PressureSince: start.Format(time.RFC3339Nano), PressureEvents: 3, BackpressureEvents: 2, PeakUsedBytes: 32 << 20, PeakCachedBytes: 8 << 20}
	snapshot.Node.Logical.ConnectionsTotal = 11
	snapshot.Node.Logical.PreferredOnlyConnections = 2
	snapshot.Node.Logical.TXAggregatingConnections = 3
	snapshot.Node.Logical.RXAggregatingConnections = 4
	snapshot.Node.Logical.BoosterDegraded = 1
	snapshot.Node.Logical.ReorderPeakBytes = 12345
	snapshot.Node.Logical.ReorderPeakPages = 7
	snapshot.Node.Logical.LastActivation = &ActivationStatus{Reason: "bytes", At: start.Format(time.RFC3339Nano), CurrentBytes: 3 << 20, ThresholdBytes: 2 << 20}
	snapshot.Node.Logical.RemoteSender = SenderDiagnostics{Available: true, Leg1TXBytes: 999, ReplayPeakBytes: 444, FallbackEvents: 2, BackpressureDurationMS: 55, MemoryPeakUsedBytes: 888, MemoryPressureEvents: 3}
	snapshot.Node.Recovery = &RecoveryStatus{Enabled: true, FailoverTimeoutMS: 2000, FailbackDelayMS: 5000, TCPPath: 1, UDPPath: 0, UDPPreferred: 0, UsableMask: 3, Paths: [2]RecoveryPathStatus{{Healthy: true, TCPAgeMS: 10}, {Healthy: true, TCPAgeMS: 20}}}
	snapshot.Node.Legs[1].Type = "hysteria2"
	snapshot.Node.Legs[1].CarryingConnections = 3
	snapshot.Node.Legs[1].StandbyConnections = 2
	snapshot.Node.Legs[1].PeakBacklogBytes = 777
	snapshot.Node.Legs[1].RemotePeakBacklogBytes = 888
	snapshot.Node.Legs[1].QueueBytesPerConnection = 65536
	snapshot.Node.Legs[1].JoinCount = 9
	snapshot.Node.Legs[1].AttemptCount = 10
	snapshot.Node.Legs[1].ErrorCount = 4
	snapshot.Node.Legs[1].RemoteFailureCount = 5
	snapshot.Node.Legs[1].ProbeSent = 6
	snapshot.Node.Legs[1].TopFlows = []FlowStatus{{SessionID: "abcdef12", Destination: "example.invalid:443", State: "carrying", BacklogBytes: 123}}

	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseStatus(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Node.Parameters.PreferredCapacityMbps != 640 || parsed.Node.Aggregation != "127.0.0.1:39002" || !parsed.Node.TCPFastOpen {
		t.Fatalf("node parameters lost: %+v", parsed.Node)
	}
	if parsed.Node.Logical.ConnectionsTotal != 11 || parsed.Node.Logical.LastActivation == nil || parsed.Node.Logical.LastActivation.CurrentBytes != 3<<20 {
		t.Fatalf("logical producer fields lost: %+v", parsed.Node.Logical)
	}
	if parsed.Node.Legs[1].JoinCount != 9 || parsed.Node.Legs[1].TopFlows[0].SessionID != "abcdef12" || parsed.Node.Legs[1].RemotePeakBacklogBytes != 888 {
		t.Fatalf("leg producer fields lost: %+v", parsed.Node.Legs[1])
	}
	if parsed.Node.Recovery == nil || parsed.Node.Recovery.TCPPath != 1 || len(parsed.Raw) == 0 {
		t.Fatalf("recovery/raw evidence lost: recovery=%+v raw=%d", parsed.Node.Recovery, len(parsed.Raw))
	}
}

func TestOfflineClosureTimelinePreservesBothDirectionsAndSnapshots(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	first := syntheticSnapshot(start, start.Add(time.Second), 0, [2]uint64{}, [2]uint64{})
	second := syntheticSnapshot(start, start.Add(2*time.Second), bytesForMbps(500, time.Second), [2]uint64{bytesForMbps(300, time.Second), bytesForMbps(220, time.Second)}, [2]uint64{})
	first.Node.Logical.Cumulative.TXBytes = 100
	second.Node.Logical.Cumulative.TXBytes = 100 + bytesForMbps(125, time.Second)
	first.Node.Legs[0].Cumulative.TXBytes = 40
	second.Node.Legs[0].Cumulative.TXBytes = 40 + bytesForMbps(75, time.Second)
	first.Node.Legs[1].Cumulative.TXBytes = 60
	second.Node.Legs[1].Cumulative.TXBytes = 60 + bytesForMbps(50, time.Second)
	second.Node.Parameters.PreferredCapacityMbps = 640
	second.Node.Logical.Connections = 1

	timeline, err := BuildTimeline([]StatusSnapshot{first, second})
	if err != nil {
		t.Fatal(err)
	}
	if len(timeline.Snapshots) != 2 || len(timeline.Windows) != 1 {
		t.Fatalf("source snapshots not preserved: snapshots=%d windows=%d", len(timeline.Snapshots), len(timeline.Windows))
	}
	window := timeline.Windows[0]
	if window.UsefulTXMbps != 125 || window.LegTXMbps != [2]float64{75, 50} || window.UsefulMbps != 500 {
		t.Fatalf("bidirectional TCP replay wrong: %+v", window)
	}
	if window.Parameters.PreferredCapacityMbps != 640 {
		t.Fatalf("window parameters lost: %+v", window.Parameters)
	}
}

func capLine(instance string, seq uint64, at time.Time, assignment, delivery float64, ready bool, protected float64, degrade int, backlog, queue int64) string {
	return fmt.Sprintf("+0000 %s INFO inbound/multipath[%s]: CAP_AUDIT schema_version=1 event_seq=%d event=CAP_WINDOW side=server instance=\"%s\" session_id=\"\" destination=\"\" at=%s original_trigger=none original_trigger_satisfied=false recovery_bypass=false target_mbps=640.00 delivery_mbps=%.2f delivery_ready=%t protected_mbps=%.2f protection_valid=true protection_active=true preferred_assignment_mbps=%.2f degrade_windows=%d normal_booster_admitted=true current_bytes=0 threshold_bytes=0 trigger_window_bytes=0 trigger_rate_mbps=0.00 trigger_threshold_mbps=0.00 backlog_bytes=%d queue_bytes=%d old_protected_mbps=0.00 new_protected_mbps=0.00 change_reason= controller_window_seq=%d\n", at.Format("2006-01-02 15:04:05"), instance, seq, instance, at.Format(time.RFC3339Nano), delivery, ready, protected, assignment, degrade, backlog, queue, seq)
}

func TestOfflineClosureExplicitInstanceBindingPreservesExactCAPState(t *testing.T) {
	start := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	trace := traceFromRates(start, []int{500, 450}, []int{300, 260}, []int{220, 210}, time.Second)
	for i := range trace {
		trace[i].Node.Tag = "mp-out-field"
	}
	var status bytes.Buffer
	for _, snapshot := range trace {
		encoded, err := json.Marshal(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		status.Write(encoded)
		status.WriteByte('\n')
	}
	var journal strings.Builder
	journal.WriteString(capLine("other-instance", 1, start.Add(2*time.Second), 999, 999, true, 999, 0, 0, 0))
	// Seed the previous completed CAP_WINDOW endpoint. The first observed CAP rate
	// has an unknown start and must not be relabeled as a full client interval.
	journal.WriteString(capLine("mp-in-field", 1, start.Add(time.Second), 400, 390, false, 390, 1, 100, 200))
	journal.WriteString(capLine("mp-in-field", 2, start.Add(2*time.Second), 420, 410, false, 400, 2, 1234, 5678))
	journal.WriteString("+0000 2026-09-27 12:00:02 INFO inbound/multipath[mp-in-field]: multipath leg1 joined data path: side=server destination=example.invalid:443 reconnect=false reason=bytes current_bytes=3145728 threshold_bytes=2097152\n")
	journal.WriteString(capLine("other-instance", 2, start.Add(3*time.Second), 998, 998, true, 998, 0, 0, 0))
	journal.WriteString(capLine("mp-in-field", 3, start.Add(3*time.Second), 430, 420, true, 415, 0, 10, 20))

	replay, err := ReplayFieldRun(&status, strings.NewReader(journal.String()), FieldRunBinding{ClientNodeTag: "mp-out-field", ServerInstance: "mp-in-field"})
	if err != nil {
		t.Fatal(err)
	}
	if replay.Journal.ServerCapacityWindows != 3 || replay.Journal.SequenceGaps != 0 || len(replay.Journal.CapacityEvents) != 3 {
		t.Fatalf("mixed journal was not isolated by instance: %+v", replay.Journal)
	}
	if len(replay.Timeline.Windows) != 2 {
		t.Fatalf("unexpected timeline windows: %d", len(replay.Timeline.Windows))
	}
	first := replay.Timeline.Windows[0]
	if first.ServerTargetMbps != 640 || first.ServerDeliveryReady || first.ServerProtectedMbps != 400 || first.ServerDegradeWindows != 2 || first.ServerBacklogBytes != 1234 || first.ServerQueueBytes != 5678 {
		t.Fatalf("CAP state lost from replay: %+v", first)
	}
	if first.ServerPreferredAssignedMbps != 420 || first.ServerPreferredDeliveryMbps != 410 || first.ServerCapacityCoverageRatio != 1 || len(first.ServerCapacityEvidence) != 1 || len(first.ServerCAPEvents) != 1 {
		t.Fatalf("CAP evidence not retained exactly: %+v", first)
	}
	if len(first.RuntimeEvents) == 0 || first.RuntimeEvents[0].ObservedAt.IsZero() {
		t.Fatalf("timestamped runtime event not correlated: %+v", first.RuntimeEvents)
	}
}

func TestOfflineClosureStatusCaptureDeduplicatesAndRejectsIdentityMutation(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	snapshot := syntheticSnapshot(start, start.Add(time.Second), 10, [2]uint64{6, 5}, [2]uint64{})
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var state statusCaptureState
	if _, accepted, err := state.accept(encoded); err != nil || !accepted {
		t.Fatalf("first sample not accepted: accepted=%v err=%v", accepted, err)
	}
	if _, accepted, err := state.accept(encoded); err != nil || accepted {
		t.Fatalf("duplicate sample not deduplicated: accepted=%v err=%v", accepted, err)
	}
	var object map[string]any
	if err := json.Unmarshal(encoded, &object); err != nil {
		t.Fatal(err)
	}
	node := object["node"].(map[string]any)
	node["udp_outbound"] = "mutated"
	mutated, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := state.accept(mutated); !errors.Is(err, ErrInvalidStatus) {
		t.Fatalf("same snapshot identity with changed content accepted: %v", err)
	}
}

func TestOfflineClosureReplayRejectsNonOverlappingCAPEvidence(t *testing.T) {
	start := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	trace := traceFromRates(start, []int{500, 450}, []int{300, 260}, []int{220, 210}, time.Second)
	for i := range trace {
		trace[i].Node.Tag = "mp-out-field"
	}
	var status bytes.Buffer
	for _, snapshot := range trace {
		encoded, err := json.Marshal(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		status.Write(encoded)
		status.WriteByte('\n')
	}
	journal := capLine("mp-in-field", 1, start.Add(30*time.Second), 420, 410, true, 400, 0, 0, 0)
	_, err := ReplayFieldRun(&status, strings.NewReader(journal), FieldRunBinding{ClientNodeTag: "mp-out-field", ServerInstance: "mp-in-field"})
	if err == nil || !strings.Contains(err.Error(), "does not overlap") {
		t.Fatalf("non-overlapping status/CAP evidence was accepted: %v", err)
	}
}

func TestOfflineClosureReportSerializesForensicEvidence(t *testing.T) {
	start := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	trace := traceFromRates(start, []int{500, 450}, []int{300, 260}, []int{220, 210}, time.Second)
	for i := range trace {
		trace[i].Node.Tag = "mp-out-field"
	}
	var status bytes.Buffer
	for _, snapshot := range trace {
		encoded, err := json.Marshal(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		status.Write(encoded)
		status.WriteByte('\n')
	}
	journal := capLine("mp-in-field", 1, start.Add(2*time.Second), 420, 410, false, 400, 2, 1234, 5678) +
		capLine("mp-in-field", 2, start.Add(3*time.Second), 430, 420, true, 415, 0, 10, 20)
	replay, err := ReplayFieldRun(&status, strings.NewReader(journal), FieldRunBinding{ClientNodeTag: "mp-out-field", ServerInstance: "mp-in-field"})
	if err != nil {
		t.Fatal(err)
	}
	var report bytes.Buffer
	if err := WriteFieldRunReplayJSON(&report, replay); err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(report.Bytes(), &decoded); err != nil {
		t.Fatalf("report is not JSON: %v\n%s", err, report.String())
	}
	for _, key := range []string{"Binding", "Timeline", "Journal", "Run"} {
		if _, ok := decoded[key]; !ok {
			t.Fatalf("report lost %s: %s", key, report.String())
		}
	}
	if !strings.Contains(report.String(), "ServerCapacityEvidence") || !strings.Contains(report.String(), "ServerDeliveryReady") || !strings.Contains(report.String(), "Parameters") {
		t.Fatalf("report lost correlated forensic state: %s", report.String())
	}
}

func TestOfflineClosureParseStatusRejectsMissingFrozenRequiredField(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	snapshot := syntheticSnapshot(start, start.Add(time.Second), 10, [2]uint64{6, 5}, [2]uint64{})
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]any
	if err := json.Unmarshal(encoded, &object); err != nil {
		t.Fatal(err)
	}
	node := object["node"].(map[string]any)
	parameters := node["parameters"].(map[string]any)
	delete(parameters, "activation_window_ms")
	broken, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseStatus(broken); !errors.Is(err, ErrInvalidStatus) {
		t.Fatalf("missing frozen producer field was accepted: %v", err)
	}
}

func TestOfflineClosureCaptureStatusFileWritesInitialSnapshot(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	snapshot := syntheticSnapshot(start, start.Add(time.Second), 10, [2]uint64{6, 5}, [2]uint64{})
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/status.json"
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var output bytes.Buffer
	count, err := CaptureStatusFile(ctx, path, &output, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("capture count=%d want=1", count)
	}
	lines, err := ReadStatusTrace(bytes.NewReader(output.Bytes()))
	if err != nil || len(lines) != 1 {
		t.Fatalf("captured trace invalid: snapshots=%d err=%v output=%q", len(lines), err, output.String())
	}
}

func TestOfflineClosureLatestCAPEventPreservesInWindowStateChange(t *testing.T) {
	start := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	trace := traceFromRates(start, []int{500, 450}, []int{300, 260}, []int{220, 210}, time.Second)
	for i := range trace {
		trace[i].Node.Tag = "mp-out-field"
	}
	var status bytes.Buffer
	for _, snapshot := range trace {
		encoded, err := json.Marshal(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		status.Write(encoded)
		status.WriteByte('\n')
	}
	seedLine := capLine("mp-in-field", 1, start.Add(1200*time.Millisecond), 400, 390, false, 390, 1, 100, 200)
	windowLine := capLine("mp-in-field", 2, start.Add(2200*time.Millisecond), 420, 410, false, 400, 2, 1234, 5678)
	changedLine := capLine("mp-in-field", 3, start.Add(2800*time.Millisecond), 430, 420, true, 500, 0, 10, 20)
	changedLine = strings.Replace(changedLine, "event=CAP_WINDOW", "event=CAP_PROTECTION_CHANGED", 1)
	replay, err := ReplayFieldRun(&status, strings.NewReader(seedLine+windowLine+changedLine), FieldRunBinding{ClientNodeTag: "mp-out-field", ServerInstance: "mp-in-field"})
	if err != nil {
		t.Fatal(err)
	}
	window := replay.Timeline.Windows[1]
	if len(window.ServerCapacityEvidence) != 1 || len(window.ServerCAPEvents) != 2 || window.ServerCAPEventCount != 2 {
		t.Fatalf("CAP evidence/event split lost: %+v", window)
	}
	if window.ServerLatestCAPEvent == nil || window.ServerLatestCAPEvent.Event != "CAP_PROTECTION_CHANGED" {
		t.Fatalf("latest CAP event not retained: %+v", window.ServerLatestCAPEvent)
	}
	if window.ServerProtectedMbps != 500 || !window.ServerDeliveryReady || window.ServerBacklogBytes != 10 || window.ServerQueueBytes != 20 {
		t.Fatalf("latest in-window CAP state not reflected: %+v", window)
	}
	if window.ServerPreferredAssignedMbps != 420 || window.ServerPreferredDeliveryMbps != 410 {
		t.Fatalf("CAP_WINDOW assignment/delivery metrics were overwritten by non-window event: %+v", window)
	}
}

func TestOfflineClosureCAPIntervalRequiresConsecutiveSequence(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	raw := capLine("mp-in-field", 1, start.Add(time.Second), 100, 100, false, 100, 0, 0, 0) +
		capLine("mp-in-field", 3, start.Add(2*time.Second), 200, 200, false, 200, 0, 0, 0) +
		capLine("mp-in-field", 4, start.Add(3*time.Second), 300, 300, true, 300, 0, 0, 0)
	journal, err := AnalyzeJournal(strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if len(journal.CapacityEvidence) != 3 {
		t.Fatalf("capacity evidence=%d want=3", len(journal.CapacityEvidence))
	}
	if journal.CapacityEvidence[0].IntervalKnown || journal.CapacityEvidence[1].IntervalKnown {
		t.Fatalf("first/gapped CAP endpoints became intervals: %+v", journal.CapacityEvidence)
	}
	last := journal.CapacityEvidence[2]
	if !last.IntervalKnown || !last.IntervalStart.Equal(start.Add(2*time.Second)) || !last.IntervalEnd.Equal(start.Add(3*time.Second)) {
		t.Fatalf("post-gap consecutive CAP interval not recovered exactly: %+v", last)
	}
}

func TestOfflineClosureCAPIntervalDoesNotBridgeTimeRollback(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	raw := capLine("mp-in-field", 1, start.Add(2*time.Second), 100, 100, false, 100, 0, 0, 0) +
		capLine("mp-in-field", 2, start.Add(1500*time.Millisecond), 200, 200, false, 200, 0, 0, 0) +
		capLine("mp-in-field", 3, start.Add(2500*time.Millisecond), 300, 300, false, 300, 0, 0, 0) +
		capLine("mp-in-field", 4, start.Add(3500*time.Millisecond), 400, 400, true, 400, 0, 0, 0)
	journal, err := AnalyzeJournal(strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if len(journal.CapacityEvidence) != 4 {
		t.Fatalf("capacity evidence=%d want=4", len(journal.CapacityEvidence))
	}
	if journal.CapacityEvidence[1].IntervalKnown || journal.CapacityEvidence[2].IntervalKnown {
		t.Fatalf("timestamp rollback was used as a temporal anchor: %+v", journal.CapacityEvidence)
	}
	last := journal.CapacityEvidence[3]
	if !last.IntervalKnown || !last.IntervalStart.Equal(start.Add(2500*time.Millisecond)) || !last.IntervalEnd.Equal(start.Add(3500*time.Millisecond)) {
		t.Fatalf("timeline did not recover after a fresh monotonic anchor: %+v", last)
	}
}

func TestOfflineClosureRebindingClearsPreviousInstanceEvidence(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	timeline := mustTimeline(t, traceFromRates(start, []int{500}, []int{270}, []int{250}, time.Second))
	raw := capLine("mp-A", 1, start.Add(time.Second), 250, 250, false, 250, 0, 0, 0) +
		capLine("mp-A", 2, start.Add(2*time.Second), 300, 300, true, 300, 0, 0, 0)
	journal, err := AnalyzeJournal(strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if err := AttachServerCapacityEvidenceForInstance(&timeline, journal, "mp-A"); err != nil {
		t.Fatal(err)
	}
	if !timeline.Windows[0].ServerPreferredAssignmentEvidence || timeline.Windows[0].ServerCapacityInstance != "mp-A" {
		t.Fatalf("initial binding did not attach mp-A: %+v", timeline.Windows[0])
	}
	if err := AttachServerCapacityEvidenceForInstance(&timeline, JournalReport{}, "mp-B"); err != nil {
		t.Fatal(err)
	}
	window := timeline.Windows[0]
	if window.ServerPreferredAssignmentEvidence || window.ServerPreferredDeliveryEvidence || window.ServerCapacityInstance != "" ||
		len(window.ServerCapacityEvidence) != 0 || len(window.ServerCAPEvents) != 0 || window.ServerLatestCAPEvent != nil || len(window.RuntimeEvents) != 0 ||
		window.ServerCapacityCoverageDuration != 0 || window.ServerCapacityCoverageRatio != 0 {
		t.Fatalf("rebinding to mp-B retained mp-A evidence: %+v", window)
	}
}

func TestOfflineClosureCAPIntervalUsesDurationWeightedOverlap(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	timeline := mustTimeline(t, traceFromRates(start, []int{500}, []int{270}, []int{250}, time.Second))
	raw := capLine("mp-in-field", 1, start.Add(500*time.Millisecond), 50, 50, false, 50, 0, 0, 0) +
		capLine("mp-in-field", 2, start.Add(1250*time.Millisecond), 100, 100, false, 100, 0, 0, 0) +
		capLine("mp-in-field", 3, start.Add(2500*time.Millisecond), 300, 300, true, 300, 0, 0, 0)
	journal, err := AnalyzeJournal(strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if err := AttachServerCapacityEvidenceForInstance(&timeline, journal, "mp-in-field"); err != nil {
		t.Fatal(err)
	}
	window := timeline.Windows[0]
	// Client interval is [1s,2s]. CAP interval ending at 1.25s contributes
	// 0.25s at 100 Mbps; the next contributes 0.75s at 300 Mbps.
	if window.ServerCapacityCoverageDuration != time.Second || window.ServerCapacityCoverageRatio != 1 {
		t.Fatalf("CAP temporal coverage wrong: %+v", window)
	}
	if window.ServerPreferredAssignedMbps != 250 || window.ServerPreferredDeliveryMbps != 250 {
		t.Fatalf("CAP rates were event-count averaged instead of duration weighted: %+v", window)
	}
}

func TestOfflineClosureCaptureToReplayUsesProvableCAPInterval(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	trace := traceFromRates(start, []int{1000}, []int{600}, []int{400}, time.Second)
	for i := range trace {
		trace[i].Node.Tag = "mp-out"
	}
	path := t.TempDir() + "/status.json"
	var captured bytes.Buffer
	for _, snapshot := range trace {
		encoded, err := json.Marshal(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, encoded, 0o600); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		count, err := CaptureStatusFile(ctx, path, &captured, time.Millisecond)
		if err != nil || count != 1 {
			t.Fatalf("capture count=%d err=%v", count, err)
		}
	}
	journal := capLine("mp-in", 1, start.Add(time.Second), 550, 550, false, 600, 0, 0, 0) +
		capLine("mp-in", 2, start.Add(2*time.Second), 600, 600, true, 640, 0, 0, 0)
	replay, err := ReplayFieldRun(&captured, strings.NewReader(journal), FieldRunBinding{ClientNodeTag: "mp-out", ServerInstance: "mp-in"})
	if err != nil {
		t.Fatal(err)
	}
	if replay.Run.Useful.MeanMbps != 1000 || len(replay.Timeline.Windows) != 1 {
		t.Fatalf("unexpected replay metrics: %+v", replay.Run)
	}
	window := replay.Timeline.Windows[0]
	if window.ServerCapacityCoverageRatio != 1 || window.ServerPreferredAssignedMbps != 600 {
		t.Fatalf("producer capture -> CAP interval replay did not close: %+v", window)
	}
	var output bytes.Buffer
	if err := WriteFieldRunReplayJSON(&output, replay); err != nil || !json.Valid(output.Bytes()) {
		t.Fatalf("invalid replay JSON: err=%v output=%q", err, output.String())
	}
}
