package audit

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func bytesForMbps(mbps int, duration time.Duration) uint64 {
	return uint64(float64(mbps) * 1_000_000 / 8 * duration.Seconds())
}

func syntheticSnapshot(start, at time.Time, logicalTCP uint64, legTCP [2]uint64, udp [2]uint64) StatusSnapshot {
	return StatusSnapshot{
		SchemaVersion:     StatusSchemaVersion,
		GeneratedAtRaw:    at.Format(time.RFC3339Nano),
		ProcessStartedRaw: start.Format(time.RFC3339Nano),
		GeneratedAt:       at,
		ProcessStartedAt:  start,
		Node: StatusNode{
			Tag:    "mp-out",
			Type:   "multipath",
			Memory: MemoryStatus{LimitBytes: 256 << 20},
			Logical: LogicalStatus{
				Cumulative: Traffic{RXBytes: logicalTCP + udp[0] + udp[1]},
			},
			Legs: []LegStatus{
				{ID: 0, Role: "preferred", Cumulative: Traffic{RXBytes: legTCP[0] + udp[0]}, UDPCumulative: Traffic{RXBytes: udp[0]}},
				{ID: 1, Role: "booster", Cumulative: Traffic{RXBytes: legTCP[1] + udp[1]}, UDPCumulative: Traffic{RXBytes: udp[1]}},
			},
		},
	}
}

func traceFromRates(start time.Time, useful, leg0, leg1 []int, step time.Duration) []StatusSnapshot {
	if len(useful) != len(leg0) || len(useful) != len(leg1) {
		panic("rate length mismatch")
	}
	var logical uint64
	var legs [2]uint64
	out := []StatusSnapshot{syntheticSnapshot(start, start.Add(time.Second), 0, [2]uint64{}, [2]uint64{})}
	at := start.Add(time.Second)
	for index := range useful {
		logical += bytesForMbps(useful[index], step)
		legs[0] += bytesForMbps(leg0[index], step)
		legs[1] += bytesForMbps(leg1[index], step)
		at = at.Add(step)
		out = append(out, syntheticSnapshot(start, at, logical, legs, [2]uint64{}))
	}
	return out
}

func mustTimeline(t *testing.T, snapshots []StatusSnapshot) Timeline {
	t.Helper()
	timeline, err := BuildTimeline(snapshots)
	if err != nil {
		t.Fatal(err)
	}
	return timeline
}

func mustRun(t *testing.T, snapshots []StatusSnapshot) RunMetrics {
	t.Helper()
	timeline := mustTimeline(t, snapshots)
	run, err := AnalyzeRun(timeline.Windows)
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func marshalSnapshot(t *testing.T, snapshot StatusSnapshot) []byte {
	t.Helper()
	content, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return content
}

func syntheticServerCAPJournal(t *testing.T, start time.Time, assignments []int, present []bool) JournalReport {
	t.Helper()
	if present != nil && len(present) != len(assignments) {
		t.Fatal("capacity presence length mismatch")
	}
	var builder strings.Builder
	var eventSeq uint64
	for index, assignment := range assignments {
		if present != nil && !present[index] {
			continue
		}
		eventSeq++
		at := start.Add(time.Duration(index+2) * time.Second).Format(time.RFC3339Nano)
		fmt.Fprintf(&builder,
			"CAP_AUDIT schema_version=1 event_seq=%d event=CAP_WINDOW side=server instance=\"mp-in-test\" session_id=\"\" destination=\"\" at=%s original_trigger=none original_trigger_satisfied=false recovery_bypass=false target_mbps=640.00 delivery_mbps=%.2f delivery_ready=true protected_mbps=%.2f protection_valid=true protection_active=true preferred_assignment_mbps=%.2f degrade_windows=0 normal_booster_admitted=true current_bytes=0 threshold_bytes=0 trigger_window_bytes=0 trigger_rate_mbps=0.00 trigger_threshold_mbps=0.00 backlog_bytes=0 queue_bytes=0 old_protected_mbps=0.00 new_protected_mbps=0.00 change_reason= controller_window_seq=%d\n",
			eventSeq, at, float64(assignment), float64(assignment), float64(assignment), index+1)
	}
	journal, err := AnalyzeJournal(strings.NewReader(builder.String()))
	if err != nil {
		t.Fatal(err)
	}
	return journal
}
