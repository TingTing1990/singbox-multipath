package audit

import (
	"encoding/json"
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
