package audit

import (
	"errors"
	"testing"
	"time"
)

func TestBuildTimelineCumulativeDeltaAndUDPRemoval(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	first := syntheticSnapshot(start, start.Add(time.Second), 100, [2]uint64{60, 50}, [2]uint64{20, 10})
	second := syntheticSnapshot(start, start.Add(2*time.Second), 100+bytesForMbps(500, time.Second), [2]uint64{60 + bytesForMbps(350, time.Second), 50 + bytesForMbps(180, time.Second)}, [2]uint64{20 + bytesForMbps(50, time.Second), 10 + bytesForMbps(20, time.Second)})
	timeline, err := BuildTimeline([]StatusSnapshot{first, second})
	if err != nil {
		t.Fatal(err)
	}
	window := timeline.Windows[0]
	if window.UsefulMbps != 500 || window.LegMbps != [2]float64{350, 180} || window.PhysicalMbps != 530 {
		t.Fatalf("unexpected window: %+v", window)
	}
}

func TestBuildTimelineSkippedSnapshotPreservesBytes(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	trace := traceFromRates(start, []int{500}, []int{300}, []int{220}, 2*time.Second)
	timeline, err := BuildTimeline(trace)
	if err != nil {
		t.Fatal(err)
	}
	if !timeline.TemporalResolutionDegraded || len(timeline.Windows) != 1 {
		t.Fatalf("gap not detected: %+v", timeline)
	}
	if timeline.Windows[0].UsefulMbps != 500 || timeline.Windows[0].UsefulRXBytes != bytesForMbps(500, 2*time.Second) {
		t.Fatalf("bytes lost across gap: %+v", timeline.Windows[0])
	}
}

func TestBuildTimelineRestartStartsNewEpoch(t *testing.T) {
	startA := time.Unix(1000, 0).UTC()
	startB := time.Unix(2000, 0).UTC()
	trace := []StatusSnapshot{
		syntheticSnapshot(startA, startA.Add(time.Second), 900, [2]uint64{900, 0}, [2]uint64{}),
		syntheticSnapshot(startA, startA.Add(2*time.Second), 1000, [2]uint64{1000, 0}, [2]uint64{}),
		syntheticSnapshot(startB, startB.Add(time.Second), 10, [2]uint64{10, 0}, [2]uint64{}),
		syntheticSnapshot(startB, startB.Add(2*time.Second), 20, [2]uint64{20, 0}, [2]uint64{}),
	}
	timeline, err := BuildTimeline(trace)
	if err != nil {
		t.Fatal(err)
	}
	if timeline.Epochs != 2 || len(timeline.Windows) != 2 {
		t.Fatalf("restart mishandled: %+v", timeline)
	}
}

func TestBuildTimelineRejectsCounterRegression(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	trace := []StatusSnapshot{
		syntheticSnapshot(start, start.Add(time.Second), 100, [2]uint64{100, 0}, [2]uint64{}),
		syntheticSnapshot(start, start.Add(2*time.Second), 90, [2]uint64{90, 0}, [2]uint64{}),
	}
	_, err := BuildTimeline(trace)
	if !errors.Is(err, ErrCounterRegression) {
		t.Fatalf("counter regression accepted: %v", err)
	}
}
