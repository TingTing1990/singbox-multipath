package audit

import (
	"errors"
	"slices"
	"testing"
	"time"
)

func baselineRuns(t *testing.T) (RunMetrics, RunMetrics) {
	t.Helper()
	start := time.Unix(1000, 0).UTC()
	b0 := mustRun(t, traceFromRates(start, []int{700, 700, 700}, []int{700, 700, 700}, []int{0, 0, 0}, time.Second))
	b1 := mustRun(t, traceFromRates(start, []int{650, 650, 650}, []int{0, 0, 0}, []int{650, 650, 650}, time.Second))
	return b0, b1
}

func hasFinding(findings []Finding, code FindingCode) bool {
	return slices.ContainsFunc(findings, func(f Finding) bool { return f.Code == code })
}

func TestS01StablePositiveAggregation(t *testing.T) {
	b0, b1 := baselineRuns(t)
	start := time.Unix(1000, 0).UTC()
	agg := mustRun(t, traceFromRates(start, []int{1170, 1170, 1170}, []int{600, 600, 600}, []int{590, 590, 590}, time.Second))
	comparison, err := CompareToBaselines(agg, b0, b1, Topology{})
	if err != nil {
		t.Fatal(err)
	}
	findings, _, err := Diagnose(agg, comparison, LoadSaturating)
	if err != nil {
		t.Fatal(err)
	}
	if comparison.GainVsBestSingle <= 0 || comparison.EfficiencyVsReference <= 0.8 || !hasFinding(findings, FindingBothPathsCarrying) {
		t.Fatalf("stable aggregation misclassified: %+v %+v", comparison, findings)
	}
}

func TestS02PeakCollapseNotReportedAsSustainedPeak(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	run := mustRun(t, traceFromRates(start, []int{800, 760, 510, 500, 505}, []int{450, 430, 280, 275, 278}, []int{360, 340, 240, 235, 237}, time.Second))
	if run.Useful.PeakMbps != 800 || run.Useful.MedianMbps != 510 || run.Useful.MeanMbps >= 700 {
		t.Fatalf("peak collapsed into sustained metric: %+v", run.Useful)
	}
}

func TestS03NegativeAggregationGain(t *testing.T) {
	b0, b1 := baselineRuns(t)
	start := time.Unix(1000, 0).UTC()
	agg := mustRun(t, traceFromRates(start, []int{500, 500, 500}, []int{270, 270, 270}, []int{250, 250, 250}, time.Second))
	comparison, _ := CompareToBaselines(agg, b0, b1, Topology{})
	findings, _, _ := Diagnose(agg, comparison, LoadSaturating)
	if comparison.GainVsBestSingle >= 0 || comparison.TimeBelowBestSingleRatio != 1 || !hasFinding(findings, FindingNegativeAggregationGain) {
		t.Fatalf("negative gain missed: %+v %+v", comparison, findings)
	}
}

func TestS04BothPathsBelowStandaloneDoesNotClaimSchedulerBug(t *testing.T) {
	b0, b1 := baselineRuns(t)
	start := time.Unix(1000, 0).UTC()
	agg := mustRun(t, traceFromRates(start, []int{500, 500}, []int{270, 270}, []int{250, 250}, time.Second))
	comparison, _ := CompareToBaselines(agg, b0, b1, Topology{})
	findings, open, _ := Diagnose(agg, comparison, LoadSaturating)
	if !hasFinding(findings, FindingBothPathRatesBelowStandalone) {
		t.Fatal("both-path observation missing")
	}
	if !slices.ContainsFunc(open, func(item OpenQuestion) bool { return item.Code == "LEG1_EXACT_ASSIGNMENT" }) {
		t.Fatal("leg1 assignment was not kept OPEN")
	}
}

func TestS05PhysicalIsNotUsefulAggregate(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	run := mustRun(t, traceFromRates(start, []int{510, 510}, []int{320, 320}, []int{280, 280}, time.Second))
	if run.PhysicalMeanMbps != 600 || run.Useful.MeanMbps != 510 || run.PathLogicalGapMeanMbps != 90 {
		t.Fatalf("physical data misused as useful: %+v", run)
	}
}

func TestS06UDPTrafficIsExcludedFromTCPAggregation(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	first := syntheticSnapshot(start, start.Add(time.Second), 0, [2]uint64{}, [2]uint64{})
	second := syntheticSnapshot(start, start.Add(2*time.Second), bytesForMbps(500, time.Second), [2]uint64{bytesForMbps(350, time.Second), bytesForMbps(180, time.Second)}, [2]uint64{bytesForMbps(50, time.Second), bytesForMbps(20, time.Second)})
	window := mustTimeline(t, []StatusSnapshot{first, second}).Windows[0]
	if window.UsefulMbps != 500 || window.LegMbps != [2]float64{350, 180} {
		t.Fatalf("udp polluted tcp metrics: %+v", window)
	}
}

func TestS07MissingOneSecondSnapshotDegradesResolutionOnly(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	run := mustRun(t, traceFromRates(start, []int{500}, []int{300}, []int{220}, 2*time.Second))
	if !run.Useful.TemporalResolutionDegraded || run.Useful.MeanMbps != 500 {
		t.Fatalf("missing sample handled incorrectly: %+v", run.Useful)
	}
}

func TestS08ProcessRestartNeverCrossSubtracts(t *testing.T) {
	TestBuildTimelineRestartStartsNewEpoch(t)
}

func TestS09StaleRemoteTelemetryCannotCreateWriteStallFinding(t *testing.T) {
	b0, b1 := baselineRuns(t)
	start := time.Unix(1000, 0).UTC()
	trace := traceFromRates(start, []int{500, 500}, []int{270, 270}, []int{250, 250}, time.Second)
	for index := 1; index < len(trace); index++ {
		trace[index].Node.Logical.RemoteSender.Available = true
		trace[index].Node.Logical.RemoteSender.Stale = true
		trace[index].Node.Legs[1].RemoteWriteBlockedMS = 900
	}
	agg := mustRun(t, trace)
	comparison, _ := CompareToBaselines(agg, b0, b1, Topology{})
	findings, _, _ := Diagnose(agg, comparison, LoadSaturating)
	if !hasFinding(findings, FindingRemoteTelemetryStale) || hasFinding(findings, FindingRemoteWriteStallObserved) {
		t.Fatalf("stale telemetry used as current evidence: %+v", findings)
	}
}

func TestS10ExactSchedulerAssignmentRemainsOpen(t *testing.T) {
	b0, b1 := baselineRuns(t)
	start := time.Unix(1000, 0).UTC()
	agg := mustRun(t, traceFromRates(start, []int{800}, []int{500}, []int{320}, time.Second))
	comparison, _ := CompareToBaselines(agg, b0, b1, Topology{})
	_, open, _ := Diagnose(agg, comparison, LoadSaturating)
	for _, code := range []string{"LEG1_EXACT_ASSIGNMENT", "SCHEDULER_DECISION_REASON", "PER_LEG_UNIQUE_USEFUL_ATTRIBUTION"} {
		if !slices.ContainsFunc(open, func(item OpenQuestion) bool { return item.Code == code }) {
			t.Fatalf("%s was not kept OPEN: %+v", code, open)
		}
	}
}

func TestUnverifiedLoadCannotClaimNegativeAggregationGain(t *testing.T) {
	b0, b1 := baselineRuns(t)
	start := time.Unix(1000, 0).UTC()
	agg := mustRun(t, traceFromRates(start, []int{200}, []int{120}, []int{90}, time.Second))
	comparison, _ := CompareToBaselines(agg, b0, b1, Topology{})
	findings, _, _ := Diagnose(agg, comparison, LoadUnverified)
	if hasFinding(findings, FindingNegativeAggregationGain) || !hasFinding(findings, FindingLoadNotProvenSaturated) {
		t.Fatalf("unverified demand received capability verdict: %+v", findings)
	}
}

func TestSharedCeilingReference(t *testing.T) {
	b0, b1 := baselineRuns(t)
	start := time.Unix(1000, 0).UTC()
	agg := mustRun(t, traceFromRates(start, []int{180}, []int{90}, []int{95}, time.Second))
	comparison, err := CompareToBaselines(agg, b0, b1, Topology{SharedCeilingMbps: 200})
	if err != nil {
		t.Fatal(err)
	}
	if comparison.ReferenceMbps != 200 || comparison.EfficiencyVsReference != 0.9 {
		t.Fatalf("shared ceiling miscomputed: %+v", comparison)
	}
}

func TestSchedulerRateEstimateIsNotPathThroughput(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	trace := traceFromRates(start, []int{490}, []int{300}, []int{200}, time.Second)
	trace[1].Node.Legs[0].RemoteDeliveryRate = 100_000_000
	trace[1].Node.Legs[1].RemoteDeliveryRate = 90_000_000
	run := mustRun(t, trace)
	if run.Paths[0].MeanMbps != 300 || run.Paths[1].MeanMbps != 200 || run.Useful.MeanMbps != 490 {
		t.Fatalf("scheduler estimate leaked into throughput metrics: %+v", run)
	}
}

func TestPeakCannotOverrideNegativeSustainedGain(t *testing.T) {
	b0, b1 := baselineRuns(t)
	start := time.Unix(1000, 0).UTC()
	agg := mustRun(t, traceFromRates(start,
		[]int{900, 500, 500, 500, 500},
		[]int{500, 270, 270, 270, 270},
		[]int{420, 250, 250, 250, 250}, time.Second))
	comparison, err := CompareToBaselines(agg, b0, b1, Topology{})
	if err != nil {
		t.Fatal(err)
	}
	if agg.Useful.PeakMbps <= comparison.BestSingleMbps || comparison.GainVsBestSingle >= 0 {
		t.Fatalf("peak incorrectly overrode sustained result: peak=%.2f comparison=%+v", agg.Useful.PeakMbps, comparison)
	}
}

func TestExistingStatusEvidenceMeasuresFreshRemoteBoosterSenderTX(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	trace := traceFromRates(start, []int{500}, []int{300}, []int{220}, time.Second)
	trace[0].Node.Logical.RemoteSender = SenderDiagnostics{Available: true, Leg1TXBytes: 2000}
	trace[1].Node.Logical.RemoteSender = SenderDiagnostics{Available: true, Leg1TXBytes: 2000 + bytesForMbps(230, time.Second)}
	run := mustRun(t, trace)
	if run.BoosterSenderTXMeanMbps != 230 || run.RemoteSenderEvidenceWindowRatio != 1 {
		t.Fatalf("existing frozen peer sender evidence not preserved: %+v", run)
	}
}

func TestBaselineLegIdentityCannotBeSwapped(t *testing.T) {
	b0, b1 := baselineRuns(t)
	start := time.Unix(1000, 0).UTC()
	agg := mustRun(t, traceFromRates(start, []int{1000}, []int{500}, []int{520}, time.Second))
	if _, err := CompareToBaselines(agg, b1, b0, Topology{}); !errors.Is(err, ErrInvalidBaseline) {
		t.Fatalf("swapped physical baselines accepted: %v", err)
	}
}

func TestDiagnosticClosureCanPassWithFrozenStatusAndCAPEvidence(t *testing.T) {
	b0, b1 := baselineRuns(t)
	start := time.Unix(1000, 0).UTC()
	trace := traceFromRates(start, []int{1000, 1000}, []int{520, 520}, []int{500, 500}, time.Second)
	for i := range trace {
		trace[i].Node.Logical.RemoteSender.Available = true
		trace[i].Node.Logical.RemoteSender.Stale = false
		trace[i].Node.Logical.RemoteSender.Leg1TXBytes = uint64(i) * bytesForMbps(510, time.Second)
	}
	agg := mustRun(t, trace)
	comparison, err := CompareToBaselines(agg, b0, b1, Topology{})
	if err != nil {
		t.Fatal(err)
	}
	findings, open, err := Diagnose(agg, comparison, LoadSaturating)
	if err != nil {
		t.Fatal(err)
	}
	report := AggregationReport{Baseline: BaselineReport{Leg0: b0, Leg1: b1}, Aggregate: agg, Comparison: comparison, Findings: findings, Open: open}
	closure := EvaluateDiagnosticClosure(&report, JournalReport{Provided: true, CapacityWindows: 2, EvidenceComplete: true})
	if !closure.Pass {
		t.Fatalf("existing frozen evidence surfaces should be able to close a future complete replay: %+v", closure)
	}
}
