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
	findings, _, err := Diagnose(agg, comparison, LoadSaturating, DemandEvidence{Verified: true, Saturated: true, Method: "controlled-test"})
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
	findings, _, _ := Diagnose(agg, comparison, LoadSaturating, DemandEvidence{Verified: true, Saturated: true, Method: "controlled-test"})
	if comparison.GainVsBestSingle >= 0 || comparison.TimeBelowBestSingleRatio != 1 || !hasFinding(findings, FindingNegativeAggregationGain) {
		t.Fatalf("negative gain missed: %+v %+v", comparison, findings)
	}
}

func TestS04BothPathsBelowStandaloneDoesNotClaimSchedulerBug(t *testing.T) {
	b0, b1 := baselineRuns(t)
	start := time.Unix(1000, 0).UTC()
	agg := mustRun(t, traceFromRates(start, []int{500, 500}, []int{270, 270}, []int{250, 250}, time.Second))
	comparison, _ := CompareToBaselines(agg, b0, b1, Topology{})
	findings, open, _ := Diagnose(agg, comparison, LoadSaturating, DemandEvidence{Verified: true, Saturated: true, Method: "controlled-test"})
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
	findings, _, _ := Diagnose(agg, comparison, LoadSaturating, DemandEvidence{Verified: true, Saturated: true, Method: "controlled-test"})
	if !hasFinding(findings, FindingRemoteTelemetryStale) || hasFinding(findings, FindingRemoteWriteStallObserved) {
		t.Fatalf("stale telemetry used as current evidence: %+v", findings)
	}
}

func TestS10ExactSchedulerAssignmentRemainsOpen(t *testing.T) {
	b0, b1 := baselineRuns(t)
	start := time.Unix(1000, 0).UTC()
	agg := mustRun(t, traceFromRates(start, []int{800}, []int{500}, []int{320}, time.Second))
	comparison, _ := CompareToBaselines(agg, b0, b1, Topology{})
	_, open, _ := Diagnose(agg, comparison, LoadSaturating, DemandEvidence{Verified: true, Saturated: true, Method: "controlled-test"})
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
	findings, _, _ := Diagnose(agg, comparison, LoadUnverified, DemandEvidence{})
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

func completeDiagnosticReport(t *testing.T, aggregateRates []int) AggregationReport {
	t.Helper()
	b0, b1 := baselineRuns(t)
	start := time.Unix(1000, 0).UTC()
	leg0 := make([]int, len(aggregateRates))
	leg1 := make([]int, len(aggregateRates))
	for i, rate := range aggregateRates {
		leg0[i] = rate * 52 / 100
		leg1[i] = rate * 50 / 100
	}
	trace := traceFromRates(start, aggregateRates, leg0, leg1, time.Second)
	for i := range trace {
		trace[i].Node.Logical.State = "aggregating"
		trace[i].Node.Logical.Connections = 1
		trace[i].Node.Logical.RemoteSender.Available = true
		trace[i].Node.Logical.RemoteSender.Stale = false
		trace[i].Node.Logical.RemoteSender.Leg1TXBytes = uint64(i) * bytesForMbps(510, time.Second)
		trace[i].Node.Logical.PreferredCapacity = &PreferredCapacityStatus{
			TargetMbps:             640,
			PreferredAssignedBytes: uint64(i) * bytesForMbps(520, time.Second),
		}
	}
	agg := mustRun(t, trace)
	comparison, err := CompareToBaselines(agg, b0, b1, Topology{})
	if err != nil {
		t.Fatal(err)
	}
	findings, open, err := Diagnose(agg, comparison, LoadSaturating, DemandEvidence{Verified: true, Saturated: true, Method: "controlled-test"})
	if err != nil {
		t.Fatal(err)
	}
	journal := JournalReport{Provided: true, RecognizedEvents: 2, CapacityWindows: 2, EvidenceComplete: true}
	return AggregationReport{
		Evidence:   EvidenceReport{StatusSchemaVersion: StatusSchemaVersion, AggregateStatusSnapshots: len(trace), AggregateEpochs: 1, AggregateWindows: len(agg.Windows), Journal: journal},
		Load:       LoadSaturating,
		Demand:     DemandEvidence{Verified: true, Saturated: true, Method: "controlled-saturated-download"},
		Baseline:   BaselineReport{Leg0: b0, Leg1: b1},
		Aggregate:  agg,
		Comparison: comparison,
		Findings:   findings,
		Open:       open,
	}
}

func TestDiagnosticClosureRejectsEmptyReport(t *testing.T) {
	closure := EvaluateDiagnosticClosure(&AggregationReport{}, JournalReport{})
	if closure.Pass || closure.Baseline == AnswerFull || closure.AggregateUseful == AnswerFull || closure.AlgorithmABComparable == AnswerFull {
		t.Fatalf("empty report falsely closed: %+v", closure)
	}
}

func TestDiagnosticClosureRejectsIncompleteJournal(t *testing.T) {
	report := completeDiagnosticReport(t, []int{1000, 1000})
	bad := report.Evidence.Journal
	bad.EvidenceComplete = false
	bad.DroppedEvents = 3
	bad.SequenceGaps = 1
	closure := EvaluateDiagnosticClosure(&report, bad)
	if closure.Pass {
		t.Fatalf("incomplete journal falsely closed: %+v", closure)
	}
}

func TestSaturatingAggregationRequiresVerifiedDemandEvidence(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	b0 := traceFromRates(start, []int{700, 700}, []int{700, 700}, []int{0, 0}, time.Second)
	b1 := traceFromRates(start, []int{650, 650}, []int{0, 0}, []int{650, 650}, time.Second)
	agg := traceFromRates(start, []int{1000, 1000}, []int{520, 520}, []int{500, 500}, time.Second)
	if _, err := AnalyzeAggregation(b0, b1, agg, Topology{}, LoadSaturating, nil); !errors.Is(err, ErrUnverifiedDemand) {
		t.Fatalf("unverified saturating load accepted: %v", err)
	}
}

func TestAlgorithmABRequiresSameVerifiedWorkload(t *testing.T) {
	a := completeDiagnosticReport(t, []int{900, 900})
	b := completeDiagnosticReport(t, []int{1000, 1000})
	comparison, err := CompareAlgorithms(
		AlgorithmRun{AlgorithmID: "A", WorkloadID: "same-controlled-run", Report: a},
		AlgorithmRun{AlgorithmID: "B", WorkloadID: "same-controlled-run", Report: b},
	)
	if err != nil || !comparison.Comparable || comparison.UsefulMeanDeltaMbps <= 0 {
		t.Fatalf("valid A/B comparison rejected: comparison=%+v err=%v", comparison, err)
	}
	if _, err := CompareAlgorithms(
		AlgorithmRun{AlgorithmID: "A", WorkloadID: "workload-a", Report: a},
		AlgorithmRun{AlgorithmID: "B", WorkloadID: "workload-b", Report: b},
	); !errors.Is(err, ErrAlgorithmNotComparable) {
		t.Fatalf("different workloads accepted: %v", err)
	}
}

func TestDiagnosticClosureRequiresAlgorithmABEvidence(t *testing.T) {
	report := completeDiagnosticReport(t, []int{1000, 1000})
	closure := EvaluateDiagnosticClosure(&report, report.Evidence.Journal)
	if closure.Pass || closure.AlgorithmABComparable != AnswerMissing {
		t.Fatalf("single run falsely declared A/B comparable: %+v", closure)
	}
}

func TestDiagnosticClosureCanPassWithOperationalSchedulerEvidenceAndAB(t *testing.T) {
	a := completeDiagnosticReport(t, []int{900, 900})
	b := completeDiagnosticReport(t, []int{1000, 1000})
	ab, err := CompareAlgorithms(
		AlgorithmRun{AlgorithmID: "A", WorkloadID: "same-controlled-run", Report: a},
		AlgorithmRun{AlgorithmID: "B", WorkloadID: "same-controlled-run", Report: b},
	)
	if err != nil {
		t.Fatal(err)
	}
	closure := EvaluateDiagnosticClosure(&b, b.Evidence.Journal, ab)
	if !closure.Pass {
		t.Fatalf("complete evidence should close: %+v", closure)
	}
	if closure.SchedulerAllocation != AnswerPartial {
		t.Fatalf("leg1 transmission was overclaimed as exact assignment: %+v", closure)
	}
}
