package audit

import "fmt"

type EvidenceLevel string

const (
	EvidenceObserved   EvidenceLevel = "OBSERVED"
	EvidenceCorrelated EvidenceLevel = "CORRELATED"
	EvidenceOpen       EvidenceLevel = "OPEN"
)

type FindingCode string

const (
	FindingBothPathsCarrying            FindingCode = "BOTH_PATHS_CARRYING"
	FindingBelowBestSingleObserved      FindingCode = "BELOW_BEST_SINGLE_OBSERVED"
	FindingNegativeAggregationGain      FindingCode = "NEGATIVE_AGGREGATION_GAIN"
	FindingBothPathRatesBelowStandalone FindingCode = "BOTH_PATH_RATES_BELOW_STANDALONE"
	FindingRemoteWriteStallObserved     FindingCode = "REMOTE_WRITE_STALL_OBSERVED"
	FindingRemoteMemoryPressureObserved FindingCode = "REMOTE_MEMORY_PRESSURE_OBSERVED"
	FindingLocalMemoryPressureObserved  FindingCode = "LOCAL_MEMORY_PRESSURE_OBSERVED"
	FindingReorderBacklogObserved       FindingCode = "REORDER_BACKLOG_OBSERVED"
	FindingRemoteTelemetryStale         FindingCode = "REMOTE_TELEMETRY_STALE"
	FindingLoadNotProvenSaturated       FindingCode = "LOAD_NOT_PROVEN_SATURATED"
)

type Finding struct {
	Code     FindingCode
	Level    EvidenceLevel
	Evidence []string
}

type OpenQuestion struct {
	Code   string
	Reason string
}

func Diagnose(aggregate RunMetrics, comparison AggregateMetrics, load LoadClass) ([]Finding, []OpenQuestion, error) {
	if load != LoadSaturating && load != LoadUnverified {
		return nil, nil, ErrUnsupportedLoad
	}
	var findings []Finding
	if aggregate.LegBytes[0] > 0 && aggregate.LegBytes[1] > 0 {
		findings = append(findings, Finding{Code: FindingBothPathsCarrying, Level: EvidenceObserved, Evidence: []string{
			fmt.Sprintf("leg0_bytes=%d", aggregate.LegBytes[0]), fmt.Sprintf("leg1_bytes=%d", aggregate.LegBytes[1]),
		}})
	}
	if comparison.TimeBelowBestSingleRatio > 0 {
		findings = append(findings, Finding{Code: FindingBelowBestSingleObserved, Level: EvidenceObserved, Evidence: []string{
			fmt.Sprintf("time_below_best_single=%.4f", comparison.TimeBelowBestSingleRatio),
		}})
	}
	if comparison.BothPathsBelowBaselineRatio > 0 {
		findings = append(findings, Finding{Code: FindingBothPathRatesBelowStandalone, Level: EvidenceObserved, Evidence: []string{
			fmt.Sprintf("time_both_paths_below_standalone=%.4f", comparison.BothPathsBelowBaselineRatio),
		}})
	}
	if load == LoadSaturating && comparison.GainVsBestSingle < 0 {
		findings = append(findings, Finding{Code: FindingNegativeAggregationGain, Level: EvidenceObserved, Evidence: []string{
			fmt.Sprintf("gain_vs_best_single=%.4f", comparison.GainVsBestSingle),
		}})
	}
	if load == LoadUnverified {
		findings = append(findings, Finding{Code: FindingLoadNotProvenSaturated, Level: EvidenceOpen})
	}
	remoteStale := false
	remoteWriteStall := false
	remoteMemory := false
	localMemory := false
	reorder := false
	for _, window := range aggregate.Windows {
		d := window.Diagnostics
		localMemory = localMemory || d.LocalMemoryPressure
		reorder = reorder || d.ReorderBytes > 0
		if d.RemoteSenderAvailable && d.RemoteSenderStale {
			remoteStale = true
		}
		if d.RemoteSenderAvailable && !d.RemoteSenderStale {
			remoteMemory = remoteMemory || d.RemoteMemoryPressure
			for _, leg := range d.Leg {
				remoteWriteStall = remoteWriteStall || leg.RemoteWriteBlockedMS > 0
			}
		}
	}
	if remoteStale {
		findings = append(findings, Finding{Code: FindingRemoteTelemetryStale, Level: EvidenceObserved})
	}
	if remoteWriteStall {
		findings = append(findings, Finding{Code: FindingRemoteWriteStallObserved, Level: EvidenceObserved})
	}
	if remoteMemory {
		findings = append(findings, Finding{Code: FindingRemoteMemoryPressureObserved, Level: EvidenceObserved})
	}
	if localMemory {
		findings = append(findings, Finding{Code: FindingLocalMemoryPressureObserved, Level: EvidenceObserved})
	}
	if reorder {
		findings = append(findings, Finding{Code: FindingReorderBacklogObserved, Level: EvidenceObserved})
	}
	open := []OpenQuestion{
		{Code: "LEG1_EXACT_ASSIGNMENT", Reason: "frozen evidence exposes no exact per-window normal scheduler assignment for leg1"},
		{Code: "SCHEDULER_DECISION_REASON", Reason: "frozen evidence exposes no per-decision scheduler reason"},
		{Code: "PER_LEG_UNIQUE_USEFUL_ATTRIBUTION", Reason: "path DATA counters can include reinjected/overlapping logical bytes"},
	}
	return findings, open, nil
}
