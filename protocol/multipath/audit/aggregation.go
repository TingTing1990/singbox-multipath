package audit

import (
	"fmt"
	"math"
	"sort"
	"time"
)

type PathMetrics struct {
	MeanMbps      float64
	MedianMbps    float64
	P10Mbps       float64
	P90Mbps       float64
	PeakMbps      float64
	PhysicalShare float64
	TotalBytes    uint64
}

type StabilityMetrics struct {
	MeanMbps                   float64
	MedianMbps                 float64
	P10Mbps                    float64
	P90Mbps                    float64
	MinimumMbps                float64
	PeakMbps                   float64
	CV                         float64
	TimeBelowBestSingleRatio   float64
	TemporalResolutionDegraded bool
}

type RunMetrics struct {
	Windows                         []Window
	Duration                        time.Duration
	UsefulBytes                     uint64
	LegBytes                        [2]uint64
	Useful                          StabilityMetrics
	Paths                           [2]PathMetrics
	PhysicalMeanMbps                float64
	PathLogicalGapMeanMbps          float64
	BoosterSenderTXMeanMbps         float64
	RemoteSenderEvidenceWindowRatio float64
	SinglePathVerified              bool
}

type weightedValue struct {
	value  float64
	weight float64
}

func weightedQuantile(values []weightedValue, q float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]weightedValue(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].value < sorted[j].value })
	var total float64
	for _, value := range sorted {
		total += value.weight
	}
	if total <= 0 {
		return 0
	}
	target := q * total
	var cumulative float64
	for _, value := range sorted {
		cumulative += value.weight
		if cumulative >= target {
			return value.value
		}
	}
	return sorted[len(sorted)-1].value
}

func AnalyzeRun(windows []Window) (RunMetrics, error) {
	active, err := ActiveEnvelope(windows)
	if err != nil {
		return RunMetrics{}, err
	}
	result := RunMetrics{Windows: active}
	result.Useful.MinimumMbps = math.Inf(1)
	var usefulValues []weightedValue
	var legValues [2][]weightedValue
	var weightedMeanNumerator float64
	var weightedSquareNumerator float64
	var physicalNumerator float64
	var gapNumerator float64
	var boosterSenderNumerator float64
	var remoteSenderDuration time.Duration
	for _, window := range active {
		seconds := window.Duration.Seconds()
		if seconds <= 0 {
			return RunMetrics{}, fmt.Errorf("invalid window duration")
		}
		result.Duration += window.Duration
		result.UsefulBytes += window.UsefulRXBytes
		result.Useful.MinimumMbps = min(result.Useful.MinimumMbps, window.UsefulMbps)
		result.Useful.PeakMbps = max(result.Useful.PeakMbps, window.UsefulMbps)
		result.Useful.TemporalResolutionDegraded = result.Useful.TemporalResolutionDegraded || window.ResolutionDegraded
		usefulValues = append(usefulValues, weightedValue{window.UsefulMbps, seconds})
		weightedMeanNumerator += window.UsefulMbps * seconds
		weightedSquareNumerator += window.UsefulMbps * window.UsefulMbps * seconds
		physicalNumerator += window.PhysicalMbps * seconds
		gapNumerator += window.PathLogicalGapMbps * seconds
		if window.RemoteSenderEvidence {
			boosterSenderNumerator += window.BoosterSenderTXMbps * seconds
			remoteSenderDuration += window.Duration
		}
		for leg := range result.Paths {
			result.LegBytes[leg] += window.LegRXBytes[leg]
			result.Paths[leg].PeakMbps = max(result.Paths[leg].PeakMbps, window.LegMbps[leg])
			legValues[leg] = append(legValues[leg], weightedValue{window.LegMbps[leg], seconds})
		}
	}
	seconds := result.Duration.Seconds()
	if seconds <= 0 {
		return RunMetrics{}, fmt.Errorf("active duration is zero")
	}
	result.Useful.MeanMbps = float64(result.UsefulBytes) * 8 / seconds / 1_000_000
	result.Useful.MedianMbps = weightedQuantile(usefulValues, 0.50)
	result.Useful.P10Mbps = weightedQuantile(usefulValues, 0.10)
	result.Useful.P90Mbps = weightedQuantile(usefulValues, 0.90)
	if math.IsInf(result.Useful.MinimumMbps, 1) {
		result.Useful.MinimumMbps = 0
	}
	mean := weightedMeanNumerator / seconds
	variance := weightedSquareNumerator/seconds - mean*mean
	if variance < 0 && variance > -1e-9 {
		variance = 0
	}
	if mean > 0 && variance >= 0 {
		result.Useful.CV = math.Sqrt(variance) / mean
	}
	result.PhysicalMeanMbps = physicalNumerator / seconds
	result.PathLogicalGapMeanMbps = gapNumerator / seconds
	if remoteSenderDuration > 0 {
		remoteSeconds := remoteSenderDuration.Seconds()
		result.BoosterSenderTXMeanMbps = boosterSenderNumerator / remoteSeconds
		result.RemoteSenderEvidenceWindowRatio = remoteSeconds / seconds
	}
	var totalLegBytes uint64
	for leg := range result.Paths {
		path := &result.Paths[leg]
		path.TotalBytes = result.LegBytes[leg]
		path.MeanMbps = float64(result.LegBytes[leg]) * 8 / seconds / 1_000_000
		path.MedianMbps = weightedQuantile(legValues[leg], 0.50)
		path.P10Mbps = weightedQuantile(legValues[leg], 0.10)
		path.P90Mbps = weightedQuantile(legValues[leg], 0.90)
		totalLegBytes += result.LegBytes[leg]
	}
	if totalLegBytes > 0 {
		for leg := range result.Paths {
			result.Paths[leg].PhysicalShare = float64(result.LegBytes[leg]) / float64(totalLegBytes)
		}
	}
	result.SinglePathVerified = result.LegBytes[0] == 0 || result.LegBytes[1] == 0
	return result, nil
}

type Topology struct {
	SharedCeilingMbps float64
}

type LoadClass string

const (
	LoadSaturating LoadClass = "SATURATING"
	LoadUnverified LoadClass = "UNVERIFIED"
)

type AggregateMetrics struct {
	BestSingleMbps              float64
	ReferenceMbps               float64
	GainVsBestSingle            float64
	EfficiencyVsReference       float64
	TimeBelowBestSingleRatio    float64
	BothPathsBelowBaselineRatio float64
}

func CompareToBaselines(aggregate RunMetrics, baselineLeg0, baselineLeg1 RunMetrics, topology Topology) (AggregateMetrics, error) {
	if !baselineLeg0.SinglePathVerified || !baselineLeg1.SinglePathVerified ||
		baselineLeg0.LegBytes[0] == 0 || baselineLeg0.LegBytes[1] != 0 ||
		baselineLeg1.LegBytes[1] == 0 || baselineLeg1.LegBytes[0] != 0 {
		return AggregateMetrics{}, ErrInvalidBaseline
	}
	a := baselineLeg0.Useful.MeanMbps
	b := baselineLeg1.Useful.MeanMbps
	if a <= 0 || b <= 0 {
		return AggregateMetrics{}, ErrInvalidBaseline
	}
	if topology.SharedCeilingMbps < 0 {
		return AggregateMetrics{}, ErrInvalidTopology
	}
	result := AggregateMetrics{BestSingleMbps: max(a, b), ReferenceMbps: a + b}
	if topology.SharedCeilingMbps > 0 {
		result.ReferenceMbps = min(result.ReferenceMbps, topology.SharedCeilingMbps)
	}
	if result.ReferenceMbps <= 0 {
		return AggregateMetrics{}, ErrInvalidTopology
	}
	result.GainVsBestSingle = aggregate.Useful.MeanMbps/result.BestSingleMbps - 1
	result.EfficiencyVsReference = aggregate.Useful.MeanMbps / result.ReferenceMbps
	var belowDuration, bothBelowDuration time.Duration
	for _, window := range aggregate.Windows {
		if window.UsefulMbps < result.BestSingleMbps {
			belowDuration += window.Duration
		}
		if window.LegMbps[0] < a && window.LegMbps[1] < b {
			bothBelowDuration += window.Duration
		}
	}
	if aggregate.Duration > 0 {
		result.TimeBelowBestSingleRatio = float64(belowDuration) / float64(aggregate.Duration)
		result.BothPathsBelowBaselineRatio = float64(bothBelowDuration) / float64(aggregate.Duration)
	}
	return result, nil
}

type EvidenceReport struct {
	StatusSchemaVersion        int
	AggregateStatusSnapshots   int
	AggregateEpochs            int
	AggregateWindows           int
	TemporalResolutionDegraded bool
	Journal                    JournalReport
}

type BaselineReport struct {
	Leg0 RunMetrics
	Leg1 RunMetrics
}

type AggregationReport struct {
	Evidence   EvidenceReport
	Load       LoadClass
	Baseline   BaselineReport
	Aggregate  RunMetrics
	Comparison AggregateMetrics
	Findings   []Finding
	Open       []OpenQuestion
}

func AnalyzeAggregation(baseline0Snapshots, baseline1Snapshots, aggregateSnapshots []StatusSnapshot, topology Topology, load LoadClass, journal *JournalReport) (AggregationReport, error) {
	b0Timeline, err := BuildTimeline(baseline0Snapshots)
	if err != nil {
		return AggregationReport{}, fmt.Errorf("baseline leg0 timeline: %w", err)
	}
	b1Timeline, err := BuildTimeline(baseline1Snapshots)
	if err != nil {
		return AggregationReport{}, fmt.Errorf("baseline leg1 timeline: %w", err)
	}
	aggTimeline, err := BuildTimeline(aggregateSnapshots)
	if err != nil {
		return AggregationReport{}, fmt.Errorf("aggregate timeline: %w", err)
	}
	b0, err := AnalyzeRun(b0Timeline.Windows)
	if err != nil {
		return AggregationReport{}, fmt.Errorf("baseline leg0: %w", err)
	}
	b1, err := AnalyzeRun(b1Timeline.Windows)
	if err != nil {
		return AggregationReport{}, fmt.Errorf("baseline leg1: %w", err)
	}
	agg, err := AnalyzeRun(aggTimeline.Windows)
	if err != nil {
		return AggregationReport{}, fmt.Errorf("aggregate: %w", err)
	}
	comparison, err := CompareToBaselines(agg, b0, b1, topology)
	if err != nil {
		return AggregationReport{}, err
	}
	agg.Useful.TimeBelowBestSingleRatio = comparison.TimeBelowBestSingleRatio
	findings, open, err := Diagnose(agg, comparison, load)
	if err != nil {
		return AggregationReport{}, err
	}
	report := AggregationReport{
		Evidence: EvidenceReport{
			StatusSchemaVersion:        StatusSchemaVersion,
			AggregateStatusSnapshots:   aggTimeline.StatusSnapshots,
			AggregateEpochs:            aggTimeline.Epochs,
			AggregateWindows:           len(agg.Windows),
			TemporalResolutionDegraded: aggTimeline.TemporalResolutionDegraded,
		},
		Load:       load,
		Baseline:   BaselineReport{Leg0: b0, Leg1: b1},
		Aggregate:  agg,
		Comparison: comparison,
		Findings:   findings,
		Open:       open,
	}
	if journal != nil {
		report.Evidence.Journal = *journal
	}
	return report, nil
}
