package audit

import (
	"bufio"
	"fmt"
	"io"
	"sort"
)

// JournalReport summarizes only evidence already emitted by the frozen runtime.
// CAP_WINDOW delivery/assignment are preferred-leg capacity-controller evidence;
// they are never relabeled as total two-leg aggregate throughput.
type JournalReport struct {
	Provided                      bool
	RecognizedEvents              uint64
	IgnoredLines                  uint64
	EvidenceComplete              bool
	DroppedEvents                 uint64
	SequenceGaps                  uint64
	CapacityWindows               int
	DeliveryReadyWindows          int
	GateOpened                    int
	GateBlocked                   int
	TargetMbps                    float64
	PreferredAssignmentMedianMbps float64
	PreferredAssignmentMinMbps    float64
	PreferredAssignmentMaxMbps    float64
	PreferredDeliveryMedianMbps   float64
	PreferredDeliveryMinMbps      float64
	PreferredDeliveryMaxMbps      float64
	LegAttached                   [2]bool
	LegDataActive                 [2]bool
}

func journalMedian(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	middle := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[middle]
	}
	return (sorted[middle-1] + sorted[middle]) / 2
}

func journalMinMax(values []float64) (float64, float64) {
	if len(values) == 0 {
		return 0, 0
	}
	minimum, maximum := values[0], values[0]
	for _, value := range values[1:] {
		minimum = min(minimum, value)
		maximum = max(maximum, value)
	}
	return minimum, maximum
}

func AnalyzeJournal(reader io.Reader) (JournalReport, error) {
	report := JournalReport{Provided: true}
	analyzer := NewAnalyzer()
	var assignments, deliveries []float64
	scanner := bufio.NewScanner(reader)
	buffer := make([]byte, 64<<10)
	scanner.Buffer(buffer, 8<<20)
	for scanner.Scan() {
		event, ok, err := ParseLine(scanner.Text())
		if err != nil {
			return report, err
		}
		if !ok {
			report.IgnoredLines++
			continue
		}
		report.RecognizedEvents++
		analyzer.Consume(event)
		if event.Capacity != nil {
			if event.Capacity.TargetMbps > 0 {
				report.TargetMbps = event.Capacity.TargetMbps
			}
			switch event.Capacity.Event {
			case "CAP_WINDOW":
				report.CapacityWindows++
				if event.Capacity.DeliveryReady {
					report.DeliveryReadyWindows++
				}
				assignments = append(assignments, event.Capacity.PreferredAssignmentMbps)
				deliveries = append(deliveries, event.Capacity.DeliveryMbps)
			case "CAP_GATE_OPENED":
				report.GateOpened++
			case "CAP_GATE_BLOCKED":
				report.GateBlocked++
			}
		}
		if event.Path != nil && event.Path.LegID >= 0 && event.Path.LegID < len(report.LegAttached) {
			if event.Path.State == PathAttached {
				report.LegAttached[event.Path.LegID] = true
			}
			if event.Path.State == PathDataActive {
				report.LegDataActive[event.Path.LegID] = true
			}
		}
		if event.Session != nil && event.Session.State == SessionEstablished && event.Session.LegID >= 0 && event.Session.LegID < len(report.LegAttached) {
			report.LegAttached[event.Session.LegID] = true
		}
	}
	if err := scanner.Err(); err != nil {
		return report, err
	}
	v2 := analyzer.Report()
	report.EvidenceComplete = v2.EvidenceComplete
	report.DroppedEvents = v2.DroppedEvents
	report.SequenceGaps = v2.SequenceGaps
	report.PreferredAssignmentMedianMbps = journalMedian(assignments)
	report.PreferredAssignmentMinMbps, report.PreferredAssignmentMaxMbps = journalMinMax(assignments)
	report.PreferredDeliveryMedianMbps = journalMedian(deliveries)
	report.PreferredDeliveryMinMbps, report.PreferredDeliveryMaxMbps = journalMinMax(deliveries)
	return report, nil
}

type AnswerState string

const (
	AnswerFull    AnswerState = "FULL"
	AnswerPartial AnswerState = "PARTIAL"
	AnswerMissing AnswerState = "MISSING"
)

type DiagnosticClosure struct {
	Baseline              AnswerState
	AggregateUseful       AnswerState
	PerLegDelivery        AnswerState
	SchedulerAllocation   AnswerState
	Stability             AnswerState
	Efficiency            AnswerState
	DegradationLayer      AnswerState
	AlgorithmABComparable AnswerState
	Blocking              []string
	Pass                  bool
}

// EvaluateDiagnosticClosure is deliberately stricter than ordinary unit-test
// correctness. V3 passes FIELD acceptance only when the evidence can answer the
// performance questions needed to change the aggregation algorithm. OPEN is not
// an automatic waiver.
func EvaluateDiagnosticClosure(report *AggregationReport, journal JournalReport, algorithmComparisons ...AlgorithmComparison) DiagnosticClosure {
	closure := DiagnosticClosure{
		Baseline:              AnswerMissing,
		AggregateUseful:       AnswerMissing,
		PerLegDelivery:        AnswerMissing,
		SchedulerAllocation:   AnswerMissing,
		Stability:             AnswerMissing,
		Efficiency:            AnswerMissing,
		DegradationLayer:      AnswerMissing,
		AlgorithmABComparable: AnswerMissing,
	}
	if report == nil {
		closure.Blocking = append(closure.Blocking,
			"missing status traces: historical journal alone cannot provide useful aggregate or both-leg delivery",
			"missing single-path status baselines")
		if journal.CapacityWindows > 0 {
			closure.SchedulerAllocation = AnswerPartial
			closure.DegradationLayer = AnswerPartial
		}
		if journal.Provided && (!journal.EvidenceComplete || journal.DroppedEvents > 0 || journal.SequenceGaps > 0) {
			closure.Blocking = append(closure.Blocking, "journal evidence is incomplete or contains dropped/sequence-gap events")
		}
		return closure
	}

	b0 := report.Baseline.Leg0
	b1 := report.Baseline.Leg1
	if b0.SinglePathVerified && b1.SinglePathVerified && b0.Duration > 0 && b1.Duration > 0 &&
		b0.LegBytes[0] > 0 && b0.LegBytes[1] == 0 && b1.LegBytes[1] > 0 && b1.LegBytes[0] == 0 {
		closure.Baseline = AnswerFull
	} else {
		closure.Blocking = append(closure.Blocking, "single-path baselines are missing or not physically verified")
	}

	if report.Aggregate.Duration > 0 && len(report.Aggregate.Windows) > 0 && report.Aggregate.UsefulBytes > 0 {
		closure.AggregateUseful = AnswerFull
	} else {
		closure.Blocking = append(closure.Blocking, "aggregate useful-delivery evidence is missing")
	}
	if report.Aggregate.LegBytes[0] > 0 && report.Aggregate.LegBytes[1] > 0 {
		closure.PerLegDelivery = AnswerFull
	} else if report.Aggregate.LegBytes[0] > 0 || report.Aggregate.LegBytes[1] > 0 {
		closure.PerLegDelivery = AnswerPartial
		closure.Blocking = append(closure.Blocking, "both-leg physical delivery was not observed")
	} else {
		closure.Blocking = append(closure.Blocking, "per-leg physical delivery evidence is missing")
	}

	if len(report.Aggregate.Windows) >= 2 {
		if report.Aggregate.Useful.TemporalResolutionDegraded || report.Evidence.TemporalResolutionDegraded {
			closure.Stability = AnswerPartial
			closure.Blocking = append(closure.Blocking, "stability evidence has degraded temporal resolution")
		} else {
			closure.Stability = AnswerFull
		}
	} else if len(report.Aggregate.Windows) == 1 {
		closure.Stability = AnswerPartial
		closure.Blocking = append(closure.Blocking, "stability requires multiple aggregate windows")
	} else {
		closure.Blocking = append(closure.Blocking, "stability evidence is missing")
	}

	if report.Comparison.BestSingleMbps > 0 && report.Comparison.ReferenceMbps > 0 {
		closure.Efficiency = AnswerFull
	} else {
		closure.Blocking = append(closure.Blocking, "efficiency reference is missing")
	}

	journalComplete := journal.Provided && journal.EvidenceComplete && journal.DroppedEvents == 0 && journal.SequenceGaps == 0
	if !journalComplete {
		closure.Blocking = append(closure.Blocking, "journal evidence must be complete with zero dropped events and zero sequence gaps")
	}
	if report.Load != LoadSaturating || !report.Demand.Verified || !report.Demand.Saturated || report.Demand.Method == "" {
		closure.Blocking = append(closure.Blocking, "saturating demand is not independently verified")
	}

	preferredAssignment := report.Aggregate.PreferredAssignmentEvidenceWindowRatio > 0
	boosterSender := report.Aggregate.RemoteSenderEvidenceWindowRatio > 0
	if preferredAssignment && boosterSender {
		// The frozen producer gives exact normal preferred-leg assignment and fresh
		// peer leg1 DATA transmission. This closes the operational
		// sender-output-vs-path-delivery question, but it is intentionally PARTIAL
		// because leg1 transmission is not exact normal scheduler assignment and no
		// per-decision reason exists in the frozen runtime.
		closure.SchedulerAllocation = AnswerPartial
	} else if preferredAssignment || boosterSender || journal.CapacityWindows > 0 {
		closure.SchedulerAllocation = AnswerPartial
		closure.Blocking = append(closure.Blocking, "need both preferred-assignment status evidence and fresh peer leg1 sender-TX evidence")
	} else {
		closure.Blocking = append(closure.Blocking, "scheduler-output evidence is missing")
	}

	discriminator := preferredAssignment && boosterSender
	for _, finding := range report.Findings {
		switch finding.Code {
		case FindingRemoteWriteStallObserved, FindingRemoteMemoryPressureObserved,
			FindingLocalMemoryPressureObserved, FindingReorderBacklogObserved:
			discriminator = true
		}
	}
	if closure.PerLegDelivery == AnswerFull && discriminator {
		closure.DegradationLayer = AnswerFull
	} else if closure.PerLegDelivery != AnswerMissing {
		closure.DegradationLayer = AnswerPartial
		closure.Blocking = append(closure.Blocking, "no complete sender-output/path-delivery or queue/resource discriminator during aggregate windows")
	} else {
		closure.Blocking = append(closure.Blocking, "degradation layer cannot be localized without per-leg delivery")
	}

	if len(algorithmComparisons) == 1 && algorithmComparisons[0].Comparable {
		closure.AlgorithmABComparable = AnswerFull
	} else {
		closure.Blocking = append(closure.Blocking, "missing comparable algorithm A/B runs for the same verified workload")
	}

	closure.Pass = journalComplete && report.Load == LoadSaturating && report.Demand.Verified && report.Demand.Saturated && report.Demand.Method != "" &&
		closure.Baseline == AnswerFull && closure.AggregateUseful == AnswerFull &&
		closure.PerLegDelivery == AnswerFull && closure.SchedulerAllocation != AnswerMissing &&
		closure.Stability == AnswerFull && closure.Efficiency == AnswerFull &&
		closure.DegradationLayer == AnswerFull && closure.AlgorithmABComparable == AnswerFull
	return closure
}

func (c DiagnosticClosure) Error() error {
	if c.Pass {
		return nil
	}
	return fmt.Errorf("diagnostic closure failed: %v", c.Blocking)
}
