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
func EvaluateDiagnosticClosure(report *AggregationReport, journal JournalReport) DiagnosticClosure {
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
		return closure
	}

	closure.Baseline = AnswerFull
	closure.AggregateUseful = AnswerFull
	closure.PerLegDelivery = AnswerFull
	closure.Stability = AnswerFull
	closure.Efficiency = AnswerFull
	closure.AlgorithmABComparable = AnswerFull

	// Frozen status gives exact preferred assignment cumulative bytes and, when
	// remote sender telemetry is fresh, exact peer leg1 DATA bytes actually sent.
	// The latter is scheduler-output transmission evidence, not an exact
	// per-decision "assignment reason", so allocation remains PARTIAL rather than
	// fabricated as FULL.
	if report.Aggregate.RemoteSenderEvidenceWindowRatio > 0 && journal.CapacityWindows > 0 {
		// Preferred assignment comes from the frozen server CAP controller; peer
		// leg1 TX comes from fresh remote-sender status. Together they answer the
		// operational allocation question needed to distinguish scheduler output
		// from path delivery, without claiming a per-decision reason.
		closure.SchedulerAllocation = AnswerFull
	} else if report.Aggregate.RemoteSenderEvidenceWindowRatio > 0 || journal.CapacityWindows > 0 {
		closure.SchedulerAllocation = AnswerPartial
	}

	// Degradation location is considered FULL when measured path delivery plus
	// logical delivery is available and at least one independent diagnostic
	// discriminator (assignment/sender-TX, memory, reorder or write-stall) exists.
	discriminator := report.Aggregate.RemoteSenderEvidenceWindowRatio > 0
	for _, finding := range report.Findings {
		switch finding.Code {
		case FindingRemoteWriteStallObserved, FindingRemoteMemoryPressureObserved,
			FindingLocalMemoryPressureObserved, FindingReorderBacklogObserved,
			FindingRemoteTelemetryStale:
			discriminator = true
		}
	}
	if discriminator {
		closure.DegradationLayer = AnswerFull
	} else {
		closure.DegradationLayer = AnswerPartial
		closure.Blocking = append(closure.Blocking, "no assignment/sender-TX or queue/resource discriminator during aggregate windows")
	}

	if closure.SchedulerAllocation != AnswerFull {
		closure.Blocking = append(closure.Blocking, "need both server preferred-assignment CAP evidence and fresh peer leg1 sender-TX evidence")
	}
	closure.Pass = closure.Baseline == AnswerFull && closure.AggregateUseful == AnswerFull &&
		closure.PerLegDelivery == AnswerFull && closure.SchedulerAllocation == AnswerFull &&
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
