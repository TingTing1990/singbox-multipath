package audit

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"time"
)

type CapacityWindowEvidence struct {
	At                      time.Time
	Side                    string
	Instance                string
	ControllerWindowSeq     uint64
	TargetMbps              float64
	PreferredAssignmentMbps float64
	DeliveryMbps            float64
	DeliveryReady           bool
	ProtectedMbps           float64
	ProtectionValid         bool
	ProtectionActive        bool
	DegradeWindows          int
	BacklogBytes            int64
	QueueBytes              int64

	// CAP_WINDOW rates summarize the completed controller interval ending at At.
	// The frozen runtime does not emit that interval's start explicitly. V3 may
	// reconstruct it only when the immediately preceding CAP_WINDOW endpoint for
	// the same side/instance is a trustworthy temporal anchor and the controller
	// window sequence is consecutive. First endpoints, sequence gaps/resets and
	// timestamp rollback therefore remain explicitly uncovered.
	IntervalStart time.Time
	IntervalEnd   time.Time
	IntervalKnown bool
}

func (e CapacityWindowEvidence) ValidServerEvidence() bool {
	return e.Side == "server" && e.Instance != "" && !e.At.IsZero()
}

func (e CapacityWindowEvidence) ValidServerInterval() bool {
	return e.ValidServerEvidence() && e.IntervalKnown && !e.IntervalStart.IsZero() &&
		e.IntervalEnd.Equal(e.At) && e.IntervalEnd.After(e.IntervalStart)
}

func deriveCapacityEvidenceIntervals(evidence []CapacityWindowEvidence) {
	type groupKey struct {
		side     string
		instance string
	}
	type anchor struct {
		value       CapacityWindowEvidence
		trusted     bool
		maxObserved time.Time
	}

	anchors := make(map[groupKey]anchor)
	for index := range evidence {
		current := &evidence[index]
		if !current.ValidServerEvidence() {
			continue
		}
		key := groupKey{side: current.Side, instance: current.Instance}
		previous, ok := anchors[key]
		if !ok {
			anchors[key] = anchor{value: *current, trusted: true, maxObserved: current.At}
			continue
		}

		strictlyForward := current.At.After(previous.maxObserved)
		consecutive := current.ControllerWindowSeq == previous.value.ControllerWindowSeq+1
		if previous.trusted && strictlyForward && consecutive {
			current.IntervalStart = previous.value.At
			current.IntervalEnd = current.At
			current.IntervalKnown = true
		}

		maxObserved := previous.maxObserved
		if current.At.After(maxObserved) {
			maxObserved = current.At
		}
		// A non-monotonic timestamp cannot be used as the start anchor for the
		// next interval. A sequence gap/reset at a forward timestamp leaves this
		// interval unknown, but the observed endpoint can safely anchor the next
		// consecutive interval.
		anchors[key] = anchor{value: *current, trusted: strictlyForward, maxObserved: maxObserved}
	}
}

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
	SequenceEpochs                uint64
	ValidationErrors              []string
	CapacityWindows               int
	ServerCapacityWindows         int
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
	CapacityEvidence              []CapacityWindowEvidence
	CapacityEvents                []CapacityState
	Events                        []Event
}

func journalCompleteForV3(report JournalReport) bool {
	if !report.Provided || report.RecognizedEvents == 0 || !report.EvidenceComplete || report.DroppedEvents != 0 || report.SequenceGaps != 0 {
		return false
	}
	for _, evidence := range report.CapacityEvidence {
		if evidence.ValidServerEvidence() {
			return true
		}
	}
	return false
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

func frozenLogTimestamp(raw string) (time.Time, bool) {
	const layout = "-0700 2006-01-02 15:04:05"
	const width = len("+0800 2026-09-27 11:23:23")
	if len(raw) < width {
		return time.Time{}, false
	}
	at, err := time.Parse(layout, raw[:width])
	if err != nil {
		return time.Time{}, false
	}
	return at, true
}

// AnalyzeJournalForInstance filters a mixed server journal by the explicit
// inbound instance before V2 sequence validation. CAP event_seq is instance
// scoped, so validating an interleaved multi-instance journal as one sequence
// would create false gaps and cross-instance evidence.
func AnalyzeJournalForInstance(reader io.Reader, serverInstance string) (JournalReport, error) {
	if serverInstance == "" {
		return JournalReport{}, fmt.Errorf("missing server instance binding")
	}
	marker := "[" + serverInstance + "]"
	capacityMarker := "instance=\"" + serverInstance + "\""
	var filtered strings.Builder
	scanner := bufio.NewScanner(reader)
	buffer := make([]byte, 64<<10)
	scanner.Buffer(buffer, 8<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, marker) || strings.Contains(line, capacityMarker) {
			filtered.WriteString(line)
			filtered.WriteByte('\n')
		}
	}
	if err := scanner.Err(); err != nil {
		return JournalReport{}, err
	}
	return AnalyzeJournal(strings.NewReader(filtered.String()))
}

func AnalyzeJournal(reader io.Reader) (JournalReport, error) {
	report := JournalReport{Provided: true}
	analyzer := NewAnalyzer()
	var assignments, deliveries []float64
	scanner := bufio.NewScanner(reader)
	buffer := make([]byte, 64<<10)
	scanner.Buffer(buffer, 8<<20)
	for scanner.Scan() {
		rawLine := scanner.Text()
		event, ok, err := ParseLine(rawLine)
		if err != nil {
			return report, err
		}
		if !ok {
			report.IgnoredLines++
			continue
		}
		if event.ObservedAt.IsZero() {
			if at, timestamped := frozenLogTimestamp(rawLine); timestamped {
				event.ObservedAt = at
			}
		}
		report.RecognizedEvents++
		report.Events = append(report.Events, event)
		analyzer.Consume(event)
		if event.Capacity != nil {
			report.CapacityEvents = append(report.CapacityEvents, *event.Capacity)
			if event.Capacity.TargetMbps > 0 {
				report.TargetMbps = event.Capacity.TargetMbps
			}
			switch event.Capacity.Event {
			case "CAP_WINDOW":
				report.CapacityWindows++
				evidence := CapacityWindowEvidence{
					At:                      event.Capacity.At,
					Side:                    event.Capacity.Side,
					Instance:                event.Capacity.Instance,
					ControllerWindowSeq:     event.Capacity.ControllerWindowSeq,
					TargetMbps:              event.Capacity.TargetMbps,
					PreferredAssignmentMbps: event.Capacity.PreferredAssignmentMbps,
					DeliveryMbps:            event.Capacity.DeliveryMbps,
					DeliveryReady:           event.Capacity.DeliveryReady,
					ProtectedMbps:           event.Capacity.ProtectedMbps,
					ProtectionValid:         event.Capacity.ProtectionValid,
					ProtectionActive:        event.Capacity.ProtectionActive,
					DegradeWindows:          event.Capacity.DegradeWindows,
					BacklogBytes:            event.Capacity.BacklogBytes,
					QueueBytes:              event.Capacity.QueueBytes,
				}
				report.CapacityEvidence = append(report.CapacityEvidence, evidence)
				if evidence.ValidServerEvidence() {
					report.ServerCapacityWindows++
				}
				if event.Capacity.DeliveryReady {
					report.DeliveryReadyWindows++
				}
				if evidence.ValidServerEvidence() {
					assignments = append(assignments, event.Capacity.PreferredAssignmentMbps)
					deliveries = append(deliveries, event.Capacity.DeliveryMbps)
				}
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
	report.SequenceEpochs = v2.SequenceEpochs
	report.ValidationErrors = append([]string(nil), v2.ValidationErrors...)
	report.PreferredAssignmentMedianMbps = journalMedian(assignments)
	report.PreferredAssignmentMinMbps, report.PreferredAssignmentMaxMbps = journalMinMax(assignments)
	report.PreferredDeliveryMedianMbps = journalMedian(deliveries)
	report.PreferredDeliveryMinMbps, report.PreferredDeliveryMaxMbps = journalMinMax(deliveries)
	deriveCapacityEvidenceIntervals(report.CapacityEvidence)
	return report, nil
}

type FieldRunBinding struct {
	ClientNodeTag  string
	ServerInstance string
}

type FieldRunReplay struct {
	Binding  FieldRunBinding
	Timeline Timeline
	Journal  JournalReport
	Run      RunMetrics
}

// ReplayFieldRun is the V3 FIELD entry point for one real run. It consumes the
// external trace of the frozen outbound status_file plus the frozen server
// journal, binds them explicitly, then returns the correlated per-window replay.
// It does not add or require any runtime collector or runtime field.
func ReplayFieldRun(statusTrace io.Reader, serverJournal io.Reader, binding FieldRunBinding) (FieldRunReplay, error) {
	var result FieldRunReplay
	result.Binding = binding
	if binding.ClientNodeTag == "" || binding.ServerInstance == "" {
		return result, fmt.Errorf("FIELD replay requires explicit client node tag and server instance binding")
	}
	snapshots, err := ReadStatusTrace(statusTrace)
	if err != nil {
		return result, fmt.Errorf("status trace: %w", err)
	}
	if len(snapshots) < 2 {
		return result, fmt.Errorf("status trace requires at least two snapshots")
	}
	for _, snapshot := range snapshots {
		if snapshot.Node.Tag != binding.ClientNodeTag {
			return result, fmt.Errorf("%w: status node.tag=%q want=%q", ErrInvalidStatus, snapshot.Node.Tag, binding.ClientNodeTag)
		}
	}
	journal, err := AnalyzeJournalForInstance(serverJournal, binding.ServerInstance)
	if err != nil {
		return result, fmt.Errorf("server journal: %w", err)
	}
	if !journalCompleteForV3(journal) {
		return result, fmt.Errorf("server journal lacks complete CAP_WINDOW evidence for instance %q", binding.ServerInstance)
	}
	timeline, err := BuildTimeline(snapshots)
	if err != nil {
		return result, fmt.Errorf("status timeline: %w", err)
	}
	if err := AttachServerCapacityEvidenceForInstance(&timeline, journal, binding.ServerInstance); err != nil {
		return result, fmt.Errorf("server CAP correlation: %w", err)
	}
	correlatedCapacityWindows := 0
	for _, window := range timeline.Windows {
		if len(window.ServerCapacityEvidence) > 0 {
			correlatedCapacityWindows++
		}
	}
	if correlatedCapacityWindows == 0 {
		return result, fmt.Errorf("server CAP evidence for instance %q does not overlap the client status trace", binding.ServerInstance)
	}
	run, err := AnalyzeRun(timeline.Windows)
	if err != nil {
		return result, fmt.Errorf("FIELD run: %w", err)
	}
	result.Timeline = timeline
	result.Journal = journal
	result.Run = run
	return result, nil
}

// WriteFieldRunReplayJSON serializes the complete correlated offline replay.
// It is intentionally a pure offline sink: the raw FIELD inputs remain the
// evidence source, while this report contains the typed status snapshots,
// exact CAP/runtime events, per-window correlations, and derived run metrics.
func WriteFieldRunReplayJSON(writer io.Writer, replay FieldRunReplay) error {
	if writer == nil {
		return fmt.Errorf("nil FIELD replay report writer")
	}
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(replay); err != nil {
		return fmt.Errorf("encode FIELD replay report: %w", err)
	}
	return nil
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
		if journal.Provided && !journalCompleteForV3(journal) {
			closure.Blocking = append(closure.Blocking, "journal evidence is incomplete, empty, lacks server CAP_WINDOW evidence, or contains dropped/sequence-gap events")
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

	journalComplete := journalCompleteForV3(journal)
	if !journalComplete {
		closure.Blocking = append(closure.Blocking, "journal evidence must contain recognized complete server CAP_WINDOW evidence with zero dropped events and zero sequence gaps")
	}
	if !reflect.DeepEqual(journal, report.Evidence.Journal) {
		closure.Blocking = append(closure.Blocking, "closure journal does not match the journal embedded in the analyzed report")
	}
	if report.Load != LoadSaturating || !report.Demand.Verified || !report.Demand.Saturated || report.Demand.Method == "" {
		closure.Blocking = append(closure.Blocking, "saturating demand is not independently verified")
	}

	// Exact scheduler internals remain OPEN by design. V3 needs only the
	// operational chain already exposed by frozen evidence: server CAP preferred
	// assignment plus fresh peer leg1 sender output in the same status window.
	if report.Aggregate.SenderOutputEvidenceOverlapWindowRatio > 0 {
		closure.SchedulerAllocation = AnswerPartial
	} else if report.Aggregate.ServerPreferredAssignmentEvidenceRatio > 0 || report.Aggregate.RemoteSenderEvidenceWindowRatio > 0 || journal.ServerCapacityWindows > 0 {
		closure.SchedulerAllocation = AnswerPartial
		closure.Blocking = append(closure.Blocking, "need same-window server preferred-assignment CAP evidence and fresh peer leg1 sender-TX evidence")
	} else {
		closure.Blocking = append(closure.Blocking, "scheduler-output evidence is missing")
	}

	if closure.PerLegDelivery == AnswerFull {
		if report.Comparison.DegradedWindowCount == 0 {
			if report.Aggregate.SenderOutputEvidenceOverlapWindowRatio > 0 {
				closure.DegradationLayer = AnswerFull
			} else {
				closure.DegradationLayer = AnswerPartial
				closure.Blocking = append(closure.Blocking, "no same-window sender-output/path-delivery evidence across the aggregate run")
			}
		} else if report.Comparison.DegradationEvidenceCoverageRatio >= 1-1e-12 {
			closure.DegradationLayer = AnswerFull
		} else {
			closure.DegradationLayer = AnswerPartial
			closure.Blocking = append(closure.Blocking, "one or more degraded windows lack same-window sender-output/path-delivery or queue/resource evidence")
		}
	} else if closure.PerLegDelivery != AnswerMissing {
		closure.DegradationLayer = AnswerPartial
		closure.Blocking = append(closure.Blocking, "degradation layer cannot fully close without both-leg physical delivery")
	} else {
		closure.Blocking = append(closure.Blocking, "degradation layer cannot be localized without per-leg delivery")
	}

	currentFingerprint, fingerprintErr := reportFingerprint(*report)
	if fingerprintErr != nil {
		closure.Blocking = append(closure.Blocking, "unable to bind algorithm A/B evidence to the current report")
	} else if len(algorithmComparisons) == 1 {
		comparison := algorithmComparisons[0]
		bound := comparison.Comparable && comparison.AlgorithmA != "" && comparison.AlgorithmB != "" && comparison.AlgorithmA != comparison.AlgorithmB && comparison.WorkloadID != "" &&
			(comparison.ReportAFingerprint == currentFingerprint || comparison.ReportBFingerprint == currentFingerprint)
		if bound {
			closure.AlgorithmABComparable = AnswerFull
		} else {
			closure.Blocking = append(closure.Blocking, "algorithm A/B result is not bound to the report being accepted")
		}
	} else {
		closure.Blocking = append(closure.Blocking, "missing comparable algorithm A/B runs for the same verified workload")
	}

	closure.Pass = journalComplete && reflect.DeepEqual(journal, report.Evidence.Journal) && report.Load == LoadSaturating && report.Demand.Verified && report.Demand.Saturated && report.Demand.Method != "" &&
		closure.Baseline == AnswerFull && closure.AggregateUseful == AnswerFull &&
		closure.PerLegDelivery == AnswerFull && closure.SchedulerAllocation != AnswerMissing &&
		closure.Stability == AnswerFull && closure.Efficiency == AnswerFull &&
		closure.DegradationLayer == AnswerFull && closure.AlgorithmABComparable == AnswerFull && len(closure.Blocking) == 0
	return closure
}

func (c DiagnosticClosure) Error() error {
	if c.Pass {
		return nil
	}
	return fmt.Errorf("diagnostic closure failed: %v", c.Blocking)
}
