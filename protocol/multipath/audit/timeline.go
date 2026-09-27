package audit

import (
	"fmt"
	"time"
)

const expectedStatusInterval = time.Second

type PathDiagnostics struct {
	BacklogBytes         int64
	WritingBytes         int64
	WriteBlockedMS       int64
	RemoteBacklogBytes   int64
	RemoteWritingBytes   int64
	RemoteWriteBlockedMS int64

	SchedulerRateEstimateBytesPS uint64
	SchedulerRTTMaxMS            uint64
	SchedulerMinimumRTTMS        uint64
	SchedulerPipelineBytes       uint64

	RTTLatestMS  float64
	RTTEWMAMS    float64
	RTTMinMS     float64
	RTTMaxMS     float64
	RTTJitterMS  float64
	RTTSamples   uint64
	ProbeTimeout uint64
}

type WindowDiagnostics struct {
	// Full frozen status evidence is retained here so V3 does not discard fields
	// that already exist in status_file. The legacy scalar fields below remain for
	// existing diagnosis code and compatibility.
	Memory       MemoryStatus
	Logical      LogicalStatus
	LocalSender  SenderDiagnostics
	RemoteSender SenderDiagnostics
	LegStatus    [2]LegStatus

	LocalMemoryPressure  bool
	LocalMemoryUsedBytes int64
	ReorderBytes         int64
	ReplayBytes          int64

	RemoteSenderAvailable bool
	RemoteSenderStale     bool
	RemoteMemoryPressure  bool
	RemoteMemoryUsedBytes uint64
	RemoteReplayBytes     int64
	RemoteReplayTimeouts  uint64

	Leg [2]PathDiagnostics
}

type Window struct {
	Epoch    int
	Start    time.Time
	End      time.Time
	Duration time.Duration

	// TCP traffic is derived from frozen cumulative counters with UDP removed.
	UsefulTXBytes      uint64
	UsefulRXBytes      uint64
	LegTXBytes         [2]uint64
	LegRXBytes         [2]uint64
	UsefulTXMbps       float64
	UsefulMbps         float64
	LegTXMbps          [2]float64
	LegMbps            [2]float64
	PhysicalMbps       float64
	PathLogicalGapMbps float64

	BoosterSenderTXMbps  float64
	RemoteSenderEvidence bool

	// LocalPreferredAssignedMbps is the status node's own preferred-capacity
	// controller. For a client-side download trace this belongs to the opposite
	// (client -> server) direction and must never satisfy server download
	// assignment evidence.
	LocalPreferredAssignedMbps       float64
	LocalPreferredAssignmentEvidence bool

	// Existing frozen server CAP_WINDOW evidence. Mean assignment/delivery are
	// retained for existing aggregation metrics; the exact per-event values and
	// all other CAP state are retained below for forensic replay.
	ServerPreferredAssignedMbps       float64
	ServerPreferredDeliveryMbps       float64
	ServerPreferredAssignmentEvidence bool
	ServerPreferredDeliveryEvidence   bool
	ServerCapacityInstance            string
	ServerControllerWindowSeq         uint64
	ServerCapacityEventCount          int
	ServerCapacityCoverageDuration    time.Duration
	ServerCapacityCoverageRatio       float64
	ServerTargetMbps                  float64
	ServerDeliveryReady               bool
	ServerProtectedMbps               float64
	ServerProtectionValid             bool
	ServerProtectionActive            bool
	ServerDegradeWindows              int
	ServerBacklogBytes                int64
	ServerQueueBytes                  int64
	ServerCapacityEvidence            []CapacityWindowEvidence
	ServerCAPEvents                   []CapacityState
	ServerLatestCAPEvent              *CapacityState
	ServerCAPEventCount               int
	RuntimeEvents                     []Event

	LogicalState       string
	LogicalConnections int
	Active             bool
	Parameters         ParametersStatus
	Recovery           *RecoveryStatus
	NodeAggregation    string
	UDPOutbound        string
	TCPFastOpen        bool
	Diagnostics        WindowDiagnostics
	ResolutionDegraded bool
}

func intervalOverlap(startA, endA, startB, endB time.Time) time.Duration {
	start := startA
	if startB.After(start) {
		start = startB
	}
	end := endA
	if endB.Before(end) {
		end = endB
	}
	if !end.After(start) {
		return 0
	}
	return end.Sub(start)
}

func resetServerCapacityCorrelation(window *Window) {
	window.ServerPreferredAssignedMbps = 0
	window.ServerPreferredDeliveryMbps = 0
	window.ServerPreferredAssignmentEvidence = false
	window.ServerPreferredDeliveryEvidence = false
	window.ServerCapacityInstance = ""
	window.ServerControllerWindowSeq = 0
	window.ServerCapacityEventCount = 0
	window.ServerCapacityCoverageDuration = 0
	window.ServerCapacityCoverageRatio = 0
	window.ServerTargetMbps = 0
	window.ServerDeliveryReady = false
	window.ServerProtectedMbps = 0
	window.ServerProtectionValid = false
	window.ServerProtectionActive = false
	window.ServerDegradeWindows = 0
	window.ServerBacklogBytes = 0
	window.ServerQueueBytes = 0
	window.ServerCapacityEvidence = nil
	window.ServerCAPEvents = nil
	window.ServerLatestCAPEvent = nil
	window.ServerCAPEventCount = 0
	window.RuntimeEvents = nil
}

type Timeline struct {
	Windows                    []Window
	Snapshots                  []StatusSnapshot
	Epochs                     int
	StatusSnapshots            int
	SourceNodeTag              string
	TemporalResolutionDegraded bool
}

func diagnosticsFromSnapshot(snapshot StatusSnapshot) WindowDiagnostics {
	logical := snapshot.Node.Logical
	d := WindowDiagnostics{
		Memory:                snapshot.Node.Memory,
		Logical:               logical,
		LocalSender:           logical.LocalSender,
		RemoteSender:          logical.RemoteSender,
		LocalMemoryPressure:   snapshot.Node.Memory.Pressure,
		LocalMemoryUsedBytes:  snapshot.Node.Memory.UsedBytes,
		ReorderBytes:          logical.ReorderBytes,
		ReplayBytes:           logical.ReplayBytes,
		RemoteSenderAvailable: logical.RemoteSender.Available,
		RemoteSenderStale:     logical.RemoteSender.Stale,
		RemoteMemoryPressure:  logical.RemoteSender.MemoryPressure,
		RemoteMemoryUsedBytes: logical.RemoteSender.MemoryUsedBytes,
		RemoteReplayBytes:     logical.RemoteSender.ReplayBytes,
		RemoteReplayTimeouts:  logical.RemoteSender.ReplayTimeouts,
	}
	for index := range d.Leg {
		leg := snapshot.Node.Legs[index]
		d.LegStatus[index] = leg
		d.Leg[index] = PathDiagnostics{
			BacklogBytes:                 leg.BacklogBytes,
			WritingBytes:                 leg.WritingBytes,
			WriteBlockedMS:               leg.WriteBlockedMS,
			RemoteBacklogBytes:           leg.RemoteBacklogBytes,
			RemoteWritingBytes:           leg.RemoteWritingBytes,
			RemoteWriteBlockedMS:         leg.RemoteWriteBlockedMS,
			SchedulerRateEstimateBytesPS: leg.RemoteDeliveryRate,
			SchedulerRTTMaxMS:            leg.RemoteDeliveryRTT,
			SchedulerMinimumRTTMS:        leg.RemoteMinimumRTT,
			SchedulerPipelineBytes:       leg.RemotePipeline,
			RTTLatestMS:                  leg.RTTLatestMS,
			RTTEWMAMS:                    leg.RTTEWMAMS,
			RTTMinMS:                     leg.RTTMinMS,
			RTTMaxMS:                     leg.RTTMaxMS,
			RTTJitterMS:                  leg.RTTJitterMS,
			RTTSamples:                   leg.RTTSamples,
			ProbeTimeout:                 leg.ProbeTimeout,
		}
	}
	return d
}

func counterDelta(name string, previous, current uint64) (uint64, error) {
	if current < previous {
		return 0, fmt.Errorf("%w: %s previous=%d current=%d", ErrCounterRegression, name, previous, current)
	}
	return current - previous, nil
}

func BuildTimeline(snapshots []StatusSnapshot) (Timeline, error) {
	var result Timeline
	result.StatusSnapshots = len(snapshots)
	result.Snapshots = append([]StatusSnapshot(nil), snapshots...)
	if len(snapshots) == 0 {
		return result, nil
	}
	result.SourceNodeTag = snapshots[0].Node.Tag
	if result.SourceNodeTag == "" {
		return result, fmt.Errorf("%w: empty node.tag", ErrInvalidStatus)
	}
	var previous *StatusSnapshot
	epoch := -1
	for index := range snapshots {
		current := snapshots[index]
		if current.Node.Tag != result.SourceNodeTag {
			return result, fmt.Errorf("%w: mixed node.tag in one trace: %q then %q", ErrInvalidStatus, result.SourceNodeTag, current.Node.Tag)
		}
		if previous == nil || !current.ProcessStartedAt.Equal(previous.ProcessStartedAt) {
			epoch++
			result.Epochs++
			previous = &current
			continue
		}
		if !current.GeneratedAt.After(previous.GeneratedAt) {
			return result, fmt.Errorf("%w: generated_at not increasing: %s then %s", ErrInvalidStatus, previous.GeneratedAt, current.GeneratedAt)
		}
		previousView, err := previous.TCPView()
		if err != nil {
			return result, err
		}
		currentView, err := current.TCPView()
		if err != nil {
			return result, err
		}
		usefulTX, err := counterDelta("logical tcp tx", previousView.LogicalTX, currentView.LogicalTX)
		if err != nil {
			return result, err
		}
		usefulRX, err := counterDelta("logical tcp rx", previousView.LogicalRX, currentView.LogicalRX)
		if err != nil {
			return result, err
		}
		var legTXBytes, legRXBytes [2]uint64
		for leg := range legRXBytes {
			legTXBytes[leg], err = counterDelta(fmt.Sprintf("leg%d tcp tx", leg), previousView.LegTX[leg], currentView.LegTX[leg])
			if err != nil {
				return result, err
			}
			legRXBytes[leg], err = counterDelta(fmt.Sprintf("leg%d tcp rx", leg), previousView.LegRX[leg], currentView.LegRX[leg])
			if err != nil {
				return result, err
			}
		}
		duration := current.GeneratedAt.Sub(previous.GeneratedAt)
		if duration <= 0 {
			return result, fmt.Errorf("%w: non-positive status interval", ErrInvalidStatus)
		}
		seconds := duration.Seconds()
		window := Window{
			Epoch:              epoch,
			Start:              previous.GeneratedAt,
			End:                current.GeneratedAt,
			Duration:           duration,
			UsefulTXBytes:      usefulTX,
			UsefulRXBytes:      usefulRX,
			LegTXBytes:         legTXBytes,
			LegRXBytes:         legRXBytes,
			LogicalState:       current.Node.Logical.State,
			LogicalConnections: current.Node.Logical.Connections,
			Active:             previous.Node.Logical.Connections > 0 || current.Node.Logical.Connections > 0,
			Parameters:         current.Node.Parameters,
			Recovery:           current.Node.Recovery,
			NodeAggregation:    current.Node.Aggregation,
			UDPOutbound:        current.Node.UDPOutbound,
			TCPFastOpen:        current.Node.TCPFastOpen,
			Diagnostics:        diagnosticsFromSnapshot(current),
		}
		window.UsefulTXMbps = float64(usefulTX) * 8 / seconds / 1_000_000
		window.UsefulMbps = float64(usefulRX) * 8 / seconds / 1_000_000
		for leg := range window.LegMbps {
			window.LegTXMbps[leg] = float64(legTXBytes[leg]) * 8 / seconds / 1_000_000
			window.LegMbps[leg] = float64(legRXBytes[leg]) * 8 / seconds / 1_000_000
		}
		window.PhysicalMbps = window.LegMbps[0] + window.LegMbps[1]
		window.PathLogicalGapMbps = window.PhysicalMbps - window.UsefulMbps

		if previous.Node.Logical.PreferredCapacity != nil && current.Node.Logical.PreferredCapacity != nil {
			assigned, assignmentErr := counterDelta("local preferred assigned bytes",
				previous.Node.Logical.PreferredCapacity.PreferredAssignedBytes,
				current.Node.Logical.PreferredCapacity.PreferredAssignedBytes)
			if assignmentErr != nil {
				return result, assignmentErr
			}
			window.LocalPreferredAssignedMbps = float64(assigned) * 8 / seconds / 1_000_000
			window.LocalPreferredAssignmentEvidence = true
		}

		// For a client-side download trace, frozen remote-sender telemetry exposes
		// cumulative peer leg1 DATA actually transmitted. Require two consecutive
		// fresh samples so a stale/first-visible counter is never turned into a
		// false one-window rate. This is sender-transmission evidence, not an exact
		// scheduler assignment reason.
		if previous.Node.Logical.RemoteSender.Available && !previous.Node.Logical.RemoteSender.Stale &&
			current.Node.Logical.RemoteSender.Available && !current.Node.Logical.RemoteSender.Stale {
			boosterTX, boosterErr := counterDelta("remote sender leg1 tx bytes",
				previous.Node.Logical.RemoteSender.Leg1TXBytes, current.Node.Logical.RemoteSender.Leg1TXBytes)
			if boosterErr != nil {
				return result, boosterErr
			}
			window.BoosterSenderTXMbps = float64(boosterTX) * 8 / seconds / 1_000_000
			window.RemoteSenderEvidence = true
		}
		// The frozen producer writes once per second. A gap larger than 1.5s means
		// at least one one-second sample was not preserved by the external capture.
		window.ResolutionDegraded = duration > expectedStatusInterval+expectedStatusInterval/2
		if window.ResolutionDegraded {
			result.TemporalResolutionDegraded = true
		}
		result.Windows = append(result.Windows, window)
		previous = &current
	}
	return result, nil
}

// AttachServerCapacityEvidence correlates existing frozen server CAP_WINDOW
// evidence to client status windows. CAP evidence is accepted only from one
// explicit server instance with a timestamp; evidence from another side is not
// re-labeled as download assignment.
func AttachServerCapacityEvidence(timeline *Timeline, journal JournalReport) error {
	instances := make(map[string]struct{})
	for _, evidence := range journal.CapacityEvidence {
		if evidence.ValidServerEvidence() {
			instances[evidence.Instance] = struct{}{}
		}
	}
	if len(instances) > 1 {
		return fmt.Errorf("%w: multiple server CAP instances cannot be correlated to one status trace without an explicit binding", ErrInvalidStatus)
	}
	instance := ""
	for value := range instances {
		instance = value
	}
	return AttachServerCapacityEvidenceForInstance(timeline, journal, instance)
}

// AttachServerCapacityEvidenceForInstance binds a client/outbound status trace to
// one explicit frozen server/inbound instance. This is required in production
// journals that contain more than one multipath inbound; it prevents evidence
// from another instance being merged into the replay.
func AttachServerCapacityEvidenceForInstance(timeline *Timeline, journal JournalReport, serverInstance string) error {
	if timeline == nil || len(timeline.Windows) == 0 {
		return nil
	}
	if serverInstance == "" {
		return fmt.Errorf("%w: missing server CAP instance binding", ErrInvalidStatus)
	}
	// Association is replace-not-append. Rebinding a reusable offline timeline
	// must never retain evidence from the previously selected server instance.
	for windowIndex := range timeline.Windows {
		resetServerCapacityCorrelation(&timeline.Windows[windowIndex])
	}
	for windowIndex := range timeline.Windows {
		window := &timeline.Windows[windowIndex]
		var assignmentWeighted, deliveryWeighted float64
		var coveredDuration time.Duration
		var latestWindow *CapacityWindowEvidence
		var latestEvent *CapacityState
		for evidenceIndex := range journal.CapacityEvidence {
			evidence := journal.CapacityEvidence[evidenceIndex]
			if !evidence.ValidServerInterval() || evidence.Instance != serverInstance {
				continue
			}
			overlap := intervalOverlap(window.Start, window.End, evidence.IntervalStart, evidence.IntervalEnd)
			if overlap <= 0 {
				continue
			}
			window.ServerCapacityEvidence = append(window.ServerCapacityEvidence, evidence)
			seconds := overlap.Seconds()
			assignmentWeighted += evidence.PreferredAssignmentMbps * seconds
			deliveryWeighted += evidence.DeliveryMbps * seconds
			coveredDuration += overlap
			if latestWindow == nil || evidence.At.After(latestWindow.At) || (evidence.At.Equal(latestWindow.At) && evidence.ControllerWindowSeq > latestWindow.ControllerWindowSeq) {
				copy := evidence
				latestWindow = &copy
			}
		}
		if coveredDuration > window.Duration {
			return fmt.Errorf("%w: overlapping server CAP intervals exceed client window duration for instance %q", ErrInvalidStatus, serverInstance)
		}
		for eventIndex := range journal.CapacityEvents {
			event := journal.CapacityEvents[eventIndex]
			if event.Side != "server" || event.Instance != serverInstance || event.At.IsZero() || !event.At.After(window.Start) || event.At.After(window.End) {
				continue
			}
			window.ServerCAPEvents = append(window.ServerCAPEvents, event)
			if latestEvent == nil || event.At.After(latestEvent.At) || (event.At.Equal(latestEvent.At) && event.EventSeq > latestEvent.EventSeq) {
				copy := event
				latestEvent = &copy
			}
		}
		for eventIndex := range journal.Events {
			event := journal.Events[eventIndex]
			if event.ObservedAt.IsZero() || !event.ObservedAt.After(window.Start) || event.ObservedAt.After(window.End) {
				continue
			}
			window.RuntimeEvents = append(window.RuntimeEvents, event)
		}
		if coveredDuration > 0 && latestWindow != nil {
			seconds := coveredDuration.Seconds()
			window.ServerPreferredAssignedMbps = assignmentWeighted / seconds
			window.ServerPreferredDeliveryMbps = deliveryWeighted / seconds
			window.ServerPreferredAssignmentEvidence = true
			window.ServerPreferredDeliveryEvidence = true
			window.ServerCapacityInstance = serverInstance
			window.ServerCapacityEventCount = len(window.ServerCapacityEvidence)
			window.ServerCapacityCoverageDuration = coveredDuration
			window.ServerCapacityCoverageRatio = float64(coveredDuration) / float64(window.Duration)
		}
		if latestEvent != nil {
			copy := *latestEvent
			window.ServerLatestCAPEvent = &copy
			window.ServerCAPEventCount = len(window.ServerCAPEvents)
			window.ServerCapacityInstance = serverInstance
			window.ServerControllerWindowSeq = latestEvent.ControllerWindowSeq
			window.ServerTargetMbps = latestEvent.TargetMbps
			window.ServerDeliveryReady = latestEvent.DeliveryReady
			window.ServerProtectedMbps = latestEvent.ProtectedMbps
			window.ServerProtectionValid = latestEvent.ProtectionValid
			window.ServerProtectionActive = latestEvent.ProtectionActive
			window.ServerDegradeWindows = latestEvent.DegradeWindows
			window.ServerBacklogBytes = latestEvent.BacklogBytes
			window.ServerQueueBytes = latestEvent.QueueBytes
		}
	}
	return nil
}

func ActiveEnvelope(windows []Window) ([]Window, error) {
	first := -1
	last := -1
	for index, window := range windows {
		hasTraffic := window.UsefulRXBytes > 0 || window.LegRXBytes[0] > 0 || window.LegRXBytes[1] > 0
		if hasTraffic && first < 0 {
			first = index
		}
		if first >= 0 && (hasTraffic || window.Active) {
			last = index
		}
	}
	if first < 0 {
		return nil, ErrNoActivity
	}
	if last < first {
		last = first
	}
	return append([]Window(nil), windows[first:last+1]...), nil
}
