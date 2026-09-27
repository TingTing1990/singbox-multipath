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
	Epoch                int
	Start                time.Time
	End                  time.Time
	Duration             time.Duration
	UsefulRXBytes        uint64
	LegRXBytes           [2]uint64
	UsefulMbps           float64
	LegMbps              [2]float64
	PhysicalMbps         float64
	PathLogicalGapMbps   float64
	BoosterSenderTXMbps  float64
	RemoteSenderEvidence bool

	// LocalPreferredAssignedMbps is the status node's own preferred-capacity
	// controller. For a client-side download trace this belongs to the opposite
	// (client -> server) direction and must never satisfy server download
	// assignment evidence.
	LocalPreferredAssignedMbps       float64
	LocalPreferredAssignmentEvidence bool

	// Server preferred assignment/delivery are correlated offline from existing
	// server CAP_WINDOW evidence. No runtime field or collector is added.
	ServerPreferredAssignedMbps       float64
	ServerPreferredDeliveryMbps       float64
	ServerPreferredAssignmentEvidence bool
	ServerPreferredDeliveryEvidence   bool
	ServerCapacityInstance            string
	ServerControllerWindowSeq         uint64
	ServerCapacityEventCount          int

	LogicalState       string
	LogicalConnections int
	Active             bool
	Diagnostics        WindowDiagnostics
	ResolutionDegraded bool
}

type Timeline struct {
	Windows                    []Window
	Epochs                     int
	StatusSnapshots            int
	SourceNodeTag              string
	TemporalResolutionDegraded bool
}

func diagnosticsFromSnapshot(snapshot StatusSnapshot) WindowDiagnostics {
	logical := snapshot.Node.Logical
	d := WindowDiagnostics{
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
		useful, err := counterDelta("logical tcp rx", previousView.LogicalRX, currentView.LogicalRX)
		if err != nil {
			return result, err
		}
		var legBytes [2]uint64
		for leg := range legBytes {
			legBytes[leg], err = counterDelta(fmt.Sprintf("leg%d tcp rx", leg), previousView.LegRX[leg], currentView.LegRX[leg])
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
			UsefulRXBytes:      useful,
			LegRXBytes:         legBytes,
			LogicalState:       current.Node.Logical.State,
			LogicalConnections: current.Node.Logical.Connections,
			Active:             previous.Node.Logical.Connections > 0 || current.Node.Logical.Connections > 0,
			Diagnostics:        diagnosticsFromSnapshot(current),
		}
		window.UsefulMbps = float64(useful) * 8 / seconds / 1_000_000
		for leg := range window.LegMbps {
			window.LegMbps[leg] = float64(legBytes[leg]) * 8 / seconds / 1_000_000
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
	if timeline == nil || len(timeline.Windows) == 0 {
		return nil
	}
	instances := make(map[string]struct{})
	for _, evidence := range journal.CapacityEvidence {
		if !evidence.ValidServerEvidence() {
			continue
		}
		instances[evidence.Instance] = struct{}{}
	}
	if len(instances) > 1 {
		return fmt.Errorf("%w: multiple server CAP instances cannot be correlated to one status trace", ErrInvalidStatus)
	}

	for windowIndex := range timeline.Windows {
		window := &timeline.Windows[windowIndex]
		var assignmentSum, deliverySum float64
		var count int
		var instance string
		var lastSeq uint64
		for _, evidence := range journal.CapacityEvidence {
			if !evidence.ValidServerEvidence() || !evidence.At.After(window.Start) || evidence.At.After(window.End) {
				continue
			}
			assignmentSum += evidence.PreferredAssignmentMbps
			deliverySum += evidence.DeliveryMbps
			count++
			instance = evidence.Instance
			if evidence.ControllerWindowSeq > lastSeq {
				lastSeq = evidence.ControllerWindowSeq
			}
		}
		if count == 0 {
			continue
		}
		window.ServerPreferredAssignedMbps = assignmentSum / float64(count)
		window.ServerPreferredDeliveryMbps = deliverySum / float64(count)
		window.ServerPreferredAssignmentEvidence = true
		window.ServerPreferredDeliveryEvidence = true
		window.ServerCapacityInstance = instance
		window.ServerControllerWindowSeq = lastSeq
		window.ServerCapacityEventCount = count
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
