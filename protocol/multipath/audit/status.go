package audit

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"
)

const StatusSchemaVersion = 3

type Traffic struct {
	TXBytes uint64 `json:"tx_bytes"`
	RXBytes uint64 `json:"rx_bytes"`
}

type Rate struct {
	TXBytesPS uint64 `json:"tx_bytes_per_second"`
	RXBytesPS uint64 `json:"rx_bytes_per_second"`
}

type SenderDiagnostics struct {
	Available                bool   `json:"available"`
	Stale                    bool   `json:"stale"`
	UpdatedAt                string `json:"updated_at,omitempty"`
	StaleConnections         int    `json:"stale_connections"`
	ReplayBytes              int64  `json:"replay_bytes"`
	ReplayPeakBytes          int64  `json:"replay_peak_bytes"`
	FallbackBytes            uint64 `json:"fallback_bytes"`
	FallbackFrames           uint64 `json:"fallback_frames"`
	FallbackEvents           uint64 `json:"fallback_events"`
	Leg1TXBytes              uint64 `json:"leg1_tx_bytes"`
	ReplayTimeouts           uint64 `json:"replay_timeouts"`
	BackpressureEvents       uint64 `json:"backpressure_events"`
	BackpressureDurationMS   uint64 `json:"backpressure_duration_ms"`
	MemoryPressure           bool   `json:"memory_pressure"`
	MemoryUsedBytes          uint64 `json:"memory_used_bytes"`
	MemoryPeakUsedBytes      uint64 `json:"memory_peak_used_bytes"`
	MemoryPressureEvents     uint64 `json:"memory_pressure_events"`
	MemoryBackpressureEvents uint64 `json:"memory_backpressure_events"`
}

type PreferredCapacityStatus struct {
	TargetMbps              uint64 `json:"target_mbps"`
	PreferredDeliveredBytes uint64 `json:"preferred_delivered_bytes"`
	PreferredAssignedBytes  uint64 `json:"preferred_assigned_bytes"`
	DeliveryBytesPS         uint64 `json:"delivery_bytes_per_second"`
	AssignmentBytesPS       uint64 `json:"assignment_bytes_per_second"`
	ProtectedMbps           uint64 `json:"protected_mbps"`
	ProtectionValid         bool   `json:"protection_valid"`
	ProtectionActive        bool   `json:"protection_active"`
	DegradeWindows          int    `json:"degrade_windows"`
	Ready                   bool   `json:"ready"`
	AssignmentCreditBytes   int64  `json:"assignment_credit_bytes"`
}

type LogicalStatus struct {
	State             string                   `json:"state"`
	Connections       int                      `json:"connections"`
	Current           Rate                     `json:"current"`
	Cumulative        Traffic                  `json:"cumulative"`
	ReplayBytes       int64                    `json:"replay_bytes"`
	ReorderBytes      int64                    `json:"reorder_bytes"`
	ReorderPages      int64                    `json:"reorder_pages"`
	LocalSender       SenderDiagnostics        `json:"local_sender"`
	RemoteSender      SenderDiagnostics        `json:"remote_sender"`
	PreferredCapacity *PreferredCapacityStatus `json:"preferred_capacity,omitempty"`
}

type MemoryStatus struct {
	LimitBytes         int64  `json:"limit_bytes"`
	UsedBytes          int64  `json:"used_bytes"`
	CachedBytes        int64  `json:"cached_bytes"`
	Pressure           bool   `json:"pressure"`
	PressureEvents     uint64 `json:"pressure_events"`
	BackpressureEvents uint64 `json:"backpressure_events"`
	PeakUsedBytes      int64  `json:"peak_used_bytes"`
}

type LegStatus struct {
	RemoteDeliveryRate   uint64  `json:"remote_delivery_bytes_per_second"`
	RemoteDeliveryRTT    uint64  `json:"remote_delivery_rtt_max_ms"`
	RemoteMinimumRTT     uint64  `json:"remote_delivery_rtt_min_ms"`
	RemotePipeline       uint64  `json:"remote_pipeline_bytes"`
	ID                   int     `json:"id"`
	Tag                  string  `json:"tag"`
	Role                 string  `json:"role"`
	State                string  `json:"state"`
	Connections          int     `json:"connections"`
	Current              Rate    `json:"current"`
	Cumulative           Traffic `json:"cumulative"`
	BacklogBytes         int64   `json:"backlog_bytes"`
	WritingBytes         int64   `json:"writing_bytes"`
	WriteBlockedMS       int64   `json:"write_blocked_ms"`
	RemoteBacklogBytes   int64   `json:"remote_backlog_bytes"`
	RemoteWritingBytes   int64   `json:"remote_writing_bytes"`
	RemoteWriteBlockedMS int64   `json:"remote_write_blocked_ms"`
	UDPCumulative        Traffic `json:"udp_cumulative"`
	RTTLatestMS          float64 `json:"rtt_latest_ms"`
	RTTAverageMS         float64 `json:"rtt_average_ms"`
	RTTEWMAMS            float64 `json:"rtt_ewma_ms"`
	RTTMinMS             float64 `json:"rtt_min_ms"`
	RTTMaxMS             float64 `json:"rtt_max_ms"`
	RTTJitterMS          float64 `json:"rtt_jitter_ms"`
	RTTSamples           uint64  `json:"rtt_samples"`
	ProbeTimeout         uint64  `json:"probe_timeout"`
}

type StatusNode struct {
	Tag     string        `json:"tag"`
	Type    string        `json:"type"`
	Memory  MemoryStatus  `json:"memory"`
	Logical LogicalStatus `json:"logical"`
	Legs    []LegStatus   `json:"legs"`
}

type StatusSnapshot struct {
	SchemaVersion     int        `json:"schema_version"`
	GeneratedAtRaw    string     `json:"generated_at"`
	ProcessStartedRaw string     `json:"process_started_at"`
	Node              StatusNode `json:"node"`

	GeneratedAt      time.Time `json:"-"`
	ProcessStartedAt time.Time `json:"-"`
}

type TCPView struct {
	LogicalRX uint64
	LegRX     [2]uint64
}

func (s StatusSnapshot) TCPView() (TCPView, error) {
	var view TCPView
	if len(s.Node.Legs) != 2 {
		return view, fmt.Errorf("%w: node.legs count=%d want=2", ErrInvalidStatus, len(s.Node.Legs))
	}
	udpLogical := s.Node.Legs[0].UDPCumulative.RXBytes + s.Node.Legs[1].UDPCumulative.RXBytes
	if s.Node.Logical.Cumulative.RXBytes < udpLogical {
		return view, errCounterUnderflow("logical cumulative rx", s.Node.Logical.Cumulative.RXBytes, udpLogical)
	}
	view.LogicalRX = s.Node.Logical.Cumulative.RXBytes - udpLogical
	for index := range view.LegRX {
		leg := s.Node.Legs[index]
		if leg.Cumulative.RXBytes < leg.UDPCumulative.RXBytes {
			return TCPView{}, errCounterUnderflow("leg cumulative rx", leg.Cumulative.RXBytes, leg.UDPCumulative.RXBytes)
		}
		view.LegRX[index] = leg.Cumulative.RXBytes - leg.UDPCumulative.RXBytes
	}
	return view, nil
}

var (
	ErrInvalidStatus          = errors.New("invalid multipath status evidence")
	ErrCounterRegression      = errors.New("cumulative counter regression")
	ErrCounterUnderflow       = errors.New("derived TCP counter underflow")
	ErrNoActivity             = errors.New("no TCP activity observed")
	ErrInvalidBaseline        = errors.New("baseline is not single-path")
	ErrInvalidTopology        = errors.New("invalid topology")
	ErrUnsupportedLoad        = errors.New("unsupported load class")
	ErrUnverifiedDemand       = errors.New("saturating load is not backed by verified demand evidence")
	ErrAlgorithmNotComparable = errors.New("algorithm runs are not comparable")
)

func errCounterUnderflow(name string, total, subtract uint64) error {
	return fmt.Errorf("%w: %s total=%d subtract=%d", ErrCounterUnderflow, name, total, subtract)
}

func requireObjectField(object map[string]json.RawMessage, name string) (json.RawMessage, error) {
	value, ok := object[name]
	if !ok || len(bytes.TrimSpace(value)) == 0 || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		return nil, fmt.Errorf("%w: missing %s", ErrInvalidStatus, name)
	}
	return value, nil
}

func requireFields(raw json.RawMessage, prefix string, names ...string) error {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return fmt.Errorf("%w: %s object: %v", ErrInvalidStatus, prefix, err)
	}
	for _, name := range names {
		if _, err := requireObjectField(object, name); err != nil {
			return fmt.Errorf("%s: %w", prefix, err)
		}
	}
	return nil
}

func ParseStatus(data []byte) (StatusSnapshot, error) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(data, &root); err != nil {
		return StatusSnapshot{}, fmt.Errorf("%w: json: %v", ErrInvalidStatus, err)
	}
	for _, name := range []string{"schema_version", "generated_at", "process_started_at", "node"} {
		if _, err := requireObjectField(root, name); err != nil {
			return StatusSnapshot{}, err
		}
	}
	if err := requireFields(root["node"], "node", "tag", "type", "memory", "logical", "legs"); err != nil {
		return StatusSnapshot{}, err
	}
	var nodeRaw map[string]json.RawMessage
	if err := json.Unmarshal(root["node"], &nodeRaw); err != nil {
		return StatusSnapshot{}, fmt.Errorf("%w: node: %v", ErrInvalidStatus, err)
	}
	if err := requireFields(nodeRaw["logical"], "node.logical", "state", "connections", "cumulative", "replay_bytes", "reorder_bytes", "reorder_pages", "local_sender", "remote_sender"); err != nil {
		return StatusSnapshot{}, err
	}
	var logicalRaw map[string]json.RawMessage
	if err := json.Unmarshal(nodeRaw["logical"], &logicalRaw); err != nil {
		return StatusSnapshot{}, fmt.Errorf("%w: node.logical: %v", ErrInvalidStatus, err)
	}
	if err := requireFields(logicalRaw["cumulative"], "node.logical.cumulative", "tx_bytes", "rx_bytes"); err != nil {
		return StatusSnapshot{}, err
	}
	for _, senderName := range []string{"local_sender", "remote_sender"} {
		if err := requireFields(logicalRaw[senderName], "node.logical."+senderName,
			"available", "stale", "leg1_tx_bytes", "replay_bytes", "replay_timeouts",
			"memory_pressure", "memory_used_bytes"); err != nil {
			return StatusSnapshot{}, err
		}
	}
	if raw, ok := logicalRaw["preferred_capacity"]; ok && len(bytes.TrimSpace(raw)) > 0 && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		if err := requireFields(raw, "node.logical.preferred_capacity",
			"target_mbps", "preferred_delivered_bytes", "preferred_assigned_bytes",
			"delivery_bytes_per_second", "assignment_bytes_per_second",
			"protected_mbps", "protection_valid", "protection_active", "degrade_windows",
			"ready", "assignment_credit_bytes"); err != nil {
			return StatusSnapshot{}, err
		}
	}
	if err := requireFields(nodeRaw["memory"], "node.memory", "limit_bytes", "used_bytes", "pressure", "pressure_events", "backpressure_events"); err != nil {
		return StatusSnapshot{}, err
	}
	var legsRaw []json.RawMessage
	if err := json.Unmarshal(nodeRaw["legs"], &legsRaw); err != nil {
		return StatusSnapshot{}, fmt.Errorf("%w: node.legs: %v", ErrInvalidStatus, err)
	}
	if len(legsRaw) != 2 {
		return StatusSnapshot{}, fmt.Errorf("%w: node.legs count=%d want=2", ErrInvalidStatus, len(legsRaw))
	}
	for index, legRaw := range legsRaw {
		if err := requireFields(legRaw, "node.legs["+strconv.Itoa(index)+"]",
			"id", "role", "current", "cumulative", "udp_cumulative", "backlog_bytes", "writing_bytes", "write_blocked_ms",
			"remote_backlog_bytes", "remote_writing_bytes", "remote_write_blocked_ms",
			"remote_delivery_bytes_per_second", "remote_delivery_rtt_max_ms", "remote_delivery_rtt_min_ms", "remote_pipeline_bytes",
			"rtt_latest_ms", "rtt_ewma_ms", "rtt_min_ms", "rtt_max_ms", "rtt_jitter_ms", "rtt_samples", "probe_timeout"); err != nil {
			return StatusSnapshot{}, err
		}
		var legObject map[string]json.RawMessage
		if err := json.Unmarshal(legRaw, &legObject); err != nil {
			return StatusSnapshot{}, fmt.Errorf("%w: node.legs[%d]: %v", ErrInvalidStatus, index, err)
		}
		for _, counterName := range []string{"cumulative", "udp_cumulative"} {
			if err := requireFields(legObject[counterName], "node.legs["+strconv.Itoa(index)+"]."+counterName, "tx_bytes", "rx_bytes"); err != nil {
				return StatusSnapshot{}, err
			}
		}
	}
	var snapshot StatusSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return StatusSnapshot{}, fmt.Errorf("%w: typed decode: %v", ErrInvalidStatus, err)
	}
	if snapshot.SchemaVersion != StatusSchemaVersion {
		return StatusSnapshot{}, fmt.Errorf("%w: schema_version=%d want=%d", ErrInvalidStatus, snapshot.SchemaVersion, StatusSchemaVersion)
	}
	if snapshot.Node.Type != "multipath" {
		return StatusSnapshot{}, fmt.Errorf("%w: node.type=%q", ErrInvalidStatus, snapshot.Node.Type)
	}
	var err error
	if snapshot.GeneratedAt, err = time.Parse(time.RFC3339Nano, snapshot.GeneratedAtRaw); err != nil {
		return StatusSnapshot{}, fmt.Errorf("%w: generated_at: %v", ErrInvalidStatus, err)
	}
	if snapshot.ProcessStartedAt, err = time.Parse(time.RFC3339Nano, snapshot.ProcessStartedRaw); err != nil {
		return StatusSnapshot{}, fmt.Errorf("%w: process_started_at: %v", ErrInvalidStatus, err)
	}
	if snapshot.GeneratedAt.Before(snapshot.ProcessStartedAt) {
		return StatusSnapshot{}, fmt.Errorf("%w: generated_at precedes process_started_at", ErrInvalidStatus)
	}
	var ordered [2]LegStatus
	seen := [2]bool{}
	for _, leg := range snapshot.Node.Legs {
		if leg.ID < 0 || leg.ID > 1 || seen[leg.ID] {
			return StatusSnapshot{}, fmt.Errorf("%w: invalid or duplicate leg id=%d", ErrInvalidStatus, leg.ID)
		}
		seen[leg.ID] = true
		ordered[leg.ID] = leg
	}
	if !seen[0] || !seen[1] {
		return StatusSnapshot{}, fmt.Errorf("%w: missing leg id", ErrInvalidStatus)
	}
	snapshot.Node.Legs = []LegStatus{ordered[0], ordered[1]}
	if _, err = snapshot.TCPView(); err != nil {
		return StatusSnapshot{}, err
	}
	return snapshot, nil
}

func ScanStatusTrace(reader io.Reader, consume func(StatusSnapshot) error) (uint64, error) {
	scanner := bufio.NewScanner(reader)
	buffer := make([]byte, 64<<10)
	scanner.Buffer(buffer, 8<<20)
	var count uint64
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		snapshot, err := ParseStatus(line)
		if err != nil {
			return count, fmt.Errorf("status trace line %d: %w", count+1, err)
		}
		count++
		if consume != nil {
			if err := consume(snapshot); err != nil {
				return count, err
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return count, err
	}
	return count, nil
}

func ReadStatusTrace(reader io.Reader) ([]StatusSnapshot, error) {
	var snapshots []StatusSnapshot
	_, err := ScanStatusTrace(reader, func(snapshot StatusSnapshot) error {
		snapshots = append(snapshots, snapshot)
		return nil
	})
	return snapshots, err
}
