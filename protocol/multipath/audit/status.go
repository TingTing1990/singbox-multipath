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

type PeakRate struct {
	TXBytesPS uint64 `json:"tx_bytes_per_second"`
	RXBytesPS uint64 `json:"rx_bytes_per_second"`
	TXAt      string `json:"tx_at,omitempty"`
	RXAt      string `json:"rx_at,omitempty"`
}

type Frames struct {
	TX uint64 `json:"tx"`
	RX uint64 `json:"rx"`
}

type ActivationStatus struct {
	Reason                         string `json:"reason"`
	At                             string `json:"at"`
	CurrentBytes                   uint64 `json:"current_bytes,omitempty"`
	ThresholdBytes                 uint64 `json:"threshold_bytes,omitempty"`
	WindowBytes                    uint64 `json:"window_bytes,omitempty"`
	RateBytesPS                    uint64 `json:"rate_bytes_per_second,omitempty"`
	ThresholdBytesPS               uint64 `json:"threshold_bytes_per_second,omitempty"`
	MinRateBytesPS                 uint64 `json:"min_rate_bytes_per_second,omitempty"`
	ElapsedMS                      int64  `json:"elapsed_ms,omitempty"`
	BacklogBytes                   int64  `json:"backlog_bytes,omitempty"`
	QueueBytes                     int64  `json:"queue_bytes,omitempty"`
	RequiredDurationMS             int64  `json:"required_duration_ms,omitempty"`
	PreferredCapacityTargetMbps    uint64 `json:"preferred_capacity_target_mbps,omitempty"`
	PreferredCapacityRateBytesPS   uint64 `json:"preferred_capacity_rate_bytes_per_second,omitempty"`
	PreferredCapacityProtectedMbps uint64 `json:"preferred_capacity_protected_mbps,omitempty"`
}

type ParametersStatus struct {
	PreferredCapacityMbps       uint64 `json:"preferred_capacity_mbps,omitempty"`
	AggregationEnabled          bool   `json:"aggregation_enabled"`
	ActivationOnQueue           bool   `json:"activation_on_queue"`
	ActivationThresholdMbps     uint64 `json:"activation_threshold_mbps"`
	ActivationAfterBytes        uint64 `json:"activation_after_bytes"`
	ActivationAfterBytesMinMbps uint64 `json:"activation_after_bytes_min_mbps"`
	ActivationWindowMS          int64  `json:"activation_window_ms"`
	ChunkSize                   int    `json:"chunk_size"`
	QueueFrames                 int    `json:"queue_frames"`
	QueueBytes                  int64  `json:"queue_bytes"`
	MaxReorderFrames            int    `json:"max_reorder_frames"`
	MaxReorderBytes             int64  `json:"max_reorder_bytes"`
	Leg1ReplayBytes             int64  `json:"leg1_replay_bytes"`
	Leg1ReplayTimeoutMS         int64  `json:"leg1_replay_timeout_ms"`
	MemoryLimitBytes            int64  `json:"memory_limit_bytes"`
	HandshakeTimeoutMS          int64  `json:"handshake_timeout_ms"`
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
	State                    string                   `json:"state"`
	Connections              int                      `json:"connections"`
	ConnectionsTotal         uint64                   `json:"connections_total"`
	PreferredOnlyConnections int                      `json:"preferred_only_connections"`
	TXAggregatingConnections int                      `json:"tx_aggregating_connections"`
	RXAggregatingConnections int                      `json:"rx_aggregating_connections"`
	BoosterDegraded          int                      `json:"booster_degraded_connections"`
	Current                  Rate                     `json:"current"`
	Peak                     PeakRate                 `json:"peak"`
	Cumulative               Traffic                  `json:"cumulative"`
	ReplayBytes              int64                    `json:"replay_bytes"`
	ReorderBytes             int64                    `json:"reorder_bytes"`
	ReorderPages             int64                    `json:"reorder_pages"`
	ReorderPeakBytes         int64                    `json:"reorder_peak_bytes"`
	ReorderPeakPages         int64                    `json:"reorder_peak_pages"`
	LocalSender              SenderDiagnostics        `json:"local_sender"`
	RemoteSender             SenderDiagnostics        `json:"remote_sender"`
	PreferredCapacity        *PreferredCapacityStatus `json:"preferred_capacity,omitempty"`
	LastActivation           *ActivationStatus        `json:"last_activation,omitempty"`
}

type MemoryStatus struct {
	LimitBytes         int64  `json:"limit_bytes"`
	UsedBytes          int64  `json:"used_bytes"`
	CachedBytes        int64  `json:"cached_bytes"`
	BoosterLimitBytes  int64  `json:"booster_limit_bytes"`
	BoosterResumeBytes int64  `json:"booster_resume_bytes"`
	Automatic          bool   `json:"automatic"`
	Pressure           bool   `json:"pressure"`
	PressureSince      string `json:"pressure_since,omitempty"`
	PressureEvents     uint64 `json:"pressure_events"`
	BackpressureEvents uint64 `json:"backpressure_events"`
	PeakUsedBytes      int64  `json:"peak_used_bytes"`
	PeakCachedBytes    int64  `json:"peak_cached_bytes"`
}

type FlowStatus struct {
	SessionID    string  `json:"session_id"`
	Destination  string  `json:"destination"`
	StartedAt    string  `json:"started_at"`
	AgeSeconds   int64   `json:"age_seconds"`
	State        string  `json:"state"`
	Current      Rate    `json:"current"`
	Cumulative   Traffic `json:"cumulative"`
	BacklogBytes int64   `json:"backlog_bytes"`
}

type LegStatus struct {
	RemoteDeliveryRate      uint64       `json:"remote_delivery_bytes_per_second"`
	RemoteDeliveryRTT       uint64       `json:"remote_delivery_rtt_max_ms"`
	RemoteMinimumRTT        uint64       `json:"remote_delivery_rtt_min_ms"`
	RemotePipeline          uint64       `json:"remote_pipeline_bytes"`
	ID                      int          `json:"id"`
	Tag                     string       `json:"tag"`
	Type                    string       `json:"type"`
	Role                    string       `json:"role"`
	State                   string       `json:"state"`
	Connections             int          `json:"connections"`
	CarryingConnections     int          `json:"carrying_connections"`
	StandbyConnections      int          `json:"standby_connections"`
	ConnectingConnections   int          `json:"connecting_connections"`
	RetryingConnections     int          `json:"retrying_connections"`
	Current                 Rate         `json:"current"`
	Peak                    PeakRate     `json:"peak"`
	Cumulative              Traffic      `json:"cumulative"`
	Frames                  Frames       `json:"frames"`
	BacklogBytes            int64        `json:"backlog_bytes"`
	WritingBytes            int64        `json:"writing_bytes"`
	WriteBlockedMS          int64        `json:"write_blocked_ms"`
	PeakBacklogBytes        int64        `json:"peak_backlog_bytes"`
	RemoteBacklogBytes      int64        `json:"remote_backlog_bytes"`
	RemoteWritingBytes      int64        `json:"remote_writing_bytes"`
	RemoteWriteBlockedMS    int64        `json:"remote_write_blocked_ms"`
	RemotePeakBacklogBytes  int64        `json:"remote_peak_backlog_bytes"`
	QueueBytesPerConnection int64        `json:"queue_bytes_per_connection"`
	JoinCount               uint64       `json:"join_count"`
	AttemptCount            uint64       `json:"attempt_count"`
	UDPSelected             bool         `json:"udp_selected"`
	UDPCurrent              Rate         `json:"udp_current"`
	UDPCumulative           Traffic      `json:"udp_cumulative"`
	LastError               string       `json:"last_error,omitempty"`
	LastErrorSource         string       `json:"last_error_source,omitempty"`
	LastErrorAt             string       `json:"last_error_at,omitempty"`
	LastErrorCategory       string       `json:"last_error_category,omitempty"`
	LastErrorStage          string       `json:"last_error_stage,omitempty"`
	LastErrorDestination    string       `json:"last_error_destination,omitempty"`
	LastErrorSessionID      string       `json:"last_error_session_id,omitempty"`
	LastErrorAttempt        uint64       `json:"last_error_attempt,omitempty"`
	ErrorCount              uint64       `json:"error_count"`
	RemoteFailureCount      uint64       `json:"remote_failure_count"`
	RemoteLastFailureStage  string       `json:"remote_last_failure_stage,omitempty"`
	RTTLatestMS             float64      `json:"rtt_latest_ms"`
	RTTAverageMS            float64      `json:"rtt_average_ms"`
	RTTEWMAMS               float64      `json:"rtt_ewma_ms"`
	RTTMinMS                float64      `json:"rtt_min_ms"`
	RTTMaxMS                float64      `json:"rtt_max_ms"`
	RTTJitterMS             float64      `json:"rtt_jitter_ms"`
	RTTSamples              uint64       `json:"rtt_samples"`
	ProbeSent               uint64       `json:"probe_sent"`
	ProbeTimeout            uint64       `json:"probe_timeout"`
	LastErrorTransient      bool         `json:"last_error_transient"`
	LastErrorHarmless       bool         `json:"last_error_harmless"`
	TopFlows                []FlowStatus `json:"top_flows"`
}

type RecoveryPathStatus struct {
	Healthy  bool  `json:"healthy"`
	TCPAgeMS int64 `json:"tcp_reply_age_ms"`
	UDPAgeMS int64 `json:"udp_reply_age_ms"`
	StableMS int64 `json:"stable_ms"`
}

type RecoveryStatus struct {
	Enabled           bool                  `json:"enabled"`
	FailoverTimeoutMS int64                 `json:"failover_timeout_ms"`
	FailbackDelayMS   int64                 `json:"failback_delay_ms"`
	TCPPath           byte                  `json:"tcp_path"`
	UDPPath           byte                  `json:"udp_path"`
	UDPPreferred      byte                  `json:"udp_preferred"`
	UsableMask        byte                  `json:"usable_mask"`
	Paths             [2]RecoveryPathStatus `json:"paths"`
}

type StatusNode struct {
	Recovery    *RecoveryStatus  `json:"recovery,omitempty"`
	Tag         string           `json:"tag"`
	Type        string           `json:"type"`
	Aggregation string           `json:"aggregation_server"`
	UDPOutbound string           `json:"udp_outbound"`
	TCPFastOpen bool             `json:"tcp_fast_open"`
	Parameters  ParametersStatus `json:"parameters"`
	Memory      MemoryStatus     `json:"memory"`
	Logical     LogicalStatus    `json:"logical"`
	Legs        []LegStatus      `json:"legs"`
}

type StatusSnapshot struct {
	SchemaVersion     int        `json:"schema_version"`
	GeneratedAtRaw    string     `json:"generated_at"`
	ProcessStartedRaw string     `json:"process_started_at"`
	Node              StatusNode `json:"node"`

	GeneratedAt      time.Time       `json:"-"`
	ProcessStartedAt time.Time       `json:"-"`
	Raw              json.RawMessage `json:"-"`
}

type TCPView struct {
	LogicalTX uint64
	LogicalRX uint64
	LegTX     [2]uint64
	LegRX     [2]uint64
}

func (s StatusSnapshot) TCPView() (TCPView, error) {
	var view TCPView
	if len(s.Node.Legs) != 2 {
		return view, fmt.Errorf("%w: node.legs count=%d want=2", ErrInvalidStatus, len(s.Node.Legs))
	}
	udpLogicalTX := s.Node.Legs[0].UDPCumulative.TXBytes + s.Node.Legs[1].UDPCumulative.TXBytes
	udpLogicalRX := s.Node.Legs[0].UDPCumulative.RXBytes + s.Node.Legs[1].UDPCumulative.RXBytes
	if s.Node.Logical.Cumulative.TXBytes < udpLogicalTX {
		return view, errCounterUnderflow("logical cumulative tx", s.Node.Logical.Cumulative.TXBytes, udpLogicalTX)
	}
	if s.Node.Logical.Cumulative.RXBytes < udpLogicalRX {
		return view, errCounterUnderflow("logical cumulative rx", s.Node.Logical.Cumulative.RXBytes, udpLogicalRX)
	}
	view.LogicalTX = s.Node.Logical.Cumulative.TXBytes - udpLogicalTX
	view.LogicalRX = s.Node.Logical.Cumulative.RXBytes - udpLogicalRX
	for index := range view.LegRX {
		leg := s.Node.Legs[index]
		if leg.Cumulative.TXBytes < leg.UDPCumulative.TXBytes {
			return TCPView{}, errCounterUnderflow("leg cumulative tx", leg.Cumulative.TXBytes, leg.UDPCumulative.TXBytes)
		}
		if leg.Cumulative.RXBytes < leg.UDPCumulative.RXBytes {
			return TCPView{}, errCounterUnderflow("leg cumulative rx", leg.Cumulative.RXBytes, leg.UDPCumulative.RXBytes)
		}
		view.LegTX[index] = leg.Cumulative.TXBytes - leg.UDPCumulative.TXBytes
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

func requirePresentField(object map[string]json.RawMessage, name string) (json.RawMessage, error) {
	value, ok := object[name]
	if !ok || len(bytes.TrimSpace(value)) == 0 {
		return nil, fmt.Errorf("%w: missing %s", ErrInvalidStatus, name)
	}
	return value, nil
}

func requireObjectField(object map[string]json.RawMessage, name string) (json.RawMessage, error) {
	value, err := requirePresentField(object, name)
	if err != nil {
		return nil, err
	}
	if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
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
	if err := requireFields(root["node"], "node", "tag", "type", "aggregation_server", "udp_outbound", "tcp_fast_open", "parameters", "memory", "logical", "legs"); err != nil {
		return StatusSnapshot{}, err
	}
	var nodeRaw map[string]json.RawMessage
	if err := json.Unmarshal(root["node"], &nodeRaw); err != nil {
		return StatusSnapshot{}, fmt.Errorf("%w: node: %v", ErrInvalidStatus, err)
	}
	if err := requireFields(nodeRaw["parameters"], "node.parameters",
		"aggregation_enabled", "activation_on_queue", "activation_threshold_mbps", "activation_after_bytes", "activation_after_bytes_min_mbps",
		"activation_window_ms", "chunk_size", "queue_frames", "queue_bytes", "max_reorder_frames", "max_reorder_bytes",
		"leg1_replay_bytes", "leg1_replay_timeout_ms", "memory_limit_bytes", "handshake_timeout_ms"); err != nil {
		return StatusSnapshot{}, err
	}
	if err := requireFields(nodeRaw["logical"], "node.logical",
		"state", "connections", "connections_total", "preferred_only_connections", "tx_aggregating_connections", "rx_aggregating_connections", "booster_degraded_connections",
		"current", "peak", "cumulative", "replay_bytes", "reorder_bytes", "reorder_pages", "reorder_peak_bytes", "reorder_peak_pages", "local_sender", "remote_sender"); err != nil {
		return StatusSnapshot{}, err
	}
	var logicalRaw map[string]json.RawMessage
	if err := json.Unmarshal(nodeRaw["logical"], &logicalRaw); err != nil {
		return StatusSnapshot{}, fmt.Errorf("%w: node.logical: %v", ErrInvalidStatus, err)
	}
	if err := requireFields(logicalRaw["current"], "node.logical.current", "tx_bytes_per_second", "rx_bytes_per_second"); err != nil {
		return StatusSnapshot{}, err
	}
	if err := requireFields(logicalRaw["peak"], "node.logical.peak", "tx_bytes_per_second", "rx_bytes_per_second"); err != nil {
		return StatusSnapshot{}, err
	}
	if err := requireFields(logicalRaw["cumulative"], "node.logical.cumulative", "tx_bytes", "rx_bytes"); err != nil {
		return StatusSnapshot{}, err
	}
	for _, senderName := range []string{"local_sender", "remote_sender"} {
		if err := requireFields(logicalRaw[senderName], "node.logical."+senderName,
			"available", "stale", "stale_connections", "replay_bytes", "replay_peak_bytes", "fallback_bytes", "fallback_frames", "fallback_events", "leg1_tx_bytes", "replay_timeouts",
			"backpressure_events", "backpressure_duration_ms", "memory_pressure", "memory_used_bytes", "memory_peak_used_bytes", "memory_pressure_events", "memory_backpressure_events"); err != nil {
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
	if raw, ok := logicalRaw["last_activation"]; ok && len(bytes.TrimSpace(raw)) > 0 && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		if err := requireFields(raw, "node.logical.last_activation", "reason", "at"); err != nil {
			return StatusSnapshot{}, err
		}
	}
	if err := requireFields(nodeRaw["memory"], "node.memory", "limit_bytes", "used_bytes", "cached_bytes", "booster_limit_bytes", "booster_resume_bytes", "automatic", "pressure", "pressure_events", "backpressure_events", "peak_used_bytes", "peak_cached_bytes"); err != nil {
		return StatusSnapshot{}, err
	}
	if recoveryRaw, ok := nodeRaw["recovery"]; ok && len(bytes.TrimSpace(recoveryRaw)) > 0 && !bytes.Equal(bytes.TrimSpace(recoveryRaw), []byte("null")) {
		if err := requireFields(recoveryRaw, "node.recovery", "enabled", "failover_timeout_ms", "failback_delay_ms", "tcp_path", "udp_path", "udp_preferred", "usable_mask", "paths"); err != nil {
			return StatusSnapshot{}, err
		}
		var recoveryObject map[string]json.RawMessage
		if err := json.Unmarshal(recoveryRaw, &recoveryObject); err != nil {
			return StatusSnapshot{}, fmt.Errorf("%w: node.recovery: %v", ErrInvalidStatus, err)
		}
		var recoveryPaths []json.RawMessage
		if err := json.Unmarshal(recoveryObject["paths"], &recoveryPaths); err != nil || len(recoveryPaths) != 2 {
			return StatusSnapshot{}, fmt.Errorf("%w: node.recovery.paths must contain two paths", ErrInvalidStatus)
		}
		for index, pathRaw := range recoveryPaths {
			if err := requireFields(pathRaw, "node.recovery.paths["+strconv.Itoa(index)+"]", "healthy", "tcp_reply_age_ms", "udp_reply_age_ms", "stable_ms"); err != nil {
				return StatusSnapshot{}, err
			}
		}
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
			"remote_delivery_bytes_per_second", "remote_delivery_rtt_max_ms", "remote_delivery_rtt_min_ms", "remote_pipeline_bytes",
			"id", "tag", "type", "role", "state", "connections", "carrying_connections", "standby_connections", "connecting_connections", "retrying_connections",
			"current", "peak", "cumulative", "frames", "backlog_bytes", "writing_bytes", "write_blocked_ms", "peak_backlog_bytes",
			"remote_backlog_bytes", "remote_writing_bytes", "remote_write_blocked_ms", "remote_peak_backlog_bytes", "queue_bytes_per_connection",
			"join_count", "attempt_count", "udp_selected", "udp_current", "udp_cumulative", "error_count", "remote_failure_count",
			"rtt_latest_ms", "rtt_average_ms", "rtt_ewma_ms", "rtt_min_ms", "rtt_max_ms", "rtt_jitter_ms", "rtt_samples", "probe_sent", "probe_timeout",
			"last_error_transient", "last_error_harmless"); err != nil {
			return StatusSnapshot{}, err
		}
		var legObject map[string]json.RawMessage
		if err := json.Unmarshal(legRaw, &legObject); err != nil {
			return StatusSnapshot{}, fmt.Errorf("%w: node.legs[%d]: %v", ErrInvalidStatus, index, err)
		}
		for _, rateName := range []string{"current", "udp_current"} {
			if err := requireFields(legObject[rateName], "node.legs["+strconv.Itoa(index)+"]."+rateName, "tx_bytes_per_second", "rx_bytes_per_second"); err != nil {
				return StatusSnapshot{}, err
			}
		}
		if err := requireFields(legObject["peak"], "node.legs["+strconv.Itoa(index)+"].peak", "tx_bytes_per_second", "rx_bytes_per_second"); err != nil {
			return StatusSnapshot{}, err
		}
		for _, counterName := range []string{"cumulative", "udp_cumulative"} {
			if err := requireFields(legObject[counterName], "node.legs["+strconv.Itoa(index)+"]."+counterName, "tx_bytes", "rx_bytes"); err != nil {
				return StatusSnapshot{}, err
			}
		}
		if err := requireFields(legObject["frames"], "node.legs["+strconv.Itoa(index)+"].frames", "tx", "rx"); err != nil {
			return StatusSnapshot{}, err
		}
		topFlowsRaw, err := requirePresentField(legObject, "top_flows")
		if err != nil {
			return StatusSnapshot{}, fmt.Errorf("node.legs[%d]: %w", index, err)
		}
		var flowsRaw []json.RawMessage
		if !bytes.Equal(bytes.TrimSpace(topFlowsRaw), []byte("null")) {
			if err := json.Unmarshal(topFlowsRaw, &flowsRaw); err != nil {
				return StatusSnapshot{}, fmt.Errorf("%w: node.legs[%d].top_flows: %v", ErrInvalidStatus, index, err)
			}
		}
		for flowIndex, flowRaw := range flowsRaw {
			if err := requireFields(flowRaw, "node.legs["+strconv.Itoa(index)+"].top_flows["+strconv.Itoa(flowIndex)+"]", "session_id", "destination", "started_at", "age_seconds", "state", "current", "cumulative", "backlog_bytes"); err != nil {
				return StatusSnapshot{}, err
			}
			var flowObject map[string]json.RawMessage
			if err := json.Unmarshal(flowRaw, &flowObject); err != nil {
				return StatusSnapshot{}, fmt.Errorf("%w: node.legs[%d].top_flows[%d]: %v", ErrInvalidStatus, index, flowIndex, err)
			}
			if err := requireFields(flowObject["current"], "flow.current", "tx_bytes_per_second", "rx_bytes_per_second"); err != nil {
				return StatusSnapshot{}, err
			}
			if err := requireFields(flowObject["cumulative"], "flow.cumulative", "tx_bytes", "rx_bytes"); err != nil {
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
	snapshot.Raw = append(json.RawMessage(nil), data...)
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
