package option

import (
	"github.com/sagernet/sing/common/byteformats"
	"github.com/sagernet/sing/common/json"
	"github.com/sagernet/sing/common/json/badoption"
)

// MultipathOutboundOptions combines multiple existing reliable outbounds into
// one logical TCP byte stream. UDP is intentionally not aggregated and is
// delegated to UDPOutbound (or Preferred / the first child).
type MultipathOutboundOptions struct {
	Outbounds                   []string                 `json:"outbounds" reference:"outbound"`
	Preferred                   string                   `json:"preferred,omitempty" reference:"outbound"`
	UDPOutbound                 string                   `json:"udp_outbound,omitempty" reference:"outbound"`
	Server                      string                   `json:"server"`
	ServerPort                  uint16                   `json:"server_port"`
	TCPFastOpen                 bool                     `json:"tcp_fast_open,omitempty"`
	FailoverEnabled             bool                     `json:"failover_enabled,omitempty"`
	FailoverTimeout             badoption.Duration       `json:"failover_timeout,omitempty"`
	FailbackDelay               badoption.Duration       `json:"failback_delay,omitempty"`
	AggregationEnabled          *bool                    `json:"aggregation_enabled,omitempty"`
	ActivationOnQueue           *bool                    `json:"activation_on_queue,omitempty"`
	ActivationThresholdMbps     *uint32                  `json:"activation_threshold_mbps,omitempty"`
	ActivationAfterBytes        *byteformats.MemoryBytes `json:"activation_after_bytes,omitempty"`
	ActivationAfterBytesMinMbps uint32                   `json:"activation_after_bytes_min_mbps,omitempty"`
	ActivationWindow            badoption.Duration       `json:"activation_window,omitempty"`
	ChunkSize                   uint32                   `json:"chunk_size,omitempty"`
	QueueFrames                 uint32                   `json:"queue_frames,omitempty"`
	MaxReorderFrames            uint32                   `json:"max_reorder_frames,omitempty"`
	MaxReorderBytes             uint64                   `json:"max_reorder_bytes,omitempty"`
	Leg1ReplayBytes             uint64                   `json:"leg1_replay_bytes,omitempty"`
	Leg1ReplayTimeout           badoption.Duration       `json:"leg1_replay_timeout,omitempty"`
	MemoryLimit                 *byteformats.MemoryBytes `json:"memory_limit,omitempty"`
	HandshakeTimeout            badoption.Duration       `json:"handshake_timeout,omitempty"`
	StatusFile                  string                   `json:"status_file,omitempty"`
	// Deprecated: accepted only to warn during migration; never used by the scheduler.
	DeprecatedBandwidthMbps json.RawMessage `json:"bandwidth_mbps,omitempty"`
}

type MultipathInboundOptions struct {
	ListenOptions
	AggregationEnabled          *bool                    `json:"aggregation_enabled,omitempty"`
	ActivationOnQueue           *bool                    `json:"activation_on_queue,omitempty"`
	ActivationThresholdMbps     *uint32                  `json:"activation_threshold_mbps,omitempty"`
	ActivationAfterBytes        *byteformats.MemoryBytes `json:"activation_after_bytes,omitempty"`
	ActivationAfterBytesMinMbps uint32                   `json:"activation_after_bytes_min_mbps,omitempty"`
	ActivationWindow            badoption.Duration       `json:"activation_window,omitempty"`
	ChunkSize                   uint32                   `json:"chunk_size,omitempty"`
	QueueFrames                 uint32                   `json:"queue_frames,omitempty"`
	MaxReorderFrames            uint32                   `json:"max_reorder_frames,omitempty"`
	MaxReorderBytes             uint64                   `json:"max_reorder_bytes,omitempty"`
	Leg1ReplayBytes             uint64                   `json:"leg1_replay_bytes,omitempty"`
	Leg1ReplayTimeout           badoption.Duration       `json:"leg1_replay_timeout,omitempty"`
	MemoryLimit                 *byteformats.MemoryBytes `json:"memory_limit,omitempty"`
	HandshakeTimeout            badoption.Duration       `json:"handshake_timeout,omitempty"`
	// Deprecated: accepted only to warn during migration; never used by the scheduler.
	DeprecatedBandwidthMbps json.RawMessage `json:"bandwidth_mbps,omitempty"`
}
