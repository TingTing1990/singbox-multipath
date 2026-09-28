// Package downloadevidence defines server download observation, not wire data.
package downloadevidence

import (
	"encoding/json"
	"time"
)

const Version = 1
const Marker = "MP_DOWNLOAD_AUDIT "

// Header identity is scoped to one inbound object. MonoNS is elapsed local
// monotonic time, not a client receipt timestamp. Seq includes dropped records.
type Header struct {
	Schema      int       `json:"schema"`
	Epoch       string    `json:"epoch"`
	Instance    string    `json:"instance"`
	Side        string    `json:"side"`
	Direction   string    `json:"direction"`
	Seq         uint64    `json:"seq"`
	At          time.Time `json:"at"`
	MonoNS      int64     `json:"mono_ns"`
	Dropped     uint64    `json:"dropped"`
	Kind        string    `json:"kind"`
	Session     string    `json:"session,omitempty"`
	Destination string    `json:"destination,omitempty"`
}

type Envelope struct {
	Header
	Data json.RawMessage `json:"data"`
}

func Encode(h Header, data any) ([]byte, error) {
	return json.Marshal(struct {
		Header
		Data any `json:"data"`
	}{h, data})
}

type Config struct {
	ObservationQueueRecords        int
	ObservationQueueBytes          int64
	Aggregation                    bool
	ActivationOnQueue              bool
	ChunkSize                      int
	QueueFrames                    int
	QueueBytes                     int64
	ThresholdBytesPS               uint64
	ActivationAfterBytes           uint64
	ActivationAfterBytesMinBytesPS uint64
	ActivationWindowNS             int64
	MaxReorderFrames               int
	MaxReorderBytes                int64
	ReplayBytes                    int64
	ReplayTimeoutNS                int64
	CapacityTargetBytesPS          uint64
}

type Path struct {
	Evaluated                        bool // True only when the actual scheduler visited this path.
	Present, Ready, Busy, Stale      bool
	Generation                       uint64
	RateBytesPS                      float64
	SRTTNS, RTTVarNS, MinRTTNS       int64
	Outstanding, Pipeline            uint64
	DrainSeconds                     float64
	Backlog, Writing, WriteBlockedNS int64
	Excluded                         string
}

type Capacity struct {
	Enabled                                          bool
	TargetBytesPS, DeliveryBytesPS, ProtectedBytesPS uint64
	Ready, ProtectionActive, ProtectionValid         bool
	CreditBytes                                      int64
	WindowSeq                                        uint64
}

type Decision struct {
	Length               int
	LogicalSeq           uint64
	Candidate, Selected  int
	Outcome              string
	Reason               string
	Repair               bool
	Active, PeerPressure bool
	MemoryAllowed        bool
	Paths                [2]Path
	Capacity             Capacity
}

type Totals struct {
	AssignedNormal                                  [2]uint64
	AssignedRepair                                  [2]uint64
	WrittenNormal                                   [2]uint64
	WrittenRepair                                   [2]uint64
	ConfirmedPath                                   [2]uint64
	ConfirmedLogical                                uint64
	Decisions, Waits, SubmitFailures, WriteFailures uint64
	SessionsStarted, SessionsClosed                 uint64
}

type Memory struct {
	Limit, Used, Cached, BoosterLimit, Resume, PeakUsed, PeakCached int64
	Pressure                                                        bool
	PressureEvents, BackpressureEvents                              uint64
}

type Snapshot struct {
	Capacity Capacity
	Totals   Totals
	Memory   Memory
	Config   Config
}
type Transfer struct {
	Leg        int
	Generation uint64
	Bytes      uint64
	Repair     bool
	LogicalSeq uint64
	Outcome    string
}
type Feedback struct {
	LogicalBytes uint64
	PathBytes    [2]uint64
	Generation   [2]uint64
}
type Session struct {
	Config Config
	Reason string
}
