package audit

import "time"

type CapacityState struct {
	SchemaVersion            int
	EventSeq                 uint64
	Event                    string
	Side                     string
	Instance                 string
	SessionID                string
	Destination              string
	At                       time.Time
	OriginalTrigger          string
	OriginalTriggerSatisfied bool
	RecoveryBypass           bool
	TargetMbps               float64
	DeliveryMbps             float64
	DeliveryReady            bool
	ProtectedMbps            float64
	ProtectionValid          bool
	ProtectionActive         bool
	PreferredAssignmentMbps  float64
	DegradeWindows           int
	NormalBoosterAdmitted    bool
	CurrentBytes             uint64
	ThresholdBytes           uint64
	TriggerWindowBytes       uint64
	TriggerRateMbps          float64
	TriggerThresholdMbps     float64
	BacklogBytes             int64
	QueueBytes               int64
	OldProtectedMbps         float64
	NewProtectedMbps         float64
	ChangeReason             string
	ControllerWindowSeq      uint64
	DroppedEvents            uint64
	EvidenceComplete         bool
}

type MemoryEventKind string

const (
	MemoryRuntimeLimit    MemoryEventKind = "runtime_limit"
	MemoryBudget          MemoryEventKind = "budget"
	MemoryPressureEntered MemoryEventKind = "pressure_entered"
	MemoryPressureCleared MemoryEventKind = "pressure_cleared"
)

type MemoryState struct {
	Kind            MemoryEventKind
	Side            string
	Source          string
	Scope           string
	LimitBytes      uint64
	HighBytes       uint64
	ResumeBytes     uint64
	CacheLimitBytes uint64
	UsedBytes       uint64
	Pressure        bool
	Duration        time.Duration
}

type SessionState string

const (
	SessionEstablished   SessionState = "established"
	SessionProtocolError SessionState = "protocol_error"
)

type SessionLifecycle struct {
	State       SessionState
	Side        string
	SessionID   string
	Destination string
	LegID       int
	Error       string
}

type PathStateKind string

const (
	PathAttached   PathStateKind = "attached"
	PathDataActive PathStateKind = "data_active"
)

type PathState struct {
	State       PathStateKind
	Side        string
	SessionID   string
	Destination string
	LegID       int
	Reconnect   bool
	Trigger     string
}

type SchedulerDecision struct {
	SelectedLeg int
	Reason      string
}

type AggregateWindow struct {
	LogicalMbps float64
	LegMbps     [2]float64
}

type Capability struct {
	Type               EventType
	Support            SupportLevel
	Evidence           string
	RequiresFrozenHook bool
}
