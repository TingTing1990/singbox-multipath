package audit

import "time"

type Event struct {
	Type       EventType
	ObservedAt time.Time
	Raw        string

	Capacity *CapacityState
	Memory   *MemoryState
	Session  *SessionLifecycle
	Path     *PathState
}
