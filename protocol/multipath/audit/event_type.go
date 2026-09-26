package audit

type EventType string

const (
	EventCapacityState     EventType = "CAPACITY_STATE"
	EventMemoryState       EventType = "MEMORY_STATE"
	EventSessionLifecycle  EventType = "SESSION_LIFECYCLE"
	EventPathState         EventType = "PATH_STATE"
	EventSchedulerDecision EventType = "SCHEDULER_DECISION"
	EventAggregateWindow   EventType = "AGGREGATE_WINDOW"
)

type SupportLevel string

const (
	SupportFull    SupportLevel = "FULL"
	SupportPartial SupportLevel = "PARTIAL"
	SupportOpen    SupportLevel = "OPEN"
)
