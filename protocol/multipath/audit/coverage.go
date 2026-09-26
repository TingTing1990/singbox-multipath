package audit

func Coverage() []Capability {
	return []Capability{
		{
			Type:     EventCapacityState,
			Support:  SupportFull,
			Evidence: "CAP_AUDIT events emitted by the frozen Server Audit v1 controller/logger path",
		},
		{
			Type:     EventMemoryState,
			Support:  SupportPartial,
			Evidence: "existing server logs expose budget/runtime-limit and pressure enter/clear transitions, but not an instance-correlated periodic full memory snapshot",
		},
		{
			Type:     EventSessionLifecycle,
			Support:  SupportPartial,
			Evidence: "existing server logs expose establishment and protocol errors, but ordinary lifecycle lines do not carry instance/session ids and there is no close event",
		},
		{
			Type:     EventPathState,
			Support:  SupportPartial,
			Evidence: "existing server logs expose leg attachment and leg1 data-path activation, but not instance/session correlation or scheduler rate/RTT/outstanding/busy/stale state",
		},
		{
			Type:               EventSchedulerDecision,
			Support:            SupportOpen,
			Evidence:           "the frozen source emits no per-decision scheduler evidence",
			RequiresFrozenHook: true,
		},
		{
			Type:               EventAggregateWindow,
			Support:            SupportOpen,
			Evidence:           "CAP_WINDOW is preferred-capacity evidence, not an aggregate two-leg throughput window",
			RequiresFrozenHook: true,
		},
	}
}
