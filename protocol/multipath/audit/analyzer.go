package audit

import "fmt"

type Report struct {
	RecognizedEvents uint64
	ByType           map[EventType]uint64
	CapacityEvents   map[string]uint64
	EvidenceComplete bool
	DroppedEvents    uint64
	SequenceGaps     uint64
	SequenceEpochs   uint64
	ValidationErrors []string
	LastCapacity     *CapacityState
	LastMemory       *MemoryState
}

type Analyzer struct {
	report       Report
	lastEventSeq uint64
	seenEventSeq bool
}

func NewAnalyzer() *Analyzer {
	return &Analyzer{report: Report{
		ByType:           make(map[EventType]uint64),
		CapacityEvents:   make(map[string]uint64),
		EvidenceComplete: true,
	}}
}

func (a *Analyzer) Consume(event Event) {
	if a == nil {
		return
	}
	a.report.RecognizedEvents++
	a.report.ByType[event.Type]++
	switch event.Type {
	case EventCapacityState:
		if event.Capacity == nil {
			a.report.EvidenceComplete = false
			a.report.ValidationErrors = append(a.report.ValidationErrors, "CAPACITY_STATE missing payload")
			return
		}
		state := *event.Capacity
		a.report.CapacityEvents[state.Event]++
		if state.Event == "CAP_AUDIT_DROPPED" {
			a.report.EvidenceComplete = false
			a.report.DroppedEvents += state.DroppedEvents
		} else if state.EventSeq > 0 {
			if state.Event == "CAP_AUDIT_READY" && state.EventSeq == 1 {
				if a.seenEventSeq {
					a.report.SequenceEpochs++
				}
				a.lastEventSeq = state.EventSeq
				a.seenEventSeq = true
			} else {
				if a.seenEventSeq && state.EventSeq != a.lastEventSeq+1 {
					a.report.EvidenceComplete = false
					a.report.SequenceGaps++
				}
				a.lastEventSeq = state.EventSeq
				a.seenEventSeq = true
			}
		}
		if err := ValidateCapacity(state); err != nil {
			a.report.EvidenceComplete = false
			a.report.ValidationErrors = append(a.report.ValidationErrors, err.Error())
		}
		a.report.LastCapacity = &state
	case EventMemoryState:
		if event.Memory != nil {
			state := *event.Memory
			a.report.LastMemory = &state
		}
	}
}

func (a *Analyzer) Report() Report {
	if a == nil {
		return Report{}
	}
	result := a.report
	result.ByType = cloneEventCounts(a.report.ByType)
	result.CapacityEvents = cloneStringCounts(a.report.CapacityEvents)
	result.ValidationErrors = append([]string(nil), a.report.ValidationErrors...)
	if a.report.LastCapacity != nil {
		copy := *a.report.LastCapacity
		result.LastCapacity = &copy
	}
	if a.report.LastMemory != nil {
		copy := *a.report.LastMemory
		result.LastMemory = &copy
	}
	return result
}

func ValidateCapacity(state CapacityState) error {
	switch state.Event {
	case "CAP_AUDIT_READY", "CAP_WINDOW", "CAP_PROTECTION_CHANGED":
		return nil
	case "CAP_AUDIT_DROPPED":
		if state.DroppedEvents == 0 || state.EvidenceComplete {
			return fmt.Errorf("CAP_AUDIT_DROPPED requires dropped_events>0 and evidence_complete=false")
		}
		return nil
	case "CAP_GATE_BLOCKED":
		if !state.OriginalTriggerSatisfied || state.DeliveryReady || state.NormalBoosterAdmitted || state.RecoveryBypass {
			return fmt.Errorf("CAP_GATE_BLOCKED inconsistent with frozen admission semantics")
		}
		return nil
	case "CAP_GATE_OPENED":
		if !state.OriginalTriggerSatisfied || !state.DeliveryReady {
			return fmt.Errorf("CAP_GATE_OPENED lacks satisfied trigger/readiness evidence")
		}
		if state.RecoveryBypass {
			if state.NormalBoosterAdmitted {
				return fmt.Errorf("recovery bypass cannot be reported as normal booster admission")
			}
			return nil
		}
		if !state.NormalBoosterAdmitted {
			return fmt.Errorf("normal CAP_GATE_OPENED must report normal booster admission")
		}
		return nil
	case "CAP_PROTECTION_ARMED":
		if state.RecoveryBypass || !state.ProtectionActive || !state.ProtectionValid || state.ProtectedMbps <= 0 {
			return fmt.Errorf("CAP_PROTECTION_ARMED lacks valid active non-recovery protection")
		}
		return nil
	default:
		return fmt.Errorf("unknown CAP_AUDIT event %q", state.Event)
	}
}

func cloneEventCounts(source map[EventType]uint64) map[EventType]uint64 {
	clone := make(map[EventType]uint64, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func cloneStringCounts(source map[string]uint64) map[string]uint64 {
	clone := make(map[string]uint64, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}
