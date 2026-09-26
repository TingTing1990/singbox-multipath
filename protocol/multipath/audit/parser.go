package audit

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var keyStartPattern = regexp.MustCompile(`(?:^|\s)([A-Za-z0-9_]+)=`)

var (
	sessionEstablishedPattern = regexp.MustCompile(`multipath session established to (.+) on leg ([0-9]+)$`)
	legJoinedPattern          = regexp.MustCompile(`multipath leg ([0-9]+) joined session for (.+)$`)
	protocolErrorPattern      = regexp.MustCompile(`multipath protocol error for (.+): (.+)$`)
)

func ParseLine(line string) (Event, bool, error) {
	raw := strings.TrimSpace(line)
	if raw == "" {
		return Event{}, false, nil
	}
	if index := strings.Index(raw, "CAP_AUDIT "); index >= 0 {
		capacity, err := parseCapacity(raw[index+len("CAP_AUDIT "):])
		if err != nil {
			return Event{}, true, err
		}
		observed := capacity.At
		return Event{Type: EventCapacityState, ObservedAt: observed, Raw: raw, Capacity: &capacity}, true, nil
	}
	if index := strings.Index(raw, "Go runtime soft memory limit: "); index >= 0 {
		memory, err := parseRuntimeLimit(raw[index+len("Go runtime soft memory limit: "):])
		if err != nil {
			return Event{}, true, err
		}
		return Event{Type: EventMemoryState, Raw: raw, Memory: &memory}, true, nil
	}
	if index := strings.Index(raw, "multipath memory budget: "); index >= 0 {
		memory, err := parseMemoryBudget(raw[index+len("multipath memory budget: "):])
		if err != nil {
			return Event{}, true, err
		}
		return Event{Type: EventMemoryState, Raw: raw, Memory: &memory}, true, nil
	}
	if index := strings.Index(raw, "multipath memory pressure entered: "); index >= 0 {
		memory, err := parseMemoryPressure(raw[index+len("multipath memory pressure entered: "):], true)
		if err != nil {
			return Event{}, true, err
		}
		return Event{Type: EventMemoryState, Raw: raw, Memory: &memory}, true, nil
	}
	if index := strings.Index(raw, "multipath memory pressure cleared: "); index >= 0 {
		memory, err := parseMemoryPressure(raw[index+len("multipath memory pressure cleared: "):], false)
		if err != nil {
			return Event{}, true, err
		}
		return Event{Type: EventMemoryState, Raw: raw, Memory: &memory}, true, nil
	}
	if index := strings.Index(raw, "multipath leg1 joined data path: "); index >= 0 {
		path, err := parseLeg1DataActive(raw[index+len("multipath leg1 joined data path: "):])
		if err != nil {
			return Event{}, true, err
		}
		return Event{Type: EventPathState, Raw: raw, Path: &path}, true, nil
	}
	if match := legJoinedPattern.FindStringSubmatch(raw); match != nil {
		legID, _ := strconv.Atoi(match[1])
		path := PathState{State: PathAttached, Side: "server", Destination: match[2], LegID: legID}
		return Event{Type: EventPathState, Raw: raw, Path: &path}, true, nil
	}
	if match := sessionEstablishedPattern.FindStringSubmatch(raw); match != nil {
		legID, _ := strconv.Atoi(match[2])
		session := SessionLifecycle{State: SessionEstablished, Side: "server", Destination: match[1], LegID: legID}
		return Event{Type: EventSessionLifecycle, Raw: raw, Session: &session}, true, nil
	}
	if match := protocolErrorPattern.FindStringSubmatch(raw); match != nil {
		session := SessionLifecycle{State: SessionProtocolError, Side: "server", Destination: match[1], LegID: -1, Error: match[2]}
		return Event{Type: EventSessionLifecycle, Raw: raw, Session: &session}, true, nil
	}
	return Event{}, false, nil
}

func parseCapacity(text string) (CapacityState, error) {
	fields := parseKeyValues(text)
	var state CapacityState
	var err error
	if state.SchemaVersion, err = requiredInt(fields, "schema_version"); err != nil {
		return state, err
	}
	state.Event = fields["event"]
	if state.Event == "" {
		return state, errors.New("CAP_AUDIT missing event")
	}
	state.Side = unquote(fields["side"])
	state.Instance = unquote(fields["instance"])
	state.SessionID = unquote(fields["session_id"])
	state.Destination = unquote(fields["destination"])
	state.EvidenceComplete = true
	if value, ok := fields["evidence_complete"]; ok {
		state.EvidenceComplete, err = strconv.ParseBool(value)
		if err != nil {
			return state, fieldError("evidence_complete", err)
		}
	}
	if value, ok := fields["event_seq"]; ok {
		state.EventSeq, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			return state, fieldError("event_seq", err)
		}
	}
	if value, ok := fields["at"]; ok && value != "" {
		state.At, err = time.Parse(time.RFC3339Nano, unquote(value))
		if err != nil {
			return state, fieldError("at", err)
		}
	}
	if value, ok := fields["dropped_events"]; ok {
		state.DroppedEvents, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			return state, fieldError("dropped_events", err)
		}
	}
	if state.Event == "CAP_AUDIT_DROPPED" {
		return state, nil
	}

	state.OriginalTrigger = unquote(fields["original_trigger"])
	if err = parseBoolField(fields, "original_trigger_satisfied", &state.OriginalTriggerSatisfied); err != nil {
		return state, err
	}
	if err = parseBoolField(fields, "recovery_bypass", &state.RecoveryBypass); err != nil {
		return state, err
	}
	if err = parseFloatField(fields, "target_mbps", &state.TargetMbps); err != nil {
		return state, err
	}
	if err = parseFloatField(fields, "delivery_mbps", &state.DeliveryMbps); err != nil {
		return state, err
	}
	if err = parseBoolField(fields, "delivery_ready", &state.DeliveryReady); err != nil {
		return state, err
	}
	if err = parseFloatField(fields, "protected_mbps", &state.ProtectedMbps); err != nil {
		return state, err
	}
	if err = parseBoolField(fields, "protection_valid", &state.ProtectionValid); err != nil {
		return state, err
	}
	if err = parseBoolField(fields, "protection_active", &state.ProtectionActive); err != nil {
		return state, err
	}
	if err = parseFloatField(fields, "preferred_assignment_mbps", &state.PreferredAssignmentMbps); err != nil {
		return state, err
	}
	if err = parseIntField(fields, "degrade_windows", &state.DegradeWindows); err != nil {
		return state, err
	}
	if err = parseBoolField(fields, "normal_booster_admitted", &state.NormalBoosterAdmitted); err != nil {
		return state, err
	}
	if err = parseUintField(fields, "current_bytes", &state.CurrentBytes); err != nil {
		return state, err
	}
	if err = parseUintField(fields, "threshold_bytes", &state.ThresholdBytes); err != nil {
		return state, err
	}
	if err = parseUintField(fields, "trigger_window_bytes", &state.TriggerWindowBytes); err != nil {
		return state, err
	}
	if err = parseFloatField(fields, "trigger_rate_mbps", &state.TriggerRateMbps); err != nil {
		return state, err
	}
	if err = parseFloatField(fields, "trigger_threshold_mbps", &state.TriggerThresholdMbps); err != nil {
		return state, err
	}
	if err = parseInt64Field(fields, "backlog_bytes", &state.BacklogBytes); err != nil {
		return state, err
	}
	if err = parseInt64Field(fields, "queue_bytes", &state.QueueBytes); err != nil {
		return state, err
	}
	if err = parseFloatField(fields, "old_protected_mbps", &state.OldProtectedMbps); err != nil {
		return state, err
	}
	if err = parseFloatField(fields, "new_protected_mbps", &state.NewProtectedMbps); err != nil {
		return state, err
	}
	state.ChangeReason = unquote(fields["change_reason"])
	if err = parseUintField(fields, "controller_window_seq", &state.ControllerWindowSeq); err != nil {
		return state, err
	}
	return state, nil
}

func parseRuntimeLimit(text string) (MemoryState, error) {
	fields := parseKeyValues(text)
	limit, err := parseByteSize(fields["limit"])
	if err != nil {
		return MemoryState{}, fieldError("limit", err)
	}
	return MemoryState{
		Kind:       MemoryRuntimeLimit,
		Source:     unquote(fields["source"]),
		Scope:      unquote(fields["scope"]),
		LimitBytes: limit,
	}, nil
}

func parseMemoryBudget(text string) (MemoryState, error) {
	fields := parseKeyValues(text)
	limit, err := requiredByteSize(fields, "limit")
	if err != nil {
		return MemoryState{}, err
	}
	high, err := requiredByteSize(fields, "high")
	if err != nil {
		return MemoryState{}, err
	}
	resume, err := requiredByteSize(fields, "resume")
	if err != nil {
		return MemoryState{}, err
	}
	cacheLimit, err := requiredByteSize(fields, "cache_limit")
	if err != nil {
		return MemoryState{}, err
	}
	return MemoryState{
		Kind:            MemoryBudget,
		Side:            unquote(fields["side"]),
		Source:          unquote(fields["source"]),
		LimitBytes:      limit,
		HighBytes:       high,
		ResumeBytes:     resume,
		CacheLimitBytes: cacheLimit,
	}, nil
}

func parseMemoryPressure(text string, entered bool) (MemoryState, error) {
	fields := parseKeyValues(text)
	used, err := requiredByteSize(fields, "used")
	if err != nil {
		return MemoryState{}, err
	}
	state := MemoryState{Side: unquote(fields["side"]), UsedBytes: used, Pressure: entered}
	if entered {
		state.Kind = MemoryPressureEntered
		state.HighBytes, err = requiredByteSize(fields, "high")
		if err != nil {
			return MemoryState{}, err
		}
		state.LimitBytes, err = requiredByteSize(fields, "limit")
		if err != nil {
			return MemoryState{}, err
		}
		return state, nil
	}
	state.Kind = MemoryPressureCleared
	state.ResumeBytes, err = requiredByteSize(fields, "resume")
	if err != nil {
		return MemoryState{}, err
	}
	if duration := fields["duration"]; duration != "" {
		state.Duration, err = time.ParseDuration(unquote(duration))
		if err != nil {
			return MemoryState{}, fieldError("duration", err)
		}
	}
	return state, nil
}

func parseLeg1DataActive(text string) (PathState, error) {
	fields := parseKeyValues(text)
	state := PathState{
		State:       PathDataActive,
		Side:        unquote(fields["side"]),
		Destination: unquote(fields["destination"]),
		LegID:       1,
		Trigger:     unquote(fields["reason"]),
	}
	if state.Side == "" || state.Destination == "" {
		return PathState{}, errors.New("leg1 data-path log missing side or destination")
	}
	if value, ok := fields["reconnect"]; ok {
		reconnect, err := strconv.ParseBool(value)
		if err != nil {
			return PathState{}, fieldError("reconnect", err)
		}
		state.Reconnect = reconnect
	}
	return state, nil
}

func Scan(reader io.Reader, consume func(Event) error) (recognized uint64, ignored uint64, err error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		event, ok, parseErr := ParseLine(scanner.Text())
		if parseErr != nil {
			return recognized, ignored, parseErr
		}
		if !ok {
			ignored++
			continue
		}
		recognized++
		if consume != nil {
			if consumeErr := consume(event); consumeErr != nil {
				return recognized, ignored, consumeErr
			}
		}
	}
	if scanErr := scanner.Err(); scanErr != nil {
		return recognized, ignored, scanErr
	}
	return recognized, ignored, nil
}

func parseKeyValues(text string) map[string]string {
	matches := keyStartPattern.FindAllStringSubmatchIndex(text, -1)
	fields := make(map[string]string, len(matches))
	for index, match := range matches {
		key := text[match[2]:match[3]]
		valueStart := match[1]
		valueEnd := len(text)
		if index+1 < len(matches) {
			valueEnd = matches[index+1][0]
		}
		fields[key] = strings.TrimSpace(text[valueStart:valueEnd])
	}
	return fields
}

func unquote(value string) string {
	value = strings.TrimSpace(value)
	if len(value) >= 2 && value[0] == '"' {
		if decoded, err := strconv.Unquote(value); err == nil {
			return decoded
		}
	}
	return value
}

func requiredInt(fields map[string]string, key string) (int, error) {
	value, ok := fields[key]
	if !ok || value == "" {
		return 0, fmt.Errorf("missing %s", key)
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fieldError(key, err)
	}
	return parsed, nil
}

func parseBoolField(fields map[string]string, key string, target *bool) error {
	value, ok := fields[key]
	if !ok {
		return fmt.Errorf("missing %s", key)
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fieldError(key, err)
	}
	*target = parsed
	return nil
}

func parseFloatField(fields map[string]string, key string, target *float64) error {
	value, ok := fields[key]
	if !ok {
		return fmt.Errorf("missing %s", key)
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fieldError(key, err)
	}
	*target = parsed
	return nil
}

func parseIntField(fields map[string]string, key string, target *int) error {
	value, ok := fields[key]
	if !ok {
		return fmt.Errorf("missing %s", key)
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fieldError(key, err)
	}
	*target = parsed
	return nil
}

func parseUintField(fields map[string]string, key string, target *uint64) error {
	value, ok := fields[key]
	if !ok {
		return fmt.Errorf("missing %s", key)
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return fieldError(key, err)
	}
	*target = parsed
	return nil
}

func parseInt64Field(fields map[string]string, key string, target *int64) error {
	value, ok := fields[key]
	if !ok {
		return fmt.Errorf("missing %s", key)
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fieldError(key, err)
	}
	*target = parsed
	return nil
}

func requiredByteSize(fields map[string]string, key string) (uint64, error) {
	value, ok := fields[key]
	if !ok || value == "" {
		return 0, fmt.Errorf("missing %s", key)
	}
	parsed, err := parseByteSize(value)
	if err != nil {
		return 0, fieldError(key, err)
	}
	return parsed, nil
}

func parseByteSize(value string) (uint64, error) {
	parts := strings.Fields(unquote(value))
	if len(parts) == 0 || len(parts) > 2 {
		return 0, fmt.Errorf("invalid byte size %q", value)
	}
	number, err := strconv.ParseFloat(parts[0], 64)
	if err != nil || number < 0 {
		return 0, fmt.Errorf("invalid byte size %q", value)
	}
	multiplier := float64(1)
	if len(parts) == 2 {
		switch parts[1] {
		case "B":
		case "KiB":
			multiplier = 1 << 10
		case "MiB":
			multiplier = 1 << 20
		case "GiB":
			multiplier = 1 << 30
		case "TiB":
			multiplier = 1 << 40
		case "kB", "KB":
			multiplier = 1_000
		case "MB":
			multiplier = 1_000_000
		case "GB":
			multiplier = 1_000_000_000
		default:
			return 0, fmt.Errorf("unsupported byte unit %q", parts[1])
		}
	}
	return uint64(number * multiplier), nil
}

func fieldError(field string, err error) error {
	return fmt.Errorf("%s: %w", field, err)
}
