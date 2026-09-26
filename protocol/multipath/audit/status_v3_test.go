package audit

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestParseStatusSchemaAndLegOrdering(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	snapshot := syntheticSnapshot(start, start.Add(time.Second), 500, [2]uint64{300, 220}, [2]uint64{10, 10})
	snapshot.Node.Legs[0], snapshot.Node.Legs[1] = snapshot.Node.Legs[1], snapshot.Node.Legs[0]
	var object map[string]any
	if err := json.Unmarshal(marshalSnapshot(t, snapshot), &object); err != nil {
		t.Fatal(err)
	}
	object["future_field"] = "ignored"
	content, _ := json.Marshal(object)
	parsed, err := ParseStatus(content)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Node.Legs[0].ID != 0 || parsed.Node.Legs[1].ID != 1 {
		t.Fatalf("legs not normalized: %+v", parsed.Node.Legs)
	}
	view, err := parsed.TCPView()
	if err != nil {
		t.Fatal(err)
	}
	if view.LogicalRX != 500 || view.LegRX != [2]uint64{300, 220} {
		t.Fatalf("unexpected tcp view: %+v", view)
	}
}

func TestParseStatusFailsClosedOnMissingEvidence(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	content := marshalSnapshot(t, syntheticSnapshot(start, start.Add(time.Second), 1, [2]uint64{1, 0}, [2]uint64{}))
	var object map[string]json.RawMessage
	if err := json.Unmarshal(content, &object); err != nil {
		t.Fatal(err)
	}
	delete(object, "generated_at")
	content, _ = json.Marshal(object)
	_, err := ParseStatus(content)
	if !errors.Is(err, ErrInvalidStatus) {
		t.Fatalf("missing evidence accepted: %v", err)
	}
}

func TestParseStatusRejectsUDPUnderflow(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	snapshot := syntheticSnapshot(start, start.Add(time.Second), 10, [2]uint64{10, 0}, [2]uint64{})
	snapshot.Node.Legs[0].Cumulative.RXBytes = 5
	snapshot.Node.Legs[0].UDPCumulative.RXBytes = 6
	_, err := ParseStatus(marshalSnapshot(t, snapshot))
	if !errors.Is(err, ErrCounterUnderflow) {
		t.Fatalf("udp underflow accepted: %v", err)
	}
}

func TestScanStatusTrace(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	a := marshalSnapshot(t, syntheticSnapshot(start, start.Add(time.Second), 0, [2]uint64{}, [2]uint64{}))
	b := marshalSnapshot(t, syntheticSnapshot(start, start.Add(2*time.Second), 10, [2]uint64{6, 4}, [2]uint64{}))
	trace := append(append(append([]byte{}, a...), '\n'), b...)
	trace = append(trace, '\n')
	count, err := ScanStatusTrace(bytes.NewReader(trace), nil)
	if err != nil || count != 2 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	bad := strings.Replace(string(b), `"schema_version":3`, `"schema_version":2`, 1)
	_, err = ScanStatusTrace(strings.NewReader(string(a)+"\n"+bad+"\n"), nil)
	if err == nil {
		t.Fatal("unsupported schema accepted")
	}
}

func TestParseStatusFailsClosedOnMissingNestedRXCounter(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	content := marshalSnapshot(t, syntheticSnapshot(start, start.Add(time.Second), 1, [2]uint64{1, 0}, [2]uint64{}))
	var root map[string]any
	if err := json.Unmarshal(content, &root); err != nil {
		t.Fatal(err)
	}
	node := root["node"].(map[string]any)
	logical := node["logical"].(map[string]any)
	cumulative := logical["cumulative"].(map[string]any)
	delete(cumulative, "rx_bytes")
	content, _ = json.Marshal(root)
	_, err := ParseStatus(content)
	if !errors.Is(err, ErrInvalidStatus) {
		t.Fatalf("missing nested rx counter accepted: %v", err)
	}
}
