package audit

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	D "github.com/sagernet/sing-box/protocol/multipath/downloadevidence"
)

func downloadFixture(t *testing.T) []string {
	t.Helper()
	// Independent numbers: two seconds; 30 normal + 10 repair bytes sent,
	// 25 unique logical bytes confirmed. No equality of pipeline stages assumed.
	values := []struct {
		k string
		v any
	}{
		{"WINDOW", D.Snapshot{Memory: D.Memory{Limit: 1000}, Config: D.Config{ChunkSize: 100}}},
		{"DECISION", D.Decision{Candidate: 0, Selected: 0, Length: 30, Outcome: "submitted", Reason: "minimum_drain_time"}},
		{"DECISION", D.Decision{Candidate: 1, Selected: 1, Length: 10, Repair: true, Outcome: "submitted", Reason: "repair_stale"}},
		{"WRITE", D.Transfer{Leg: 0, Bytes: 30, Outcome: "written"}},
		{"WRITE", D.Transfer{Leg: 1, Bytes: 10, Repair: true, Outcome: "written"}},
		{"FEEDBACK", D.Feedback{LogicalBytes: 25, PathBytes: [2]uint64{30, 10}}},
		{"WINDOW", D.Snapshot{Memory: D.Memory{Limit: 1000}, Config: D.Config{ChunkSize: 100}, Totals: D.Totals{Decisions: 2, AssignedNormal: [2]uint64{30, 0}, AssignedRepair: [2]uint64{0, 10}, WrittenNormal: [2]uint64{30, 0}, WrittenRepair: [2]uint64{0, 10}, ConfirmedPath: [2]uint64{30, 10}, ConfirmedLogical: 25}}},
	}
	var lines []string
	for i, v := range values {
		h := D.Header{Schema: 1, Epoch: "epoch", Instance: "mp-in", Side: "server", Direction: "download", Seq: uint64(i + 1), At: time.Unix(1000, 0), MonoNS: int64(i) * 2000000000 / 6, Kind: v.k, Session: "session"}
		data, e := D.Encode(h, v.v)
		if e != nil {
			t.Fatal(e)
		}
		lines = append(lines, D.Marker+string(data))
	}
	return lines
}

func TestServerDownloadIndependentCountersAndFaults(t *testing.T) {
	good := downloadFixture(t)
	r, e := AnalyzeServerDownload(strings.NewReader(strings.Join(good, "\n")), "mp-in", "")
	if e != nil || !r.Complete || len(r.Windows) != 1 {
		t.Fatalf("positive replay: %v", e)
	}
	w := r.Windows[0]
	if w.UsefulConfirmedMbps != 0.0001 || w.WrittenMbps != [2]float64{0.00012, 0.00004} || w.Delta.AssignedRepair[1] != 10 {
		t.Fatalf("counter meaning changed: %+v", w)
	}
	if !w.TotalsComplete || !w.TraceComplete {
		t.Fatalf("complete window classified incomplete: %+v", w)
	}
	historical := make([]string, len(good))
	for i, line := range good {
		historical[i] = strings.Replace(line, `"dropped":0`, `"dropped":597`, 1)
	}
	historicalReport, historicalErr := AnalyzeServerDownload(strings.NewReader(strings.Join(historical, "\n")), "mp-in", "")
	if historicalErr != nil || !historicalReport.Complete || len(historicalReport.Windows) != 1 || !historicalReport.Windows[0].TraceComplete {
		t.Fatalf("historical dropped baseline polluted clean interval: report=%+v err=%v", historicalReport, historicalErr)
	}
	gapped := append([]string{}, good[:2]...)
	gapped = append(gapped, good[3:]...)
	gapReport, gapErr := AnalyzeServerDownload(strings.NewReader(strings.Join(gapped, "\n")), "mp-in", "")
	if gapErr == nil || gapReport.Complete || len(gapReport.Windows) != 1 {
		t.Fatalf("gapped trace not rejected with aggregate window retained: report=%+v err=%v", gapReport, gapErr)
	}
	if !gapReport.Windows[0].TotalsComplete || gapReport.Windows[0].TraceComplete || gapReport.Windows[0].Delta.AssignedRepair[1] != 10 {
		t.Fatalf("aggregate/trace completeness not separated: %+v", gapReport.Windows[0])
	}
	// Privacy-safe compact replay of the first real FIELD loss shape:
	// dropped 597 -> 703 and seq 36333 -> 36440, i.e. 106 missing records.
	fieldShape := []struct {
		seq     uint64
		dropped uint64
		kind    string
		data    any
	}{
		{1, 597, "WINDOW", D.Snapshot{Memory: D.Memory{Limit: 1000}, Config: D.Config{ChunkSize: 100}}},
		{2, 597, "WAIT", D.Decision{Outcome: "waiting", Reason: "field-shape"}},
		{109, 703, "WAIT", D.Decision{Outcome: "waiting", Reason: "field-shape"}},
		{110, 703, "WINDOW", D.Snapshot{Memory: D.Memory{Limit: 1000}, Config: D.Config{ChunkSize: 100}, Totals: D.Totals{Waits: 108}}},
	}
	fieldLines := make([]string, 0, len(fieldShape))
	for i, item := range fieldShape {
		h := D.Header{Schema: 1, Epoch: "field", Instance: "mp-in", Side: "server", Direction: "download", Seq: item.seq, Dropped: item.dropped, At: time.Unix(2000, int64(i)), MonoNS: int64(i) * int64(time.Second), Kind: item.kind, Session: "session"}
		encoded, encodeErr := D.Encode(h, item.data)
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		fieldLines = append(fieldLines, D.Marker+string(encoded))
	}
	fieldReport, fieldErr := AnalyzeServerDownload(strings.NewReader(strings.Join(fieldLines, "\n")), "mp-in", "field")
	if fieldErr == nil || fieldReport.Complete || len(fieldReport.Windows) != 1 || fieldReport.Windows[0].TraceComplete || !fieldReport.Windows[0].TotalsComplete {
		t.Fatalf("FIELD-derived loss shape misclassified: report=%+v err=%v", fieldReport, fieldErr)
	}
	joinedIssues := strings.Join(fieldReport.Issues, "\n")
	for _, want := range []string{"missing=106", "delta=106 baseline=597 end=703", "window totals disagree"} {
		if !strings.Contains(joinedIssues, want) {
			t.Fatalf("FIELD-derived issue detail missing %q: %v", want, fieldReport.Issues)
		}
	}
	for _, mode := range []string{"gap", "duplicate", "epoch", "drop", "counter", "direction", "rollback", "empty", "truncated", "missing_payload", "missing_drop", "short_paths"} {
		t.Run(mode, func(t *testing.T) {
			lines := append([]string{}, good...)
			switch mode {
			case "missing_payload":
				lines[5] = strings.Replace(lines[5], `"LogicalBytes":25,`, "", 1)
			case "missing_drop":
				lines[2] = strings.Replace(lines[2], `"dropped":0,`, "", 1)
			case "short_paths":
				lines[5] = strings.Replace(lines[5], `"PathBytes":[30,10]`, `"PathBytes":[30]`, 1)
			case "gap":
				lines = append(lines[:2], lines[3:]...)
			case "duplicate":
				lines = append(lines[:2], append([]string{lines[1]}, lines[2:]...)...)
			case "epoch":
				lines[2] = strings.Replace(lines[2], `"epoch":"epoch"`, `"epoch":"other"`, 1)
			case "drop":
				lines[2] = strings.Replace(lines[2], `"dropped":0`, `"dropped":1`, 1)
			case "counter":
				lines[6] = strings.Replace(lines[6], `"ConfirmedLogical":25`, `"ConfirmedLogical":26`, 1)
			case "direction":
				lines[2] = strings.Replace(lines[2], `"direction":"download"`, `"direction":"upload"`, 1)
			case "rollback":
				lines[2] = strings.Replace(lines[2], `"mono_ns":666666666`, `"mono_ns":0`, 1)
			case "empty":
				lines = nil
			case "truncated":
				lines[6] = lines[6][:30]
			}
			bad, err := AnalyzeServerDownload(strings.NewReader(strings.Join(lines, "\n")), "mp-in", "")
			if err == nil || bad.Complete {
				t.Fatalf("%s falsely accepted", mode)
			}
		})
	}
	// Actual journal JSON framing must preserve the same calculations.
	var jsonl bytes.Buffer
	for _, s := range good {
		b, _ := json.Marshal(map[string]string{"MESSAGE": s, "__CURSOR": "test"})
		jsonl.Write(b)
		jsonl.WriteByte('\n')
	}
	r, e = AnalyzeServerDownload(&jsonl, "mp-in", "epoch")
	if e != nil || len(r.Windows) != 1 || r.Windows[0] != w {
		t.Fatalf("journal framing differs: %v", e)
	}
}
