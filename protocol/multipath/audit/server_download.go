package audit

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"

	D "github.com/sagernet/sing-box/protocol/multipath/downloadevidence"
)

// ServerDownloadReport covers server-observed feedback time. It is not client
// application throughput, NIC throughput, unique per-leg attribution, or A/B.
type ServerDownloadReport struct {
	Scope                        string
	UsefulMeaning                string
	WriteMeaning                 string
	PartialWriteBytesUnknown     bool
	CoveredStartNS, CoveredEndNS int64

	Schema          int
	Instance, Epoch string
	Complete        bool
	Issues          []string
	Windows         []ServerDownloadWindow
	Events          []D.Envelope
	Reasons         map[string]uint64
}
type ServerDownloadWindow struct {
	StartNS, EndNS                         int64
	Delta                                  D.Totals
	AssignedNormalMbps, AssignedRepairMbps [2]float64
	WrittenMbps, ConfirmedPathMbps         [2]float64
	UsefulConfirmedMbps                    float64
	TotalsComplete, TraceComplete          bool
	Memory                                 D.Memory
	Config                                 D.Config
}

// Subtraction is deliberately fieldwise. No reset, wrapping or cross-epoch
// cumulative subtraction is permitted.
func downloadDelta(a, b D.Totals) (D.Totals, bool) {
	out := b
	av, bv, ov := reflect.ValueOf(a), reflect.ValueOf(b), reflect.ValueOf(&out).Elem()
	for i := 0; i < av.NumField(); i++ {
		x, y, z := av.Field(i), bv.Field(i), ov.Field(i)
		if x.Kind() == reflect.Array {
			for j := 0; j < x.Len(); j++ {
				if y.Index(j).Uint() < x.Index(j).Uint() {
					return D.Totals{}, false
				}
				z.Index(j).SetUint(y.Index(j).Uint() - x.Index(j).Uint())
			}
		} else {
			if y.Uint() < x.Uint() {
				return D.Totals{}, false
			}
			z.SetUint(y.Uint() - x.Uint())
		}
	}
	return out, true
}

// Input may be journalctl -o cat text or -o json JSONL. Explicit instance and
// optional epoch selection are mandatory boundaries, not inferred from traffic.
// Partial captures still produce a useful report, with Complete=false and an
// error. The caller must never convert that error into a positive acceptance.
func AnalyzeServerDownload(reader io.Reader, instance, epoch string) (ServerDownloadReport, error) {
	report := ServerDownloadReport{Schema: 1, Instance: instance, Epoch: epoch, Reasons: make(map[string]uint64), Scope: "complete snapshot-to-snapshot intervals only; leading/trailing events retained but not extrapolated", UsefulMeaning: "server-observed logical DATA acknowledgement; not client application reads", WriteMeaning: "successfully completed DATA payload writes; excludes wire headers, lower-transport overhead and unknown failed-write prefixes"}
	if instance == "" {
		return report, fmt.Errorf("server download requires instance binding")
	}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64<<10), 4<<20)
	var lastSeq uint64
	var lastNS int64
	var last *D.Snapshot
	var lastSnapshotNS int64
	var lastSnapshotDropped uint64
	var lastDropped uint64
	var intervalMaxDropped uint64
	var intervalSequenceIssues []string
	var intervalDroppedRollback bool
	var checked D.Totals
	var selectedEpoch string
	issue := func(s string) {
		report.Issues = append(report.Issues, s)
	}
	resetIntervalEvidence := func(snapshotDropped uint64) {
		lastSnapshotDropped = snapshotDropped
		lastDropped = snapshotDropped
		intervalMaxDropped = snapshotDropped
		intervalSequenceIssues = nil
		intervalDroppedRollback = false
		checked = D.Totals{}
	}
	for scanner.Scan() {
		raw := scanner.Text()
		if strings.HasPrefix(strings.TrimSpace(raw), "{") {
			var journal struct {
				Message string `json:"MESSAGE"`
			}
			if err := json.Unmarshal([]byte(raw), &journal); err != nil {
				issue("malformed journal JSON")
				continue
			}
			if journal.Message != "" {
				raw = journal.Message
			}
		}
		at := strings.Index(raw, D.Marker)
		if at < 0 {
			continue
		}
		var required map[string]json.RawMessage
		if err := json.Unmarshal([]byte(raw[at+len(D.Marker):]), &required); err == nil {
			for _, key := range []string{"schema", "epoch", "instance", "side", "direction", "seq", "at", "mono_ns", "dropped", "kind", "data"} {
				if value, ok := required[key]; !ok || string(value) == "null" {
					issue("missing envelope field " + key)
				}
			}
		}
		var e D.Envelope
		if err := json.Unmarshal([]byte(raw[at+len(D.Marker):]), &e); err != nil {
			issue("malformed download event")
			continue
		}
		if e.Instance != instance {
			continue
		}
		if epoch != "" && e.Epoch != epoch {
			continue
		}
		if selectedEpoch == "" {
			selectedEpoch = e.Epoch
			report.Epoch = e.Epoch
		}
		if e.Schema != D.Version || e.Side != "server" || e.Direction != "download" || e.Epoch == "" || e.Seq == 0 || e.At.IsZero() || e.MonoNS < 0 {
			issue("invalid download envelope")
			continue
		}
		if e.Epoch != selectedEpoch {
			issue("multiple epochs: select one explicitly")
			continue
		}
		if last != nil {
			if lastSeq != 0 && e.Seq != lastSeq+1 {
				if e.Seq > lastSeq+1 {
					intervalSequenceIssues = append(intervalSequenceIssues, fmt.Sprintf("prev=%d next=%d missing=%d", lastSeq, e.Seq, e.Seq-lastSeq-1))
				} else {
					intervalSequenceIssues = append(intervalSequenceIssues, fmt.Sprintf("prev=%d next=%d duplicate_or_reset", lastSeq, e.Seq))
				}
			}
			if e.Dropped < lastDropped {
				intervalDroppedRollback = true
			} else if e.Dropped > intervalMaxDropped {
				intervalMaxDropped = e.Dropped
			}
			lastDropped = e.Dropped
		}
		if lastSeq != 0 && e.MonoNS < lastNS {
			issue("monotonic timestamp rollback")
		}
		lastSeq, lastNS = e.Seq, e.MonoNS
		report.Events = append(report.Events, e)
		switch e.Kind {
		case "START", "WINDOW", "STOP":
			var s D.Snapshot
			if err := decodeDownloadPayload(e.Data, &s); err != nil {
				issue("invalid window snapshot")
				continue
			}
			if s.Config.CapacityTargetBytesPS > 0 && (!s.Capacity.Enabled || s.Capacity.TargetBytesPS != s.Config.CapacityTargetBytesPS) {
				issue("enabled Capacity state missing or mismatched")
			}
			if s.Config.ChunkSize <= 0 || s.Memory.Limit <= 0 {
				issue("missing effective configuration or memory snapshot")
			}
			if last != nil {
				delta, ok := downloadDelta(last.Totals, s.Totals)
				if !ok {
					issue("invalid cumulative window")
					last = nil
					continue
				}
				totalsComplete := true
				if s.Config != last.Config {
					issue("effective configuration changed within epoch")
					totalsComplete = false
				}
				traceComplete := true
				if len(intervalSequenceIssues) != 0 {
					for _, detail := range intervalSequenceIssues {
						issue(fmt.Sprintf("event sequence gap within window %d-%d: %s", lastSnapshotNS, e.MonoNS, detail))
					}
					traceComplete = false
				}
				if intervalDroppedRollback {
					issue(fmt.Sprintf("runtime observation dropped counter rollback within window %d-%d", lastSnapshotNS, e.MonoNS))
					traceComplete = false
				} else if intervalMaxDropped > lastSnapshotDropped {
					issue(fmt.Sprintf("runtime observation dropped events within window %d-%d: delta=%d baseline=%d end=%d", lastSnapshotNS, e.MonoNS, intervalMaxDropped-lastSnapshotDropped, lastSnapshotDropped, intervalMaxDropped))
					traceComplete = false
				}
				if delta != checked {
					issue(fmt.Sprintf("window totals disagree with observed runtime events within window %d-%d", lastSnapshotNS, e.MonoNS))
					traceComplete = false
				}
				if e.MonoNS < lastSnapshotNS {
					issue("invalid cumulative window")
					last = nil
					continue
				}
				if e.MonoNS == lastSnapshotNS {
					if delta != (D.Totals{}) || checked != (D.Totals{}) {
						issue("invalid cumulative window")
						last = nil
						continue
					}
					copy := s
					last = &copy
					lastSnapshotNS = e.MonoNS
					resetIntervalEvidence(e.Dropped)
					continue
				}
				seconds := float64(e.MonoNS-lastSnapshotNS) / 1e9
				w := ServerDownloadWindow{StartNS: lastSnapshotNS, EndNS: e.MonoNS, Delta: delta, Config: s.Config, Memory: s.Memory, TotalsComplete: totalsComplete, TraceComplete: traceComplete}
				rate := func(n uint64) float64 { return float64(n) * 8 / seconds / 1e6 }
				for i := 0; i < 2; i++ {
					w.AssignedNormalMbps[i] = rate(delta.AssignedNormal[i])
					w.AssignedRepairMbps[i] = rate(delta.AssignedRepair[i])
					w.WrittenMbps[i] = rate(delta.WrittenNormal[i]) + rate(delta.WrittenRepair[i])
					w.ConfirmedPathMbps[i] = rate(delta.ConfirmedPath[i])
				}
				w.UsefulConfirmedMbps = rate(delta.ConfirmedLogical)
				if len(report.Windows) == 0 {
					report.CoveredStartNS = w.StartNS
				}
				report.CoveredEndNS = w.EndNS
				report.Windows = append(report.Windows, w)
			}
			copy := s
			last = &copy
			lastSnapshotNS = e.MonoNS
			resetIntervalEvidence(e.Dropped)
		case "DECISION", "WAIT":
			var d D.Decision
			if err := decodeDownloadPayload(e.Data, &d); err != nil {
				issue("invalid decision")
				continue
			}
			if e.Session == "" || d.Reason == "" {
				issue("unbound decision or missing branch reason")
			}
			report.Reasons[d.Reason]++
			if e.Kind == "WAIT" {
				checked.Waits++
				continue
			}
			checked.Decisions++
			if d.Outcome == "submitted" {
				if d.Selected < 0 || d.Selected > 1 || d.Length <= 0 {
					issue("invalid committed assignment")
					continue
				}
				if d.Repair {
					checked.AssignedRepair[d.Selected] += uint64(d.Length)
				} else {
					checked.AssignedNormal[d.Selected] += uint64(d.Length)
				}
			} else if d.Outcome == "failed" {
				checked.SubmitFailures++
			} else {
				issue("unknown submit outcome")
			}
		case "WRITE":
			var w D.Transfer
			if err := decodeDownloadPayload(e.Data, &w); err != nil || w.Leg < 0 || w.Leg > 1 || e.Session == "" {
				issue("invalid DATA completion")
				continue
			}
			if w.Outcome == "written" {
				if w.Repair {
					checked.WrittenRepair[w.Leg] += w.Bytes
				} else {
					checked.WrittenNormal[w.Leg] += w.Bytes
				}
			} else if w.Outcome == "failed" {
				checked.WriteFailures++
				report.PartialWriteBytesUnknown = true
			} else {
				issue("unknown write outcome")
			}
		case "FEEDBACK":
			var f D.Feedback
			if err := decodeDownloadPayload(e.Data, &f); err != nil || e.Session == "" {
				issue("invalid feedback")
				continue
			}
			checked.ConfirmedLogical += f.LogicalBytes
			for i := range checked.ConfirmedPath {
				checked.ConfirmedPath[i] += f.PathBytes[i]
			}
		case "ACTIVATION", "SESSION_ESTABLISHED", "PATH_ATTACHED", "PATH_FAILED", "SESSION_OPEN", "SESSION_CLOSE":
			var s D.Session
			if err := decodeDownloadPayload(e.Data, &s); err != nil || e.Session == "" {
				issue("invalid session lifecycle evidence")
			}
			if e.Kind == "SESSION_OPEN" {
				checked.SessionsStarted++
			}
			if e.Kind == "SESSION_CLOSE" {
				checked.SessionsClosed++
			}
		default:
			issue("unknown download event kind")
		}
	}
	if err := scanner.Err(); err != nil {
		issue("journal read: " + err.Error())
	}
	if len(report.Windows) == 0 {
		issue("need two valid aggregate snapshots")
	}
	report.Complete = len(report.Issues) == 0
	if !report.Complete {
		return report, fmt.Errorf("server download evidence incomplete: %s", strings.Join(report.Issues, "; "))
	}
	return report, nil
}

// Every payload field is emitted by schema v1, including zero values. Missing
// fields/null must not silently become Go zero values and fabricate completeness.
func decodeDownloadPayload(raw json.RawMessage, out any) error {
	if err := requireDownloadShape(raw, reflect.TypeOf(out).Elem()); err != nil {
		return err
	}
	return json.Unmarshal(raw, out)
}
func requireDownloadShape(raw json.RawMessage, typ reflect.Type) error {
	if string(raw) == "null" || len(raw) == 0 {
		return fmt.Errorf("null evidence")
	}
	switch typ.Kind() {
	case reflect.Struct:
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			return err
		}
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			value, ok := fields[f.Name]
			if !ok {
				return fmt.Errorf("missing evidence field %s", f.Name)
			}
			if err := requireDownloadShape(value, f.Type); err != nil {
				return err
			}
		}
	case reflect.Array:
		var fields []json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			return err
		}
		if len(fields) != typ.Len() {
			return fmt.Errorf("invalid evidence array length")
		}
		for _, field := range fields {
			if err := requireDownloadShape(field, typ.Elem()); err != nil {
				return err
			}
		}
	}
	return nil
}
