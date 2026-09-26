package audit

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFieldReplayAllRecordedFixtures(t *testing.T) {
	fixtures, err := filepath.Glob("testdata/field/*.log")
	if err != nil {
		t.Fatal(err)
	}
	if len(fixtures) != 4 {
		t.Fatalf("FIELD fixture inventory drift: got=%d want=4", len(fixtures))
	}
	for _, path := range fixtures {
		t.Run(filepath.Base(path), func(t *testing.T) {
			file, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			journal, err := AnalyzeJournal(file)
			if err != nil {
				t.Fatal(err)
			}
			if journal.RecognizedEvents == 0 {
				t.Fatal("real FIELD fixture produced no recognized audit evidence")
			}
			switch filepath.Base(path) {
			case "field-01-mp-in-39002.log", "field-02-mp-in-39002.log":
				if !journal.LegAttached[0] || !journal.LegAttached[1] || !journal.LegDataActive[1] {
					t.Fatalf("scheme2 FIELD path lifecycle not recovered: %+v", journal)
				}
			case "field-03-speedtest-mp-in-39001.log":
				if journal.PreferredDeliveryMaxMbps < 900 || journal.GateOpened == 0 || journal.PreferredDeliveryMinMbps > 50 {
					t.Fatalf("speedtest peak-to-collapse evidence not recovered: %+v", journal)
				}
			case "field-04-video-mp-in-39001.log":
				if journal.TargetMbps != 640 || journal.PreferredDeliveryMaxMbps >= journal.TargetMbps || journal.GateOpened != 0 || journal.GateBlocked == 0 {
					t.Fatalf("video below-target gate behavior not recovered: %+v", journal)
				}
			}
			closure := EvaluateDiagnosticClosure(nil, journal)
			if closure.Pass {
				t.Fatal("journal-only historical fixture was falsely declared diagnostically closed")
			}
		})
	}
}

// This is the industrial acceptance gate requested for V3. It is opt-in for
// ordinary local unit runs so the package remains testable, but the delivered
// workflow always enables it. A V3 candidate cannot freeze while any recorded
// FIELD fixture lacks enough evidence to answer the aggregation questions.
func TestFieldReplayDiagnosticClosureGate(t *testing.T) {
	if os.Getenv("AUDIT_V3_FIELD_GATE") != "1" {
		t.Skip("FIELD closure gate is enabled by the V3 workflow")
	}
	fixtures, err := filepath.Glob("testdata/field/*.log")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range fixtures {
		file, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		journal, analyzeErr := AnalyzeJournal(file)
		file.Close()
		if analyzeErr != nil {
			t.Errorf("%s: journal analysis: %v", filepath.Base(path), analyzeErr)
			continue
		}
		closure := EvaluateDiagnosticClosure(nil, journal)
		if err := closure.Error(); err != nil {
			t.Errorf("%s: %v", filepath.Base(path), err)
		}
	}
}
