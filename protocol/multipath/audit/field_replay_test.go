package audit

import (
	"os"
	"path/filepath"
	"testing"
)

func fieldFixtures(t *testing.T) []string {
	t.Helper()
	fixtures, err := filepath.Glob("testdata/field/*.log")
	if err != nil {
		t.Fatal(err)
	}
	if len(fixtures) == 0 {
		if os.Getenv("AUDIT_V3_FIELD_FIXTURES") == "1" || os.Getenv("AUDIT_V3_FIELD_GATE") == "1" {
			t.Fatal("FIELD fixtures required by acceptance environment but none were materialized")
		}
		t.Skip("runner-only FIELD fixtures are not materialized in this test context")
	}
	if len(fixtures) != 4 {
		t.Fatalf("FIELD fixture inventory drift: got=%d want=4", len(fixtures))
	}
	return fixtures
}

func TestFieldReplayAllRecordedFixtures(t *testing.T) {
	fixtures := fieldFixtures(t)
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
				t.Fatal("FIELD fixture produced no recognized audit evidence")
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

// The acceptance gate has two responsibilities: historical journal-only FIELD
// evidence must fail closed, while a complete status+CAP replay with verified
// demand and a comparable A/B pair must close successfully. The real historical
// fixtures remain privacy-safe runner-only inputs and are never promoted into
// fabricated status evidence.
func TestFieldReplayDiagnosticClosureGate(t *testing.T) {
	if os.Getenv("AUDIT_V3_FIELD_GATE") != "1" {
		t.Skip("FIELD closure gate is enabled by the V3 workflow")
	}
	fixtures := fieldFixtures(t)
	for _, path := range fixtures {
		file, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		journal, analyzeErr := AnalyzeJournal(file)
		file.Close()
		if analyzeErr != nil {
			t.Fatalf("%s: journal analysis: %v", filepath.Base(path), analyzeErr)
		}
		if EvaluateDiagnosticClosure(nil, journal).Pass {
			t.Fatalf("%s: journal-only FIELD evidence falsely closed", filepath.Base(path))
		}
	}

	a := completeDiagnosticReport(t, []int{900, 900, 900})
	b := completeDiagnosticReport(t, []int{1000, 1000, 1000})
	ab, err := CompareAlgorithms(
		AlgorithmRun{AlgorithmID: "A", WorkloadID: "controlled-saturated-replay", Report: a},
		AlgorithmRun{AlgorithmID: "B", WorkloadID: "controlled-saturated-replay", Report: b},
	)
	if err != nil {
		t.Fatal(err)
	}
	closure := EvaluateDiagnosticClosure(&b, b.Evidence.Journal, ab)
	if err := closure.Error(); err != nil {
		t.Fatal(err)
	}
}
