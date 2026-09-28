package audit

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	D "github.com/sagernet/sing-box/protocol/multipath/downloadevidence"
)

type DownloadCaptureIdentity struct {
	PID          int    `json:"pid"`
	Invocation   string `json:"invocation"`
	ProcessStart string `json:"process_start"`
	BinaryPath   string `json:"binary_path"`
	BinarySHA256 string `json:"binary_sha256"`
	Version      string `json:"version"`
	ConfigSHA256 string `json:"config_sha256"`
	BootID       string `json:"boot_id"`
}
type DownloadCaptureManifest struct {
	Schema        int                     `json:"schema"`
	Kind          string                  `json:"kind"`
	Instance      string                  `json:"instance"`
	Epoch         string                  `json:"epoch"`
	Complete      bool                    `json:"complete"`
	StartCursor   string                  `json:"start_cursor"`
	EndCursor     string                  `json:"end_cursor"`
	JournalSHA256 string                  `json:"journal_sha256"`
	FirstSnapshot uint64                  `json:"first_snapshot_seq"`
	LastSnapshot  uint64                  `json:"last_snapshot_seq"`
	Start         DownloadCaptureIdentity `json:"start_identity"`
	End           DownloadCaptureIdentity `json:"end_identity"`
}

// VerifyDownloadCapture verifies the supplied collector contract, not the
// authenticity of the machine or proof of an independently witnessed FIELD run.
func VerifyDownloadCapture(journal io.ReadSeeker, manifest io.Reader, instance string) (DownloadCaptureManifest, error) {
	var m DownloadCaptureManifest
	if err := json.NewDecoder(manifest).Decode(&m); err != nil {
		return m, err
	}
	fail := func(reason string) (DownloadCaptureManifest, error) {
		return m, fmt.Errorf("capture integrity: %s", reason)
	}
	if m.Schema != 1 || m.Kind != "systemd-server-download" || !m.Complete || m.Instance != instance || m.Epoch == "" || m.StartCursor == "" || m.EndCursor == "" || m.FirstSnapshot == 0 || m.LastSnapshot <= m.FirstSnapshot {
		return fail("missing complete capture boundaries")
	}
	if m.Start != m.End || m.Start.PID <= 0 || m.Start.Invocation == "" || m.Start.Version == "" || m.Start.BootID == "" || m.Start.BinaryPath == "" || m.Start.ProcessStart == "" {
		return fail("runtime identity missing or changed")
	}
	for _, h := range []string{m.Start.BinarySHA256, m.Start.ConfigSHA256, m.JournalSHA256} {
		if b, e := hex.DecodeString(h); e != nil || len(b) != sha256.Size {
			return fail("invalid identity hash")
		}
	}
	if _, err := journal.Seek(0, io.SeekStart); err != nil {
		return m, err
	}
	h := sha256.New()
	if _, err := io.Copy(h, journal); err != nil {
		return m, err
	}
	if hex.EncodeToString(h.Sum(nil)) != m.JournalSHA256 {
		return fail("journal SHA256 mismatch")
	}
	if _, err := journal.Seek(0, io.SeekStart); err != nil {
		return m, err
	}
	scan := bufio.NewScanner(journal)
	scan.Buffer(make([]byte, 64<<10), 4<<20)
	var first, last, lastTarget uint64
	lastCursor := ""
	for scan.Scan() {
		var entry struct {
			Message    string `json:"MESSAGE"`
			Cursor     string `json:"__CURSOR"`
			Invocation string `json:"_SYSTEMD_INVOCATION_ID"`
			Boot       string `json:"_BOOT_ID"`
		}
		if err := json.Unmarshal(scan.Bytes(), &entry); err != nil {
			return fail("invalid journal JSONL")
		}
		lastCursor = entry.Cursor
		at := strings.Index(entry.Message, D.Marker)
		if at < 0 {
			continue
		}
		var e D.Envelope
		if err := json.Unmarshal([]byte(entry.Message[at+len(D.Marker):]), &e); err != nil {
			return fail("invalid download event")
		}
		if e.Instance != instance {
			continue
		}
		if e.Epoch != m.Epoch || entry.Invocation != m.Start.Invocation || strings.ReplaceAll(entry.Boot, "-", "") != strings.ReplaceAll(m.Start.BootID, "-", "") {
			return fail("journal epoch/boot/invocation mismatch")
		}
		lastTarget = e.Seq
		if e.Kind == "START" || e.Kind == "WINDOW" || e.Kind == "STOP" {
			if first == 0 {
				first = e.Seq
			}
			last = e.Seq
		}
	}
	if err := scan.Err(); err != nil {
		return m, err
	}
	if first != m.FirstSnapshot || last != m.LastSnapshot || lastTarget != last || lastCursor != m.EndCursor {
		return fail("declared cursor/snapshot boundary absent")
	}
	_, err := journal.Seek(0, io.SeekStart)
	return m, err
}
