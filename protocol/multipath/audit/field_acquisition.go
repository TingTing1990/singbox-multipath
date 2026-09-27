package audit

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"time"
)

const defaultStatusCapturePoll = 100 * time.Millisecond

type statusCaptureState struct {
	lastIdentity string
	lastHash     [sha256.Size]byte
	haveLast     bool
}

func (s *statusCaptureState) accept(data []byte) (StatusSnapshot, bool, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return StatusSnapshot{}, false, nil
	}
	snapshot, err := ParseStatus(trimmed)
	if err != nil {
		return StatusSnapshot{}, false, err
	}
	identity := snapshot.ProcessStartedRaw + "\x00" + snapshot.GeneratedAtRaw + "\x00" + snapshot.Node.Tag
	hash := sha256.Sum256(trimmed)
	if s.haveLast && identity == s.lastIdentity {
		if hash != s.lastHash {
			return StatusSnapshot{}, false, fmt.Errorf("%w: status snapshot identity changed content at generated_at=%s node=%q", ErrInvalidStatus, snapshot.GeneratedAtRaw, snapshot.Node.Tag)
		}
		return snapshot, false, nil
	}
	s.lastIdentity = identity
	s.lastHash = hash
	s.haveLast = true
	return snapshot, true, nil
}

// CaptureStatusFile turns the frozen outbound status_file (an atomically
// replaced one-snapshot document) into a newline-delimited FIELD trace without
// changing the runtime producer. Polling faster than the producer's one-second
// cadence prevents intentional sampling loss; BuildTimeline still marks gaps
// larger than 1.5 seconds as degraded evidence.
func CaptureStatusFile(ctx context.Context, path string, output io.Writer, pollInterval time.Duration) (uint64, error) {
	if ctx == nil {
		return 0, fmt.Errorf("nil capture context")
	}
	if path == "" {
		return 0, fmt.Errorf("empty status_file path")
	}
	if output == nil {
		return 0, fmt.Errorf("nil status trace output")
	}
	if pollInterval <= 0 {
		pollInterval = defaultStatusCapturePoll
	}

	var state statusCaptureState
	var count uint64
	capture := func() error {
		data, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		_, accepted, err := state.accept(data)
		if err != nil {
			return err
		}
		if !accepted {
			return nil
		}
		trimmed := bytes.TrimSpace(data)
		if _, err := output.Write(trimmed); err != nil {
			return err
		}
		if _, err := io.WriteString(output, "\n"); err != nil {
			return err
		}
		count++
		return nil
	}

	if err := capture(); err != nil {
		return count, err
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return count, nil
		case <-ticker.C:
			if err := capture(); err != nil {
				return count, err
			}
		}
	}
}
