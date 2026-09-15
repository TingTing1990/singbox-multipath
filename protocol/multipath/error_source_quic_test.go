//go:build with_quic

package multipath

import (
	"testing"

	"github.com/sagernet/quic-go"
)

func TestQUICCancellationNeedsEndpointProvenance(t *testing.T) {
	core := &mpCore{}
	err := &quic.StreamError{StreamID: 12, Remote: true, ErrorCode: 0}
	if got := core.sourcedLegError(err).(*sourcedLegError).source; got != closeSourceUnknown {
		t.Fatal("unattributed QUIC cancellation was hidden")
	}
	core.noteCloseSource(closeSourceRemoteEndpoint)
	if got := core.sourcedLegError(err).(*sourcedLegError).source; got != closeSourceRemoteEndpoint {
		t.Fatal("explicit peer endpoint close was lost")
	}
	err.ErrorCode = 42
	if got := core.sourcedLegError(err).(*sourcedLegError).source; got != closeSourceUnknown {
		t.Fatal("nonzero QUIC error was hidden")
	}
}
