package multipath

import (
	"context"
	"errors"
	"io"
	"net"
	"syscall"
)

// Sources describe the boundary that initiated shutdown, not a diagnosis of
// the physical network. Only an explicit endpoint close can hide a leg event.
type closeSource uint32

const (
	closeSourceUnknown closeSource = iota
	closeSourceLocalEndpoint
	closeSourceRemoteEndpoint
	closeSourceTransport
	closeSourceShutdown
	closeSourcePeerUnknown
)

func (s closeSource) String() string {
	switch s {
	case closeSourceLocalEndpoint:
		return "local_endpoint"
	case closeSourceRemoteEndpoint:
		return "remote_endpoint"
	case closeSourceTransport:
		return "transport"
	case closeSourceShutdown:
		return "shutdown"
	default:
		return "unknown"
	}
}

// The sender's local/remote distinction is intentionally not transmitted:
// either endpoint may be relaying a previously received endpoint shutdown.
const (
	closeReasonUnknown byte = iota
	closeReasonEndpoint
	closeReasonTransport
	closeReasonShutdown
)

func (c *mpCore) noteCloseSource(source closeSource) {
	c.closeSource.CompareAndSwap(uint32(closeSourceUnknown), uint32(source))
}

func (c *mpCore) wireCloseReason() byte {
	switch closeSource(c.closeSource.Load()) {
	case closeSourceLocalEndpoint, closeSourceRemoteEndpoint:
		return closeReasonEndpoint
	case closeSourceTransport:
		return closeReasonTransport
	case closeSourceShutdown:
		return closeReasonShutdown
	default:
		return closeReasonUnknown
	}
}

func (c *mpCore) notePeerCloseReason(reason byte) {
	switch reason {
	case closeReasonEndpoint:
		c.noteCloseSource(closeSourceRemoteEndpoint)
	case closeReasonTransport:
		c.noteCloseSource(closeSourceTransport)
	case closeReasonShutdown:
		c.noteCloseSource(closeSourceShutdown)
	}
}

type sourcedLegError struct {
	error
	source closeSource
}

func (e *sourcedLegError) Unwrap() error { return e.error }

func isEndpointLegError(err error) bool {
	var sourced *sourcedLegError
	return errors.As(err, &sourced) &&
		(sourced.source == closeSourceLocalEndpoint || sourced.source == closeSourceRemoteEndpoint)
}

var isQUICStreamClose = func(error) bool { return false }

func isLegCloseError(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, context.Canceled) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) || isQUICStreamClose(err)
}

func (c *mpCore) sourcedLegError(err error) error {
	source := closeSourceUnknown
	if isLegCloseError(err) {
		source = closeSource(c.closeSource.Load())
	}
	// A timeout or a malformed frame remains visible even if the application
	// is concurrently closing. Never hide an event merely because of timing.
	return &sourcedLegError{error: err, source: source}
}
