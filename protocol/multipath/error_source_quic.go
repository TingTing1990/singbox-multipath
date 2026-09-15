//go:build with_quic

package multipath

import (
	"errors"

	"github.com/sagernet/quic-go"
)

func init() {
	isQUICStreamClose = func(err error) bool {
		var streamError *quic.StreamError
		return errors.As(err, &streamError) && streamError.ErrorCode == 0
	}
}
