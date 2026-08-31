//go:build !linux

package multipath

import "errors"

func availableMemory() (uint64, error) {
	return 0, errors.New("MemAvailable is unavailable on this platform")
}
