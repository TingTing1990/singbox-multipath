//go:build linux

package multipath

import (
	"bufio"
	"errors"
	"os"
	"strconv"
	"strings"
)

func availableMemory() (uint64, error) {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 || fields[0] != "MemAvailable:" {
			continue
		}
		kilobytes, parseErr := strconv.ParseUint(fields[1], 10, 64)
		if parseErr != nil {
			return 0, parseErr
		}
		return kilobytes * 1024, nil
	}
	if err = scanner.Err(); err != nil {
		return 0, err
	}
	return 0, errors.New("MemAvailable is missing from /proc/meminfo")
}
