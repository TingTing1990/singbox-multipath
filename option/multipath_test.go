package option

import (
	"testing"

	"github.com/sagernet/sing/common/byteformats"
	"github.com/sagernet/sing/common/json"
)

func TestMultipathByteOptions(t *testing.T) {
	var outbound MultipathOutboundOptions
	if err := json.Unmarshal([]byte(`{
		"outbounds": ["leg0", "leg1"],
		"server": "127.0.0.1",
		"server_port": 39000,
		"activation_after_bytes": "2MB",
		"activation_after_bytes_min_mbps": 120,
		"memory_limit": "64MB"
	}`), &outbound); err != nil {
		t.Fatal(err)
	}
	if outbound.ActivationAfterBytes.Value() != 2*byteformats.MiByte {
		t.Fatalf("unexpected activation_after_bytes: %d", outbound.ActivationAfterBytes.Value())
	}
	if outbound.ActivationAfterBytesMinMbps != 120 {
		t.Fatalf("unexpected activation_after_bytes_min_mbps: %d", outbound.ActivationAfterBytesMinMbps)
	}
	if outbound.MemoryLimit.Value() != 64*byteformats.MiByte {
		t.Fatalf("unexpected memory_limit: %d", outbound.MemoryLimit.Value())
	}

	var inbound MultipathInboundOptions
	if err := json.Unmarshal([]byte(`{
		"activation_after_bytes": 4096,
		"memory_limit": 8388608
	}`), &inbound); err != nil {
		t.Fatal(err)
	}
	if inbound.ActivationAfterBytes.Value() != 4096 {
		t.Fatalf("unexpected numeric activation_after_bytes: %d", inbound.ActivationAfterBytes.Value())
	}
	if inbound.MemoryLimit.Value() != 8388608 {
		t.Fatalf("unexpected numeric memory_limit: %d", inbound.MemoryLimit.Value())
	}
}
