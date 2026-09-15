package multipath

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"
)

func TestReorderFrameOptions(t *testing.T) {
	for _, test := range []struct {
		name    string
		options string
		want    int
		invalid bool
	}{
		{name: "omitted", options: `{}`, want: 2048},
		{name: "zero", options: `{"max_reorder_frames":0}`, want: 2048},
		{name: "minimum", options: `{"max_reorder_frames":64}`, want: 64},
		{name: "default", options: `{"max_reorder_frames":2048}`, want: 2048},
		{name: "custom", options: `{"max_reorder_frames":8192}`, want: 8192},
		{name: "maximum", options: `{"max_reorder_frames":65536}`, want: 65536},
		{name: "below_minimum", options: `{"max_reorder_frames":63}`, invalid: true},
		{name: "above_maximum", options: `{"max_reorder_frames":65537}`, invalid: true},
	} {
		for _, side := range []string{"client", "server"} {
			t.Run(side+"/"+test.name, func(t *testing.T) {
				cfg, err := reorderOptionsConfig(t, side, test.options)
				if test.invalid {
					if err == nil || !strings.Contains(err.Error(), "invalid max_reorder_frames") {
						t.Fatalf("expected max_reorder_frames validation failure, got %v", err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if cfg.MaxReorderFrames != test.want {
					t.Fatalf("MaxReorderFrames=%d, want %d", cfg.MaxReorderFrames, test.want)
				}
				// A frame-count override must not change the other receive/send limits.
				if cfg.MaxReorderBytes != 64<<20 || cfg.ChunkSize != 64<<10 || cfg.QueueFrames != 256 {
					t.Fatalf("unrelated limits changed: reorder_bytes=%d chunk=%d queue=%d", cfg.MaxReorderBytes, cfg.ChunkSize, cfg.QueueFrames)
				}
			})
		}
	}
}

func TestReorderFrameOptionsIndependent(t *testing.T) {
	client, err := reorderOptionsConfig(t, "client", `{"max_reorder_frames":8192}`)
	if err != nil {
		t.Fatal(err)
	}
	server, err := reorderOptionsConfig(t, "server", `{"max_reorder_frames":64}`)
	if err != nil {
		t.Fatal(err)
	}
	if client.MaxReorderFrames != 8192 || server.MaxReorderFrames != 64 {
		t.Fatalf("local limits not independent: client=%d server=%d", client.MaxReorderFrames, server.MaxReorderFrames)
	}
	status := newOutboundStatus("", outboundStatusConfig{cfg: client})
	if got := status.buildDocument(time.Now()).Node.Parameters.MaxReorderFrames; got != 8192 {
		t.Fatalf("status max_reorder_frames=%d, want 8192", got)
	}
}

func reorderOptionsConfig(t *testing.T, side string, data string) (coreConfig, error) {
	t.Helper()
	logger := log.NewNOPFactory().Logger()
	if side == "client" {
		var options option.MultipathOutboundOptions
		reorderOptionsRoundTrip(t, data, &options)
		options.Outbounds = []string{"leg0", "leg1"}
		options.Server, options.ServerPort = "127.0.0.1", 39000
		outbound, err := NewOutbound(context.Background(), nil, logger, "out", options)
		if err != nil {
			return coreConfig{}, err
		}
		return outbound.(*Outbound).cfg, nil
	}
	var options option.MultipathInboundOptions
	reorderOptionsRoundTrip(t, data, &options)
	inbound, err := NewInbound(context.Background(), nil, logger, "in", options)
	if err != nil {
		return coreConfig{}, err
	}
	return inbound.(*Inbound).cfg, nil
}

func reorderOptionsRoundTrip[T any](t *testing.T, data string, options *T) {
	t.Helper()
	if err := json.Unmarshal([]byte(data), options); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(options)
	if err != nil {
		t.Fatal(err)
	}
	var decoded T
	if err = json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	*options = decoded
}
