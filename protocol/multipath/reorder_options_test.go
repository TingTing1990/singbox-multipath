package multipath

import (
	"bytes"
	"context"
	"reflect"
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
		{name: "omitted", options: `{}`, want: 0},
		{name: "zero", options: `{"max_reorder_frames":0}`, want: 0},
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
				if cfg.MaxReorderBytes != automaticBufferLimit(cfg.Memory, cfg.ChunkSize) || cfg.ChunkSize != 64<<10 || cfg.QueueFrames != 256 {
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

func TestBandwidthOptionIgnoredWithWarning(t *testing.T) {
	for _, side := range []string{"client", "server"} {
		baseline, err := reorderOptionsConfig(t, side, `{"memory_limit":"512MB"}`)
		if err != nil {
			t.Fatal(err)
		}
		baseline.Memory = nil
		for _, value := range []string{"", `[160,750]`, `[0,0]`, `[]`, `null`, `0`, `[-1,750]`, `"legacy"`, `{"legacy":true}`} {
			t.Run(side+"/"+value, func(t *testing.T) {
				data := `{"memory_limit":"512MB"`
				if value != "" {
					data += `,"bandwidth_mbps":` + value
				}
				data += `}`
				var options any = &option.MultipathInboundOptions{}
				if side == "client" {
					options = &option.MultipathOutboundOptions{}
				}
				decoder := json.NewDecoder(strings.NewReader(data))
				decoder.DisallowUnknownFields()
				if err := decoder.Decode(options); err != nil {
					t.Fatalf("legacy field rejected: %v", err)
				}
				var output bytes.Buffer
				factory, err := log.New(log.Options{Context: context.Background(), Options: option.LogOptions{Level: "warn", DisableColor: true}, DefaultWriter: &output})
				if err != nil {
					t.Fatal(err)
				}
				defer factory.Close()
				cfg, err := reorderOptionsConfigWithLogger(t, side, data, factory.NewLogger("multipath-test"))
				if err != nil {
					t.Fatalf("legacy field prevented initialization: %v", err)
				}
				// Constructors run before logging starts; Start flushes their warnings.
				if err = factory.Start(); err != nil {
					t.Fatal(err)
				}
				cfg.Memory = nil
				if !reflect.DeepEqual(cfg, baseline) {
					t.Fatal("ignored field changed runtime configuration")
				}
				want := 0
				if value != "" {
					want = 1
				}
				if got := strings.Count(output.String(), "bandwidth_mbps is obsolete and ignored"); got != want {
					t.Fatalf("warnings=%d, want=%d; log=%q", got, want, output.String())
				}
				if want != 0 && !strings.Contains(output.String(), "WARN") {
					t.Fatalf("not a warning: %q", output.String())
				}
			})
		}
	}
}

func TestUnknownMultipathOptionsStillRejected(t *testing.T) {
	for _, options := range []any{&option.MultipathInboundOptions{}, &option.MultipathOutboundOptions{}} {
		decoder := json.NewDecoder(strings.NewReader(`{"bandwidth_mbps":[160,750],"unknown_multipath_option":true}`))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(options); err == nil || !strings.Contains(err.Error(), "unknown_multipath_option") {
			t.Fatalf("unknown field silently accepted: %v", err)
		}
	}
}

func TestAutomaticByteLimitsFollowNodeBudget(t *testing.T) {
	for _, side := range []string{"client", "server"} {
		cfg, err := reorderOptionsConfig(t, side, `{"memory_limit":"512MB"}`)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.MaxReorderFrames != 0 || cfg.MaxReorderBytes != 224<<20 || cfg.ReplayBytes != 224<<20 {
			t.Fatalf("unexpected automatic limits: %+v", cfg)
		}
		cfg, err = reorderOptionsConfig(t, side, `{"memory_limit":"64MB"}`)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.MaxReorderBytes != 28<<20 || cfg.ReplayBytes != 28<<20 {
			t.Fatal("limits did not scale with node budget")
		}
		cfg, err = reorderOptionsConfig(t, side, `{"memory_limit":"512MB","max_reorder_frames":64,"max_reorder_bytes":8388608,"leg1_replay_bytes":4194304}`)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.MaxReorderFrames != 64 || cfg.MaxReorderBytes != 8<<20 || cfg.ReplayBytes != 4<<20 {
			t.Fatal("explicit caps were overridden")
		}
	}
}

func reorderOptionsConfig(t *testing.T, side string, data string) (coreConfig, error) {
	t.Helper()
	return reorderOptionsConfigWithLogger(t, side, data, log.NewNOPFactory().Logger())
}

func reorderOptionsConfigWithLogger(t *testing.T, side string, data string, logger log.ContextLogger) (coreConfig, error) {
	t.Helper()
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
