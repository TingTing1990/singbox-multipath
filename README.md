# sing-box

The universal proxy platform.

[![Packaging status](https://repology.org/badge/vertical-allrepos/sing-box.svg)](https://repology.org/project/sing-box/versions)

## Experimental multipath

This branch adds an experimental `multipath` inbound and outbound. It carries one
logical TCP byte stream over exactly two existing reliable outbounds. Traffic starts
on the preferred, stable low-latency leg (leg 0); the secondary leg (leg 1) joins the
data path after the configured traffic or queue threshold is reached. This is an
application-layer aggregation protocol, not kernel MPTCP. UDP is not aggregated and
is delegated to one selected child outbound.

The multipath protocol does not provide authentication or encryption by itself. The
aggregation listener should only be reachable through trusted or authenticated child
paths, such as a private WireGuard path and a Hysteria2 path.

### Data path and leg roles

Leg 0 is the session anchor and preferred path. It creates the session, carries
cumulative acknowledgements and stream control, and carries all application data
before aggregation activates. This keeps connection setup and small transfers on the
configured low-latency path. Losing leg 0 closes the logical connection because the
session no longer has its control path.

Leg 1 is a capacity booster. It can attach while the connection is still using only
leg 0, but it does not carry application data until a local activation trigger fires.
Each direction makes that decision independently from its own sent-byte count,
measured rate, and leg 0 queue pressure. Once active, new frames are assigned by each
leg's queued bytes divided by its `bandwidth_mbps` weight, so a backing-up leg becomes
less attractive without delaying writes already queued on the other leg.

The logical byte stream is split into globally sequenced frames. The receiver accepts
frames from both legs, buffers only bounded out-of-order data, and writes contiguous
frames to the application. Cumulative ACKs return on leg 0. A frame assigned to leg 1
is retained once in the replay map until that ACK covers it; the queue and replay map
refer to the same payload rather than keeping duplicate copies.

### Weak leg 1 and fallback

The two legs have independent send queues and writers. A slow leg 1 therefore cannot
block the leg 0 socket or its queue. If leg 1 stops making progress, unacknowledged
frames reach `leg1_replay_timeout`; leg 1 is detached, those frames are re-injected on
leg 0 in sequence order, and the client retries leg 1 in the background. Leg 0 remains
the usable fallback path throughout this process.

Because the result is still one ordered TCP byte stream, a missing earlier frame on
leg 1 can temporarily hold later frames from the same connection until replay starts.
This head-of-line delay is bounded by the replay timeout. It does not stop leg 0 for
other connections: shared memory pressure pauses new booster work first, preserves
the final budget for leg 0, and applies backpressure only to an affected session that
still has outstanding leg 1 replay. Memory pressure alone does not close leg 1.

Payload buffers and estimated per-session overhead share one budget per multipath
inbound or outbound. With `memory_limit` omitted, the budget is
`min(512 MiB, MemAvailable * 0.5)`. Leg 1 pauses at 7/8 of the budget and resumes at
3/4; leg 0 may use the hard limit. Below the high watermark, scheduling and queueing
behavior is unchanged.

### Runtime telemetry

When the client enables `status_file`, protocol v5 requests a compact sender-status
frame from the server on leg 0. It reports the server-side downlink queues, replay and fallback counters,
write stalls, and memory pressure for the matching logical session. Status frames
are coalesced and do not consume data sequence numbers, replay space, or the payload
memory budget. The client marks remote status stale when updates stop rather than
interpreting missing telemetry as zero.

When `status_file` is enabled, the client also sends low-rate PING/PONG probes over
each attached leg. Reported RTT is the effective application-layer round trip and
therefore includes transport and proxy queueing. Probe timeouts and replay fallback
ratios describe multipath-visible stalls; they are not raw IP or UDP packet-loss
measurements. Traffic peaks are the highest one-second averages since process start,
while memory peaks are updated directly by the allocator.

At startup, each multipath inbound or outbound logs its resolved memory limit, high
and resume watermarks, and cache limit. Crossing the high watermark and recovering
below the resume watermark each emit one informational transition log.

### Client outbound example

The following example uses a system WireGuard interface for the preferred leg and an
already configured Hysteria2 outbound for the secondary leg:

```json
{
  "outbounds": [
    {
      "type": "direct",
      "tag": "wg-dedicated",
      "bind_interface": "wg1",
      "tcp_fast_open": true
    },
    {
      "type": "hysteria2",
      "tag": "hy2-public",
      "server": "hy2.example.com",
      "server_port": 443,
      "password": "change-me",
      "tls": {
        "enabled": true,
        "server_name": "hy2.example.com"
      }
    },
    {
      "type": "multipath",
      "tag": "mp-out",
      "outbounds": [
        "wg-dedicated",
        "hy2-public"
      ],
      "preferred": "wg-dedicated",
      "udp_outbound": "wg-dedicated",
      "server": "10.66.67.1",
      "server_port": 39000,
      "tcp_fast_open": true,
      "activation_threshold_mbps": 120,
      "activation_after_bytes": "2MB",
      "activation_after_bytes_min_mbps": 120,
      "activation_window": "1s",
      "chunk_size": 65536,
      "queue_frames": 256,
      "bandwidth_mbps": [
        160,
        700
      ]
    }
  ]
}
```

`tcp_fast_open` on the multipath outbound enables its early-write path: the
multipath hello and first data frame are emitted together. It does not enable TCP
Fast Open inside a child outbound. To place that first write in the TCP SYN, also
enable `tcp_fast_open` on the preferred child outbound and on the server's multipath
inbound, as shown in the examples. If the child transport does not support TCP Fast
Open, the combined early write still works but is sent after its connection is
established. When multipath `tcp_fast_open` is false or omitted, the multipath hello
is completed before the logical connection is returned.

### Server inbound example

```json
{
  "inbounds": [
    {
      "type": "multipath",
      "tag": "mp-in",
      "listen": "10.66.67.1",
      "listen_port": 39000,
      "tcp_fast_open": true,
      "activation_threshold_mbps": 120,
      "activation_window": "1s",
      "chunk_size": 65536,
      "queue_frames": 256,
      "bandwidth_mbps": [
        160,
        700
      ]
    }
  ]
}
```

Client and server scheduling parameters control traffic sent by that side and may be
tuned independently for asymmetric links. The client-requested `chunk_size` must not
exceed the server value.

### Outbound fields

| Field | Description | Accepted format / example |
| --- | --- | --- |
| `outbounds` | Exactly two child outbound tags. Both children must support TCP. | `["leg0", "leg1"]` |
| `preferred` | Child used as leg 0 before aggregation activates. Defaults to the first entry in `outbounds`. | `"leg0"` |
| `udp_outbound` | Child used for UDP without aggregation. Defaults to `preferred` and must support UDP. | `"leg0"` |
| `server` / `server_port` | Address and port of the remote multipath inbound, reachable through both children. | `"10.66.67.1"` / `39000` |
| `tcp_fast_open` | Enables the multipath early-write path. Also enable TCP Fast Open on the preferred child and server inbound for SYN data. Default: `false`. | `true` or `false` |
| `status_file` | Optional path for periodically written multipath runtime status JSON. | `"/var/run/multipath.json"` |

### Shared tuning fields

| Field | Description | Accepted format / example |
| --- | --- | --- |
| `activation_threshold_mbps` | Activates leg 1 when locally sent traffic reaches this average rate during `activation_window`. Defaults to `150` when this and `activation_after_bytes` are both unset. | Non-negative integer Mbps, e.g. `120` |
| `activation_after_bytes` | Optional total locally sent byte count that activates leg 1. It is an alternative trigger to the rate and queue triggers. | Non-negative integer or memory string, e.g. `2097152` or `"2MB"` |
| `activation_after_bytes_min_mbps` | Optional recent-rate gate for `activation_after_bytes`. When non-zero, the byte trigger also requires the measured rate over a complete `activation_window` to reach this value. It does not change the throughput or leg 0 queue triggers. | Non-negative integer Mbps, e.g. `120` |
| `activation_window` | Rate sampling window and sustained high-queue trigger duration. Default: `1s`. | Duration, e.g. `"1s"` |
| `chunk_size` | Maximum payload per multipath data frame, from 1 KiB to 1 MiB. Default: 64 KiB. | Non-negative integer bytes, e.g. `65536` |
| `queue_frames` | Per-leg send queue capacity in frames, from 8 to 4096. `chunk_size * queue_frames` must not exceed 64 MiB. Default: 256. | Non-negative integer, e.g. `256` |
| `bandwidth_mbps` | Optional two-entry scheduling weights in leg 0/leg 1 order. Values express the expected relative capacity and are not rate limits. | Two-entry integer array, e.g. `[160, 700]` |
| `max_reorder_frames` | Server-only limit for buffered out-of-order frames. Default: 2048. | Non-negative integer, e.g. `2048` |
| `max_reorder_bytes` | Limit for buffered out-of-order data. Default: 64 MiB; maximum: 512 MiB. | Non-negative integer bytes, e.g. `67108864` |
| `leg1_replay_bytes` | Maximum retained send history used to recover data assigned to a stalled leg 1. Default: 64 MiB; maximum: 512 MiB. | Non-negative integer bytes, e.g. `67108864` |
| `leg1_replay_timeout` | Time before unacknowledged leg 1 data is replayed on leg 0. Default: `5s`. | Duration, e.g. `"5s"` |
| `memory_limit` | Shared memory budget for all sessions of this multipath inbound or outbound. Defaults to `min(512 MiB, MemAvailable * 0.5)`. Booster backpressure starts at 7/8 and clears at 3/4. | Non-negative integer or memory string, e.g. `268435456` or `"256MB"` |
| `handshake_timeout` | Timeout for a multipath leg handshake. Default: `10s`. | Duration, e.g. `"10s"` |

The server inbound also accepts the standard sing-box listen fields, including
`listen`, `listen_port`, and `tcp_fast_open`.

Byte fields that accept a memory string use binary units: for example, `"2MB"`
means 2 MiB. Bare integers remain supported and are interpreted as bytes.

## Documentation

https://sing-box.sagernet.org

## License

```
Copyright (C) 2022 by nekohasekai <contact-sagernet@sekai.icu>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.

In addition, no derivative work may use the name or imply association
with this application without prior consent.
```
