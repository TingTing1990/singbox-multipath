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

Both endpoints must use multipath protocol **v7**. Older protocol versions are
rejected; there is no compatibility mode.

### Data path and leg roles

Leg 0 is the session anchor and preferred path. It creates the session, carries
cumulative acknowledgements and stream control, and carries all application data
before aggregation activates. This keeps connection setup and small transfers on the
configured low-latency path. Losing leg 0 closes the logical connection because the
session no longer has its control path.

Leg 1 is a capacity booster. It can attach while the connection is still using only
leg 0, but it does not carry application data until a local activation trigger fires.
Each direction makes that decision independently from its own accepted-byte count,
measured rate, and leg 0 queue pressure. Once active, new frames are assigned by each
leg's queued bytes divided by its `bandwidth_mbps` weight, so a backing-up leg becomes
less attractive without delaying writes already queued on the other leg.

`aggregation_enabled` controls only the local sending direction: client upload on
an outbound, server download on an inbound. With it disabled, all locally sent
application data stays on leg 0. Leg 1 can still attach and receive data when the
peer enables aggregation, and the selected UDP outbound is unaffected.

With aggregation enabled, the following triggers are independent alternatives
(OR), evaluated separately for each connection and sending direction:

- Queue: `activation_on_queue` is enabled and leg 0 backlog stays at least 80% of
  its queue byte capacity for `activation_window`.
- Rate: `activation_threshold_mbps` is greater than zero and the average local
  ingress rate over `activation_window` reaches it.
- Bytes: `activation_after_bytes` is greater than zero and the local TX accepted-byte
  count reaches it. If `activation_after_bytes_min_mbps` is non-zero, this trigger
  also requires that average rate over a complete `activation_window`.

The minimum byte-trigger rate does not gate the queue or rate triggers. Disabling
all three triggers keeps local TX on leg 0; zero thresholds never imply immediate
activation. Once activated, aggregation does not automatically deactivate when
traffic drops. Explicit zero disables a numeric trigger. An omitted rate threshold
retains the default of 150 Mbps when the byte trigger is disabled, or zero when a
non-zero byte trigger is configured.

The logical byte stream is split into globally sequenced frames. The receiver accepts
frames from both legs, buffers only bounded out-of-order data, and writes contiguous
frames to the application. Cumulative receipt ACKs return on leg 0 as part of window
updates; they acknowledge data held by the receiver, not application consumption.
A frame assigned to leg 1
is retained once in the replay map until that ACK covers it; the queue and replay map
refer to the same payload rather than keeping duplicate copies.

### Weak leg 1 and fallback

Each receiver grants a connection-level window shared by both legs. Every credit
reserves one full negotiated chunk plus frame metadata in the local memory budget.
The window is bounded by both `max_reorder_frames` and `max_reorder_bytes`, including
in-order data waiting for the application. The sender cannot assign new frames
beyond the granted window, so ordinary reordering or exhausted credit applies
backpressure instead of resetting the logical session. Malformed frames and data
outside the granted window remain protocol errors.

Receipt ACKs and application-consumption credit are separate. A slow application
stops the window from growing without blocking control processing or being mistaken
for lost leg 1 data. Each admitted session retains one reusable receive credit and
budgeted reader scratch space and a reusable leg0-only TX buffer. Leg 0 can therefore
deliver a missing frame even if leg 1 is stuck partway through the original frame.
After 250 ms without new TX data,
the sender returns unused credit using an epoch change, retaining one immediately
usable slot. Stale window updates cannot restore returned credit, but their
receipt ACKs, FIN acknowledgement and leg 1 progress remain valid. Returning
credit therefore does not interrupt acknowledgement processing while the return
request is still queued in a child transport.

Startup credit is advertised before the first DATA frame. With early write,
hello, startup credit and first DATA share one physical write; advertising credit
does not start a lazy child connection on its own. The startup grant is at most
`min(2 * queue_frames * chunk_size, 32 MiB)`, subject to local receive limits and
the shared budget. Speculative grants stop once local budget usage reaches 1/8
of `memory_limit`; every admitted session still retains its guaranteed first slot.
Further window growth is demand-driven. Its per-session target shares half of the
booster budget, excluding the buffer cache, between live sessions. Already granted
credit is never revoked unilaterally. This leaves budget for TX and session
overhead while allowing a few busy connections to use large receive windows.

The two legs have independent writers. Control/window updates have priority on leg
0, followed by a dedicated recovery queue, then ordinary data. The receiver also
reports the highest complete leg 1 DATA sequence independently of the cumulative
ACK. A leg 0 gap therefore does not make already received leg 1 data look lost.
Only the cumulative ACK releases replay storage.

When the earliest unreceived frame belongs to stalled leg 1, recovery re-injects
retained frames on leg 0 in sequence order, at most 16 frames / 1 MiB per 20 ms
pass (at least one frame), without waiting for recovery-queue space. New booster
assignments pause until delivery progress resumes; already queued originals are
not dropped unless that specific frame has been reassigned. A hard transport
failure still schedules all remaining retained frames for recovery immediately.
Recovery does not wait for the original writer to
finish, and it does not immediately close leg 1. Queue, original-write, and replay
references keep a shared payload alive until every user has released it; late
duplicates are discarded by sequence number.

`leg1_replay_timeout` defaults to **1 second** and is a lower bound on the recovery
interval, not a queue-residence deadline. The timer starts when an original DATA
write starts, or when a control write blocks queued DATA, and restarts on observed
leg 1 delivery progress. RTT/jitter estimates and twice the smoothed DATA
write-to-receipt time may lengthen this interval; they never shorten the configured
value. This accounts for buffering inside child transports as well as the network.
A leg with no subsequently observed delivery progress is detached only after
`max(2s, 5 * effective recovery interval)` from recovery starting; the client then
retries the leg. The stall timer clears once no unacknowledged data or blocked write remains;
an idle leg is not detached merely because its delivery counters stop increasing.

This preserves the logical connection through booster stalls, disconnections, and
receive-window pressure while leg 0 can still carry data. It does not guarantee
identical latency to a standalone leg 0: detecting a missing earlier frame and
delivering its replacement still take time, and retransmissions consume bandwidth.
Bytes already written into a child transport cannot be reprioritized.

Payloads, granted receive credit, reader scratch/TX reserve space, and estimated
per-session overhead share one budget per multipath inbound or outbound. With `memory_limit`
omitted, the budget is `min(512 MiB, MemAvailable * 0.5)`. New booster work and window
growth pause at 7/8 of the budget and resume at 3/4; admitted sessions retain their
leg 0 progress slot. Memory pressure alone does not close leg 1. Reported budget
usage includes unused promised credit and is not a measurement of process RSS.

### Connection shutdown

FIN identifies the final sequence of each sending direction. A window update
acknowledges FIN only after every preceding frame has been received. An application
close rejects further local I/O but drains accepted TX in the background; session-close
is sent only after the peer acknowledges that final sequence. Half-closing one
direction leaves the reverse direction usable, including through connection wrappers.

If FIN and all its preceding data have arrived, a subsequent session-close or control
leg transport failure preserves buffered RX until the application reads it. Receipt ACKs therefore
remain valid even when the application is slow. Session accounting is finalized after
that drain and buffer release. Errors, resets, and service shutdown can still abort
immediately; a local application close may discard its own unread RX. Normal draining
uses protocol confirmation rather than a fixed delay and remains subject to peer
backpressure and child transport failure. Service shutdown can interrupt a drain.

Application read/write deadlines also cover the initial fast-open write wait.
An application timeout does not reset the session; the accepted prefix remains
queued and the caller can resume with the unwritten suffix after clearing or
extending the deadline. An incomplete stream terminated without FIN reports an
error rather than a clean EOF, while local close interrupts pending application I/O.

### Runtime telemetry

When the client enables `status_file`, protocol v7 requests a compact sender-status
frame from the server on leg 0. It reports the server-side downlink queues, replay and fallback counters,
write stalls, and memory pressure for the matching logical session. Status frames
are coalesced and do not consume data sequence numbers, replay space, or the payload
memory budget. The client marks remote status stale when updates stop rather than
interpreting missing telemetry as zero.

Active aggregation sessions send low-rate PING/PONG probes independently of
`status_file` so recovery has RTT samples. Client status collection also probes
attached legs. Reported RTT is the effective application-layer round trip and
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
      "aggregation_enabled": true,
      "activation_on_queue": true,
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
      "aggregation_enabled": true,
      "activation_on_queue": true,
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

### Parameter direction and negotiation

Here, **client** means the multipath outbound and **server** means the multipath
inbound. TX and RX are relative to the side where a field is configured:

| Local scope | Client outbound | Server inbound |
| --- | --- | --- |
| **Local TX** | Upload towards the aggregation server | Download towards the client |
| **Local RX** | Download received from the server | Upload received from the client |

Unless stated otherwise, tuning limits apply per logical TCP connection; send
queues additionally apply per leg. The memory budget is shared by all sessions of
one multipath inbound or outbound. These TCP tuning fields do not configure UDP
forwarding or the child transports' TCP/QUIC buffers and congestion control.

Most settings are local and independent: the server does not push its activation,
queue, weight, replay, or memory configuration to the client. Receive limits are
also configured locally, but constrain the credit advertised to the peer. Telemetry
reports remote values without applying them to local configuration.

`chunk_size` is agreed during the hello exchange: the client requests one maximum frame payload size
for the session. The server accepts that exact value if it does not exceed the
server's configured limit, otherwise it rejects the connection; it does not silently
reduce an oversized request. The accepted size is used for **both upload and
download**, and both legs must use the same session value. Actual frames may be
smaller. Enabling client `status_file` also sends a hello flag requesting server
telemetry; the file path itself is not sent.

For example, with client `chunk_size: 16384, queue_frames: 64` and server
`chunk_size: 65536, queue_frames: 256`, both directions use frames of at most 16 KiB.
Each client leg has a pending-send limit of 64 frames / 1 MiB; each server leg has
a limit of 256 frames / 4 MiB. These figures exclude the frame currently being
written, replay/receive storage, and child transport buffers. Leg 0 additionally has
a priority recovery queue of `queue_frames` slots referencing retained payloads;
it does not duplicate their storage. Receive buffering uses the granted window,
not a `queue_frames`-sized handoff channel.

`max_reorder_frames` independently bounds local RX: client download on the outbound,
server upload on the inbound. Both sides default to 2048. The maximum receive
window is `min(max_reorder_frames, floor(max_reorder_bytes / chunk_size))` chunks;
actual credit can be smaller due to shared memory availability and sender demand.
With 16 KiB chunks, 2048 slots represent 32 MiB, not 64 MiB. Small frames consume a
whole slot because their buffer allocation still has full chunk capacity. Raising
only `max_reorder_bytes` does not increase a frame-limited window.

### Outbound fields

| Field | Scope and peer interaction | Description | Accepted format / example |
| --- | --- | --- | --- |
| `outbounds` | Client path selection for **both directions**; tags are local, while hello messages identify the leg roles. | Exactly two child outbound tags. Both children must support TCP. | `["leg0", "leg1"]` |
| `preferred` | Client assigns the shared leg 0 role; the server uses the leg IDs supplied by the client. | Session anchor, initial data path, and fallback path. Defaults to the first entry in `outbounds`; changing it affects both directions' path roles, not just upload scheduling. | `"leg0"` |
| `udp_outbound` | Client UDP path selection, separate from the multipath TCP session; not negotiated. | Outbound used for UDP requests and replies without aggregation. Defaults to `preferred` and must support UDP; may name an outbound outside the two TCP legs. | `"leg0"` |
| `server` / `server_port` | Client connection destination for both legs; must reach the server listener. | Address and port of the remote multipath inbound. These are not the final application destination. | `"10.66.67.1"` / `39000` |
| `tcp_fast_open` | Client connection setup / early TX; not a negotiated multipath flag. The server's same-named listen option has a different role. | Enables the multipath early-write path. Also enable TCP Fast Open on the preferred child and server inbound for SYN data. Default: `false`. | `true` or `false` |
| `status_file` | Client-local file containing local TX/RX statistics and requested remote sender telemetry. | Optional path for periodically written status JSON. Enables client status probes and requests server TX statistics; the server does not use or write this path. Active-session recovery probes do not require it. | `"/var/run/multipath.json"` |

### Tuning fields and direction

All fields below are available on both sides.

| Field | Scope and peer interaction | Description | Accepted format / example |
| --- | --- | --- | --- |
| `aggregation_enabled` | **Local TX**, independent on each side; not negotiated. | `false` keeps local application data on leg 0 without disabling peer TX aggregation, local RX over leg 1, or UDP. Default: `true`. | `true` or `false` |
| `activation_on_queue` | **Local TX** leg 0 backlog; not negotiated. | **Condition 1**, an independent OR trigger: backlog (queued plus currently writing payload) stays at least 80% of the local send-queue byte capacity for `activation_window`. Default: `true`. | `true` or `false` |
| `activation_threshold_mbps` | **Local TX** ingress rate per connection; not negotiated. | **Condition 2**, an independent OR trigger measured over `activation_window`. Explicit `0` disables it. If omitted, defaults to `150` when the byte trigger is disabled, otherwise `0`. | Non-negative integer Mbps, e.g. `120` or `0` |
| `activation_after_bytes` | **Local TX** cumulative application bytes per connection; not negotiated. | **Condition 3**, an independent OR trigger. Counts bytes accepted into the local multipath sender, not peer delivery or combined RX/TX traffic. `0` or omitted disables it. | Non-negative integer or memory string, e.g. `2097152`, `"2MB"`, or `0` |
| `activation_after_bytes_min_mbps` | **Local TX** rate gate for condition 3 only; not negotiated. | The byte threshold and this average rate over a complete `activation_window` must both be met. `0` or omitted removes the gate. Conditions 1 and 2 remain independent. | Non-negative integer Mbps, e.g. `120` or `0` |
| `activation_window` | **Local TX** measurement / trigger timing; not negotiated. | Rate sampling window and sustained high-queue duration. Default: `1s`. Does not set a receive or replay timeout. | Duration, e.g. `"1s"` |
| `chunk_size` | **Both TX and RX**, client-requested and server-accepted per session. | Client value is the requested maximum frame payload for both directions; server value is the maximum acceptable request. Oversized requests are rejected, not reduced. Range: 1 KiB to 1 MiB; default: 64 KiB. | Non-negative integer bytes, e.g. `16384` or `65536` |
| `queue_frames` | **Local TX**, per leg; local and not negotiated. | Ordinary pending-send capacity of 8 to 4096 frames per leg; leg 0 has an additional priority recovery queue with the same slot count. Ordinary byte capacity is accepted `chunk_size * queue_frames`; the configured product must not exceed 64 MiB. Also influences sender window demand, but does not set peer RX limits. Default: `256`. | Non-negative integer, e.g. `64` or `256` |
| `bandwidth_mbps` | **Local TX** scheduler only; not negotiated. | Optional relative capacity weights, not rate limits. On the client, entries follow the original `outbounds` order and move with the selected `preferred` child; on the server, entries are in leg 0 / leg 1 order. An omitted array uses `[1, 1]`; each `0` entry uses weight `1`. | Two-entry non-negative integer array, e.g. `[160, 700]` or `[0, 0]` |
| `max_reorder_frames` | **Local RX**, independently configured; constrains window credit advertised to peer TX. Client value controls download; server value controls upload. | Maximum receive-window slots per connection, including in-order data awaiting application consumption. Credit exhaustion pauses new sends instead of closing the session. Default: `2048`; range on both sides: 64 to 65536. | Non-negative integer, e.g. `2048` or `8192` |
| `max_reorder_bytes` | **Local RX** per connection; constrains window credit advertised to peer TX. | Client value controls download; server value controls upload. Maximum receive-window payload allocation, charged in full negotiated chunks even for short frames. Default: 64 MiB; maximum: 512 MiB. The frame limit and shared memory budget can further restrict the window. | Non-negative integer bytes, e.g. `67108864` |
| `leg1_replay_bytes` | **Local TX** retained leg 1 payload per connection; not negotiated. | Client history recovers upload; server history recovers download. Released by cumulative ACKs from the peer on leg 0. Default: 64 MiB; maximum: 512 MiB. This is not the peer's receive window. | Non-negative integer bytes, e.g. `67108864` |
| `leg1_replay_timeout` | **Local TX** recovery timeout for leg 1 data; not negotiated. | Minimum no-progress interval after an original write starts, not queue residence. RTT and DATA receipt timing may extend it. A missing leg 1 frame starts bounded replay on leg 0 without immediately closing leg 1; resumed delivery stops recovery. Persistent stalls detach the leg after `max(2s, 5 * effective interval)` from recovery starting. Not a bound on total recovery time. Default: `1s`; range: `100ms` to `5m`. | Duration, e.g. `"1s"` |
| `memory_limit` | **Local TX and RX**, shared across all sessions of one inbound or outbound; independently resolved on each side. RX credit reflects available local budget. | Budget for payloads, promised receive credit, reader scratch and estimated session overhead, not whole-process RSS or child transport buffers. Default: `min(512 MiB, MemAvailable * 0.5)`. Booster/window growth backpressure starts at 7/8 and clears at 3/4. Does not replace per-session receive limits. | Non-negative integer or memory string, e.g. `268435456` or `"256MB"` |
| `handshake_timeout` | **Local connection setup**, covering hello reads/writes rather than application TX/RX; not negotiated. | Client limits the hello exchange and each secondary dial-plus-handshake attempt; the initial preferred-child dial uses its own context/child settings. Server applies a deadline while handling each accepted leg's hello. Default: `10s`; range: `1s` to `1m`. | Duration, e.g. `"10s"` |

### Server listen fields

The inbound also accepts the standard sing-box listen options. They configure the
local listener and do not override client outbound or child transport options.

| Field | Scope and peer interaction | Description | Accepted format / example |
| --- | --- | --- | --- |
| `listen` / `listen_port` | Server-local listening endpoint for both bidirectional legs; not a multipath negotiation setting. | Both client-selected paths must be able to reach this listener. | `"10.66.67.1"` / `39000` |
| `tcp_fast_open` | Server TCP listener setup, not the client's multipath early-write switch or the server's data scheduler. | Enables TCP Fast Open on the listening socket. Actual SYN data also requires support and configuration on the TCP peer/child transport and operating system. Default: `false`. | `true` or `false` |

For `chunk_size`, `queue_frames`, reorder limits, replay bytes, and `memory_limit`,
omission or `0` selects the default/automatic value; it does not disable buffering.
For `activation_window`, `leg1_replay_timeout`, and `handshake_timeout`, omission or
`"0s"` selects the default. This differs from the activation thresholds and the
condition-3 minimum rate, where explicit `0` disables the corresponding trigger or
gate, and from `bandwidth_mbps`, where `0` means weight `1`.

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
