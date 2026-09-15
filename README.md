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

Both endpoints must use multipath protocol **v9**. Older protocol versions are
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
measured rate, and leg 0 backlog. After activation, the scheduler compares outstanding
path bytes divided by observed delivery rate. Receipt feedback comes from the far
multipath endpoint, so outstanding bytes include buffering inside a local proxy and
its remote transport. Completing a local socket write is not delivery confirmation.
There are no configured bandwidth weights or rate limits.

An unmeasured path starts with a bounded probe. Feedback establishes its observed
delivery rate and timing, not a guaranteed estimate of unused capacity. Once sampled,
the shared receive window, send-history budget and child write backpressure bound
assignment; there is no second per-path congestion window. Busy or stalled writers
do not block assignment to another eligible path. The preferred-only phase uses
normal child backpressure without the discovery-probe limit. Packet-level congestion
control, pacing, and retransmission remain in the child: this protocol does not
replace Hysteria2's congestion controller with an outer TCP one.

`aggregation_enabled` controls only the local sending direction: client upload on
an outbound, server download on an inbound. With it disabled, all locally sent
application data stays on leg 0. Leg 1 can still attach and receive data when the
peer enables aggregation, and the selected UDP outbound is unaffected.

With aggregation enabled, the following triggers are independent alternatives
(OR), evaluated separately for each connection and sending direction:

- Queue: `activation_on_queue` is enabled and leg 0 in-flight plus local unsent
  bytes stay at least 80% of `queue_frames * chunk_size` for `activation_window`.
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

### Byte stream, receive window, and recovery

Both paths carry mappings into one 64-bit **byte** sequence space. Each path also
has a separate sequence space and incarnation ID. A receiver deduplicates overlapping
mappings and returns a cumulative Data ACK for the contiguous received prefix.
That ACK means the receiver owns the bytes, not that the application has read them.
DATA mappings are processed incrementally: an arriving prefix can be delivered and
acknowledged before the remaining payload of the same mapping reaches the receiver.
Only Data ACK releases the sender's connection-level history, which retains data
from **both** paths. An original or recovery writer holds an independent reference,
so acknowledgement cannot free a buffer while a blocked child still uses it.

The receiver advertises one monotonic byte-window right edge shared by both paths.
Application reads move the window; individual path receipts do not. Short writes
consume their actual byte length, not a whole frame credit. Local unsent capacity,
whole-path in-flight bytes, retained send history, and receive storage are separate
states. There are no idle-credit epochs, weight-based quotas, or separate leg 1
replay ownership rules.

Receive storage uses sparse 16 KiB pages, allocated only for arriving bytes. Under
memory pressure, a receiver may decline speculative data or prune wholly
unacknowledged out-of-order pages. It never discards Data-ACKed bytes waiting for the
application. Path receipts still report transport delivery; they do not release the
connection history. If a received mapping still covers the missing connection head,
the sender can reinject it from that history. Each admitted session has reserved
reader scratch, one head receive page, and a reusable primary TX buffer, so a missing
head is not dependent on leg 1 releasing speculative storage.

### Weak leg 1 and fallback

Each path has an independent writer. A blocked leg 1 write does not hold the state
lock, prevent leg 0 writes, or stop receive/control processing. Recovery reuses the
same immutable byte history, does not consume new connection-window space, and does
not need another payload allocation. Late originals are harmless duplicates.

Whole-path receipts drive delivery-rate and timing estimates. A leg without progress
is marked stale and pauses new assignments; retained mappings can be reinjected on
another available path. Receipt progress clears the stale state. A stall is not an
automatic disconnect/reconnect, and a hard secondary failure does not close the
logical stream. An individual leg receipt above a missing global byte is ordinary
reordering, not evidence that the missing byte was lost.

The adaptive no-progress interval is smoothed delivery RTT plus four times its
variation, at least 200 ms, initially one second before measurements are available.
An explicit `leg1_replay_timeout` supplies an additional lower bound. Buffering in
child transports is included in timing measurements. Control/window updates take
priority over DATA not yet submitted to the primary child; they cannot overtake an
already blocked child write or bytes already queued inside a reliable transport.

These mechanisms preserve a usable leg 0 through secondary stalls and failures.
They cannot guarantee the same latency as leg 0 alone after data have already been
assigned to a slow path: detecting and recovering an earlier missing byte takes
time and bandwidth. Aggregation also cannot exceed shared physical bottlenecks.

Payload storage, path/mapping metadata, reserved progress buffers, cache, and estimated
session overhead share one budget per multipath inbound or outbound. The default is
`min(512 MiB, MemAvailable * 0.5)`. New booster assignments and ordinary receive-window
growth pause at 7/8 and resume below 3/4; head progress remains reserved. Advertised
but unused window space is not an allocation. This budget is not process RSS and
does not include child TCP/QUIC buffers.

Omitted receive/send-history ceilings are derived from the node budget: half of
its ordinary allocation region (7/16 of the total, capped at 512 MiB and at least
one chunk). With a 512 MiB budget this is 224 MiB per direction. These are ceilings,
not allocations or per-session reservations; concurrent sessions still share the
same global allocator. Explicit byte limits remain hard caps. An omitted
`max_reorder_frames` adds no independent chunk-count ceiling.

The byte-sequence, Data ACK, shared-window, reinjection, and DATA_FIN model follows
[RFC 8684](https://www.rfc-editor.org/rfc/rfc8684.html). The delivery-based scheduling
basis follows [Linux MPTCP](https://github.com/torvalds/linux/blob/587858367581b9c55c3690f4e63382ad622719d4/net/mptcp/protocol.c).
This is an independent implementation over reliable proxy streams, not MPTCP wire
compatibility, native subflow TCP congestion control, or an implementation of every
optional MPTCP path-management/security mechanism.

### Connection shutdown

DATA_FIN occupies one byte-sequence position at the end of each sending direction.
A cumulative Data ACK covers it only after all preceding bytes have been received. An application
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

When the client enables `status_file`, protocol v9 requests a compact sender-status
frame from the server on leg 0. It reports the server-side downlink queues, replay and fallback counters,
write stalls, and memory pressure for the matching logical session. Status frames
are coalesced and do not consume data sequence numbers, replay space, or the payload
memory budget. The client marks remote status stale when updates stop rather than
interpreting missing telemetry as zero.

Active paths send low-rate PING/PONG probes independently of `status_file` for
observability. Recovery timing uses DATA receipt samples, not probe success alone.
An idle unused secondary is not probed just because it is attached. Reported RTT is the effective application-layer round trip and
therefore includes transport and proxy queueing. Probe timeouts and reinjection/stall
counters describe multipath-visible events; they are not raw IP or UDP packet-loss
measurements. Traffic peaks are the highest one-second averages since process start,
while memory peaks are updated directly by the allocator.

Leg joins count successfully attached transports independently of local TX
activation. Joins, attempts and reported remote failures retain closed-session
totals; probe statistics cover active connections only. A lazy primary transport
can be attached before its deferred handshake finishes. Remote failure totals
include only events actually reported by the peer.

Leg events include `last_error_source`: `local_endpoint`, `remote_endpoint`,
`transport`, `shutdown`, or `unknown`. Closing a healthy logical connection marks
an application-endpoint shutdown; a preceding multipath failure keeps its original
source. Session-close frames carry this provenance to the peer on both legs.
Only close-related I/O errors inherit endpoint attribution; timeouts and protocol
errors remain visible. A missing close marker leaves the source unknown rather
than guessing from EOF, reset or QUIC cancellation text. The marker is diagnostic
only: it does not change FIN handling, scheduling, recovery or close timing.
Status schema 3 adds the source field. Confirmed endpoint-close events do not
increment leg failure/event counters; other events, including unattributed and
harmless closures, retain their existing counting semantics. Protocol v9 requires
updating both endpoints.

Remote scheduler rate estimates are not one-second throughput or physical link
capacity. DATA-feedback RTT follows the selected data leg outward and leg0 for
the return feedback. Stall detection need not result in reinjection, and sender
backpressure duration is accumulated across connections, not a single pause.

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
      "queue_frames": 256
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
      "queue_frames": 256
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

Unless stated otherwise, tuning limits apply per logical TCP connection. The memory budget is shared by all sessions of
one multipath inbound or outbound. These TCP tuning fields do not configure UDP
forwarding or the child transports' TCP/QUIC buffers and congestion control.

Most settings are local and independent: the server does not push its activation,
queue, replay, or memory configuration to the client. Receive limits are
also configured locally, but constrain the byte window advertised to the peer. Telemetry
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
The client has 1 MiB of local unsent capacity per connection; the server has 4 MiB.
This does not cap whole-path in-flight data. Already assigned bytes stay in the
connection's send history until Data ACK; each path writer holds at most one
assignment. Reinjection references history rather than a separate recovery queue.

`max_reorder_frames` is a capacity in negotiated chunk-size units, not a count of
received wire frames. Together with `max_reorder_bytes` it sets the local RX byte
window: `min(max_reorder_frames * chunk_size, max_reorder_bytes)`. With 16 KiB chunks,
an explicit 2048 units mean 32 MiB. Short mappings consume only their actual byte range. Both
limits include in-order data awaiting application reads; sparse page allocation and
the shared memory budget separately control actual storage.

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
| `activation_on_queue` | **Local TX**; not negotiated. | **Condition 1**, an independent OR trigger: primary path in-flight plus local unsent bytes stay at least 80% of `chunk_size * queue_frames` for `activation_window`. Default: `true`. | `true` or `false` |
| `activation_threshold_mbps` | **Local TX** ingress rate per connection; not negotiated. | **Condition 2**, an independent OR trigger measured over `activation_window`. Explicit `0` disables it. If omitted, defaults to `150` when the byte trigger is disabled, otherwise `0`. | Non-negative integer Mbps, e.g. `120` or `0` |
| `activation_after_bytes` | **Local TX** cumulative application bytes per connection; not negotiated. | **Condition 3**, an independent OR trigger. Counts bytes accepted into the local multipath sender, not peer delivery or combined RX/TX traffic. `0` or omitted disables it. | Non-negative integer or memory string, e.g. `2097152`, `"2MB"`, or `0` |
| `activation_after_bytes_min_mbps` | **Local TX** rate gate for condition 3 only; not negotiated. | The byte threshold and this average rate over a complete `activation_window` must both be met. `0` or omitted removes the gate. Conditions 1 and 2 remain independent. | Non-negative integer Mbps, e.g. `120` or `0` |
| `activation_window` | **Local TX** measurement / trigger timing; not negotiated. | Rate sampling window and sustained high-queue duration. Default: `1s`. Does not set a receive or replay timeout. | Duration, e.g. `"1s"` |
| `chunk_size` | **Both TX and RX**, client-requested and server-accepted per session. | Client value is the requested maximum frame payload for both directions; server value is the maximum acceptable request. Oversized requests are rejected, not reduced. Range: 1 KiB to 1 MiB; default: 64 KiB. | Non-negative integer bytes, e.g. `16384` or `65536` |
| `queue_frames` | **Local TX**, per connection; not negotiated. | Local unsent capacity in negotiated chunk-size units. Range: 8–4096; configured product at most 64 MiB. Not a per-leg in-flight limit or receive window. Default: `256`. | Non-negative integer, e.g. `64` or `256` |
| `max_reorder_frames` | **Local RX**; window constrains peer TX. Client: download; server: upload. | Window capacity in negotiated chunk-size units, limited further by `max_reorder_bytes`. Not a count of wire frames. Omitted / `0`: no additional chunk-count ceiling. Explicit range: 64–65536. | Non-negative integer, e.g. `2048` or `8192` |
| `max_reorder_bytes` | **Local RX**, per connection; window constrains peer TX. | Maximum receive-window byte span, including unread in-order data. Sparse storage is allocated on arrival; small frames do not consume a full chunk. Default: budget-derived (7/16 of total, capped at 512 MiB); explicit maximum: 512 MiB. | Non-negative integer bytes, e.g. `67108864` |
| `leg1_replay_bytes` | **Local TX**, per connection; not negotiated. | Historical field name; now limits the connection send history for **both** paths, including unsent bytes. Only peer Data ACK releases history. Separate from peer RX capacity. Default: budget-derived (7/16 of total, capped at 512 MiB); explicit maximum: 512 MiB. | Non-negative integer bytes, e.g. `67108864` |
| `leg1_replay_timeout` | **Local TX** recovery timing; not negotiated. | Historical field name; optional floor on adaptive path no-progress detection. Stale paths pause assignment without automatic disconnection. Omitted / `"0s"`: adaptive. Explicit non-zero range: `100ms`–`5m`. Not a total recovery deadline. | Duration, e.g. `"0s"` or `"1s"` |
| `memory_limit` | **Local TX and RX**, shared across sessions of one inbound/outbound; independently resolved. | Budget for actual storage, cache, metadata and reserved progress buffers, not unused advertised windows, RSS, or child buffers. Default: `min(512 MiB, MemAvailable * 0.5)`. Booster/ordinary window growth pauses at 7/8 and resumes below 3/4. | Non-negative integer or memory string, e.g. `268435456` or `"256MB"` |
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
gate. `bandwidth_mbps` no longer affects scheduling. For configuration migration,
both inbound and outbound accept this legacy field, ignore its value, and emit one
warning per node initialization. Remove it from existing configurations when convenient.
Other unknown fields still fail strict validation; this does not enable old wire protocols.

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
