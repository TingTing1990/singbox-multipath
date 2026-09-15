# Multipath beta5: a connection-level byte stream

Status: implemented on the isolated `multipath-beta5` development branch; not a
release. See `multipath-beta5-validation.md` for validation and remaining failures.

## Reference baseline

The protocol baseline is RFC 8684 (including verified erratum 6609). RFC 6182
provides the architectural separation, RFC 6356 describes coupled congestion
control, and RFC 6897 specifies the application-facing byte-stream semantics.
The implementation reference is Linux commit
`587858367581b9c55c3690f4e63382ad622719d4`, especially `net/mptcp/protocol.c`,
`options.c`, `subflow.c`, `pm.c`, and `sched.c`. This is a reimplementation of
mechanisms, not an import of Linux source code.

The relevant mechanisms are not a fixed-ratio scheduler:

| MPTCP mechanism | Application in this reliable-stream overlay |
| --- | --- |
| MP_CAPABLE, keys, initial data sequence number | A versioned primary-leg hello creates the connection; no negotiation with older multipath versions. |
| MP_JOIN, token, nonces, HMAC | A secondary transport joins an existing connection, with an incarnation separate from logical connection state. A bearer session identifier is not equivalent to MPTCP's authenticated MP_JOIN. |
| DSS mapping | Every payload maps a byte range in a path stream to an immutable byte range in the logical stream. Reinjection changes the path mapping, not the data sequence number. |
| Data ACK | A cumulative, monotonic acknowledgement of the contiguous received prefix. It is independent of application consumption and individual-path receipts. |
| Subflow ACK | Explicit peer feedback measures progress through the entire child path. A successful local Write, or TCP_INFO for a proxy's local TCP hop, is not an end-to-end receipt. |
| Connection receive window | One byte-addressed window shared by both paths. Its advertised right edge never retreats. Application consumption advances available space. |
| Send/retransmit queue | One connection-level queue for both paths. Data remain owned until Data ACK, and while any transport writer still references them. |
| Out-of-order receive queue | Byte ranges, duplicate suppression and overlap handling. Receipt acknowledgement transfers ownership to the receiver; acknowledged unread bytes cannot subsequently be evicted. |
| Receive-memory pressure | Stop window growth first. Linux may prune unacknowledged out-of-order data, but never acknowledged application data; a port must preserve this distinction and reserve progress for the receive head. |
| Default Linux send scheduler | Select an eligible path using outstanding work and observed path service rate; do not use a manually configured ratio or local socket queue emptiness as a delivery estimate. |
| Reinjection and stale paths | Repair the oldest unacknowledged connection data on a usable path. A temporarily stalled path is not a failed logical connection and need not be closed and reconnected. |
| DATA_FIN | An ordered sequence-space event consuming one logical byte. A path EOF is not a logical EOF. Read and write shutdown remain independent. |
| MP_FASTCLOSE and MP_TCPRST | Separate logical abort from failure of one transport. Only a validated logical terminal event may terminate the entire stream. |
| ADD_ADDR, REMOVE_ADDR, MP_PRIO | Configured child outbounds replace address discovery. Directional activation and primary/booster policy remain separate from delivery correctness. |
| MP_FAIL, DSS checksum, infinite mapping | Middlebox translation and fallback to legacy TCP are not part of this framed protocol. TCP/QUIC already provides transport integrity; malformed mappings remain protocol errors. |
| LIA/coupled congestion control | Do not layer RFC 6356 over already reliable child streams. Its packet ACK, loss, cwnd and MSS inputs are not available here. Child TCP/QUIC, including Brutal, retains congestion control. |
| TCP Fast Open | Retain the existing lazy hello plus first-payload write. Kernel MPTCP's special treatment of SYN payload does not imply excluding overlay early data from logical sequence space. |

Linux's current default send path uses `sk_wmem_queued / avg_pacing_rate`, a
bounded send burst and shared-window eligibility. The queued memory includes
unacknowledged TCP data. Its reinjection path considers transport progress and
idle usable alternatives. Neither mechanism is equivalent to dividing a local
Go channel's length by `bandwidth_mbps`.

## Replacement boundary

Replace the aggregation data state, frame-indexed flow-control epochs, and
leg1-only replay map. Do not run old and new state machines concurrently.

Keep child dialing, UDP forwarding, public net.Conn behaviour, TFO, directional
activation policy, status publication and the shared memory-budget service.
Their adapters must report the new state rather than retain obsolete counters.
Remove `bandwidth_mbps` from runtime scheduling and status; it is neither a rate
cap nor an initial estimate in beta5. The parser accepts only this legacy field
for migration, ignores its value, and logs a warning per node initialization.
Other unknown fields are still rejected; old wire protocols remain unsupported.

## State and ownership

The connection owns `snd_una`, `snd_nxt`, `peer_window_end`, `rcv_nxt`,
`read_next`, local/remote DATA_FIN, and the send/receive stores. Each path owns its
incarnation, send/receive byte frontier, delivery measurements and transport
lifecycle. No network write, application write or memory wait holds the
connection state lock.

The following are distinct events:

1. Application Write is accepted into the bounded logical send store.
2. A child Write accepts a mapping. This does not acknowledge delivery.
3. The peer reports a path receipt. This updates path progress, not `snd_una`.
4. The peer's Data ACK advances `snd_una`. The send store releases the covered
   bytes when outstanding writer references have also been released.
5. The receiving application reads bytes. Only now is their receive storage
   reusable and the receive window allowed to slide.

Late duplicate data cannot change already accepted bytes. Reconnect events and
feedback from an old incarnation cannot acknowledge or fail a new path. Window
updates, duplicate feedback and reordered controls cannot move an acknowledgement
or right edge backwards. Future acknowledgements are rejected.

The runtime publishes DATA prefixes as the reliable child delivers them, rather
than waiting for an entire maximum-sized mapping. Sequence overflow is rejected
before publication. Failure halfway through a secondary mapping leaves its accepted
prefix valid; reinjection can repeat that prefix without duplicate application data.

## Scheduling and recovery

Scheduling uses peer-observed path progress, not child buffer acceptance. The
initial estimate is internal and is replaced by measurements; no manual bandwidth
ratio survives. Startup sampling must be work-conserving and must not strand
leg0 while warming leg1. A path that has stopped making progress stops receiving
new assignments; the logical stream and the other path remain usable.

Reinjection always refers to existing connection data and runs independently of
allocating new application data. It must remain possible with a full receive
window and at the memory high watermark. It does not create new logical data or
withdraw data already queued in TCP/QUIC. Reinjected duplicates count as transport
traffic but not new application traffic.

An in-band control frame cannot overtake bytes already queued in a reliable
child stream. Writer priority can only order data not yet submitted. This is a
fundamental difference from TCP-option ACKs and must be reflected in timeout
estimation and full-duplex tests, not hidden by a claim that control is out of
band. Likewise, this overlay cannot directly observe the underlying link's
packet-loss percentage.

## Required verification

Pure state tests must cover arbitrary byte fragmentation, overlap, duplicate
reinjection, reordered feedback, partial ACKs, FIN before missing data, half
close, stale-incarnation feedback, full-window recovery and memory-pressure
progress. Model tests compare the received bytes and EOF with an ordered-stream
oracle, including simultaneous stalls of either path and application backpressure.

Integration tests retain all direct/proxy and parent/child TFO combinations,
short requests, server-first protocols, cancellation and deadline behaviour.
Failure of leg1 must not synthesize EOF or reset on a live primary connection.

Performance acceptance uses isolated real TCP/HY2 transports on nec, including
60/120 ms RTT asymmetry, independently applied 20% UDP loss, large QUIC windows,
single and concurrent downloads, uploads, and a blocked receiving application.
Report startup and sustained application goodput against the same-run standalone
leg0, standalone HY2 and beta4 baselines. Passing synthetic queue tests alone is
not evidence of line-rate performance. No protocol offers an unconditional
instant-by-instant guarantee of outperforming the fastest standalone path.

## Sources

- https://www.rfc-editor.org/rfc/rfc8684.html
- https://www.rfc-editor.org/errata/eid6609
- https://www.rfc-editor.org/rfc/rfc6182.html
- https://www.rfc-editor.org/rfc/rfc6356.html
- https://www.rfc-editor.org/rfc/rfc6897.html
- https://github.com/torvalds/linux/tree/587858367581b9c55c3690f4e63382ad622719d4/net/mptcp
- https://docs.kernel.org/networking/mptcp.html
