# Multipath beta5 development validation

## Updated beta5: endpoint error provenance

The updated release uses protocol v9: session-close frames include a bounded
close-reason field, and status schema 3 adds `last_error_source`. Both endpoints
must be updated. Application closure is distinguished from prior MP failure;
only closure-related I/O events inherit endpoint attribution. Missing provenance,
timeouts and malformed frames are not filtered. Scheduling, flow control,
reinjection, FIN/drain handling and close timing are unchanged.

Close-reason codecs, cross-leg provenance, primary-failure attribution, QUIC
code-zero cancellation classification and LuCI source-based filtering have
regression coverage. The correctness race suite (excluding the historical
`TestPerformanceHealthyLinks`) and direct/proxy TFO integration matrix passed.
The performance results below are historical, not new measurements.

## Earlier checkpoints

Historical development checkpoint: 2026-09-16, before commit `cd94d40a6`.
The source fingerprints, development artifacts and performance results below
describe that checkpoint, at which the full performance suite had two failures.

The original beta5 release at `a6e18e4d8` adds statistics-only corrections on top of
`cd94d40a6`: attachment counts independent of local TX activation and cumulative
reported remote failures. Scheduling, path estimates, wire v8 and the data path
are unchanged. Five race repetitions of the new counter tests, the correctness
race suite (excluding `TestPerformanceHealthyLinks`), vet and the direct/proxy
TFO integration suite passed again. The historical performance failures below
were neither changed nor rerun by this statistics-only release.

## Reproducible state

- Host: `nec`.
- sing-box worktree: `/home/wusiyu/work/sing-box-multipath-beta5`, branch
  `multipath-beta5`, based on `ad10690649980a512f384db5f3278d541028f822`.
- HomeProxy worktree: `/home/wusiyu/work/luci-app-homeproxy-multipath-beta5`, branch
  `multipath-beta5`, based on `2435b0f`.
- At this historical checkpoint, changes were uncommitted and no beta5 tag,
  push, or production deployment had occurred.
  Original worktrees and their pre-existing untracked artifacts were preserved.
- Wire protocol: v8, with no old-version compatibility. Status JSON: schema 3.
  `bandwidth_mbps` has no runtime effect. It is accepted for migration, ignored,
  and warned about once per node initialization; other unknown fields are rejected.
- Go: 1.25.5 in the existing toolbox environment.
- Source fingerprint: sorted `sha256sum` lines for all Go files under
  `protocol/multipath`, hashed again:
  `73d738a75a2f1d731ead167a85aadcf4a4ebd7c7d773836dc0a2f969a781d4cf`.
- SHA-256 of `option/multipath.go`:
  `b345ae3db8f9f3d9431ece1fb6e9c22a34c1ef8463f707fb9101266ebfd6a407`.

## Correctness and build checks

Passed on the retained implementation:

```sh
go test -race -skip TestPerformanceHealthyLinks -count=1 -timeout 180s \
  ./protocol/multipath ./protocol/multipath/stream
go vet ./protocol/multipath ./protocol/multipath/stream ./option
# From the separate test module:
go test -run '^TestMultipath' -count=1 -timeout 180s ./...
```

Coverage includes byte fragmentation/overlap, partial ACK ownership, immutable
writer references, arbitrary-order duplicate reinjection, FIN/half-close, application
deadlines, receive pressure/head recovery, stale path generations, secondary failure,
and delayed headers, partial payloads and feedback on either/both legs. Direct and
Shadowsocks child paths cover the parent/child TFO combinations in the integration
module. These are bounded tests, not proof for every possible network schedule.

Incremental DATA tests also verify that 777 bytes of a 64 KiB mapping become readable
and acknowledged before its suffix is sent. Closing the secondary at that point and
reinjecting the complete mapping through leg0 preserves exact application bytes and
EOF. Sequence-overflow headers are rejected before publishing payload. Five race
repetitions of these dedicated tests passed, followed by the full correctness suite.

The Linux/amd64 static development build uses all tags in
`release/DEFAULT_BUILD_TAGS_OTHERS`, `release/LDFLAGS`, `CGO_ENABLED=0`, and reports
`1.14.0-multipath-beta5-dev`. `file` confirms static linking.

Artifact:
`/home/wusiyu/work/sing-box-beta5-compat-build.V7AUiC/sing-box-1.14.0-multipath-beta5-dev-linux-amd64`

SHA-256:
`b59a8aefe032a81f02dbc39ab5a54dba32730ba128f85e1c8d7fb80861fd1b1d`.

The latest rebuild only adds legacy-field migration handling. Three race repetitions
verified both inbound and outbound with omitted, array, zero, empty, null, and other
valid JSON values: present values are ignored with a warning, absent fields do not
warn, and runtime configuration matches the field-free baseline. Unknown unrelated
fields remain errors. The compiled binary also passed `check` and live loopback-only
startup with the legacy field on both nodes, producing exactly one warning each.
The performance measurements below predate this parser-only change and were not rerun.

HomeProxy JavaScript syntax checks and a mocked LuCI renderer test passed using an
actual schema-3 simulator sample. The test covers new counters/tooltips, one-second
polling and rejection of old schema documents. It is **not** an actual OpenWrt/rpcd
or browser integration test. No HomeProxy APK/IPK was built for beta5.

## Isolated real TCP/HY2 tests

Harness: `/home/wusiyu/work/mp-beta5-sim.inRL1E` (`main.go`, `run-realnet.sh`).
Each run uses separate user/network namespaces, not production routes or services.
The source and receiver run outside the measured server's process, with a 1 Gbps
origin path. Server and client each have two assigned CPU cores.

- leg0: 160 Mbps down / 50 Mbps up, 60 ms RTT.
- HY2: 120 ms base RTT, independently applied 20% downlink UDP loss, 1 Gbps wire.
- HY2 settings: up 40, down 750 or 600 Mbps; 256 MB stream / 640 MB connection
  receive window. ACK uplink limited to 40 Mbps, without added uplink loss.
- GSO segmentation was checked: loss was per UDP datagram, not per batch.
- Latest long tests: 1 GiB aggregate warm-up followed by 2 GiB measured transfer.
  Concurrent runs split that total among flows and start measurement after all warm.
- CRC32C verified received payload; rates below are application goodput, not wire bytes.
- Automatic multipath limits: 512 MiB global budget; 224 MiB RX/history ceilings,
  no extra chunk-count cap. These are sparse ceilings, not reserved allocations.

| Run directory under `results/` | Mode | Mbps | CRC |
| --- | --- | ---: | --- |
| `long750-hy2` | Single HY2, 750 setting | 719.848 | OK |
| `automatic750-mp` | Aggregated, whole-mapping receive | 858.935 | OK |
| `automatic750-eight-mp` | Aggregated, 8 flows, whole-mapping receive | 837.594 | All 8 OK |
| `baseline600-hy2` | Single HY2, 600 setting | 590.320 | OK |
| `automatic600-mp` | Aggregated, 600 setting, whole-mapping receive | 737.786 | OK |
| `incremental750-mp` | Final incremental receive, single flow | 734.298 | OK |
| `incremental750-repeat-mp` | Same incremental version, repeat | 845.060 | OK |
| `incremental750-eight-mp` | Final incremental receive, 8 flows | 760.083 | All 8 OK |

There is substantial run-to-run variation. Do not treat the largest result as a
guaranteed rate, or the earlier whole-mapping results as measurements of the final
incremental version. The inspected `incremental750-mp` status sample showed no
reinjection, path stall timeout, or memory-pressure event. The 600-setting result has not yet been
repeated after incremental receive. These are isolated simulations, not a claim
that the user's router will deliver the same goodput.

An example reproduction command (the script builds isolated network state):

```sh
env EXTERNAL_ORIGIN=1 SERVER_CPU=2,3 CLIENT_CPU=4,5 \
  SIZE_MIB=2048 WARM_MIB=1024 DOWN_MBPS=750 LOSS_SEED=44 \
  unshare --user --map-root-user --net bash \
  /home/wusiyu/work/mp-beta5-sim.inRL1E/run-realnet.sh mp NEW_LABEL
```

Use a new label to preserve prior results. `STREAMS=8` selects concurrent flows;
replace `mp` with `hy2` for the standalone control. Logs, parameters, profiles,
traffic-control counters and status samples are retained in each result directory.

## Remaining performance failures

`TestPerformanceHealthyLinks` retains its original beta3 thresholds and explicit
64 MiB RX/history configuration. It is a deterministic reliable FIFO fixture, not
QUIC. The retained implementation fails these two cases:

| Case | beta5 Mbps | Recorded beta3 Mbps | Allowed floor |
| --- | ---: | ---: | ---: |
| 8 flows, 160+600 Mbps, RTT 65/400 ms | 419.4 | 447.0 | 438.06 |
| 1 flow, 1000+1000 Mbps, RTT 65/110 ms | 1793.8 | 1920.8 | 1882.384 |

Other cases in that fixture pass, including 1/8/32 flows at 160+600 and 65/110 ms.
No healthy-link reinjection or memory-pressure events were recorded. Preferred-only
cold response tests, from one byte through 4 MiB, pass without an additional first-byte
RTT. The two failing aggregation cases must not be described as passing, removed,
or hidden by lowering the thresholds. They remain release acceptance work.

## Rejected experiments and useful negative evidence

- Treating `queue_frames` as a whole-path flight count capped the large-BDP HY2 path.
  It now bounds only unsent application data.
- Equal weight per receipt biased the delivery estimate toward loss-recovery bursts.
  The retained estimator uses elapsed-time weighting.
- Adding another minimum-RTT-based path BDP cap reduced real aggregation to about
  155 Mbps; it was reverted. The reliable child already controls its packet pipeline.
- Selecting the lowest-drain path before checking Go writer availability reduced
  aggregation to about 152 Mbps. Go `Write` blockage is not Linux
  `sk_stream_memory_free`; that literal transplant was reverted.
- An experiment labelled `shared-window256-mp` accidentally used the old 64 MiB
  settings because its environment-variable names were wrong. Its 617.460 Mbps
  result is **not** evidence for a 256 MiB window. The corrected explicit 256 MiB
  run reached 843.533 Mbps; the retained default derives ceilings from the global
  budget instead of replacing one fixed constant with another.
- Removing discovery bounds only from leg0 improved one short test but regressed
  the 32-flow fixture to 164.7 Mbps, with almost all data preassigned to leg0 before
  discovery completed. This experiment was reverted; it is not in the artifact.

No unconditional lower bound relative to standalone leg0/HY2 has been established.
