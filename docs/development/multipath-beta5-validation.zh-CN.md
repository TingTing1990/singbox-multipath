# Multipath beta5 开发验证记录

历史开发检查点：2026-09-16，位于提交 `cd94d40a6` 之前。
下文源码指纹、开发产物和性能结果描述的是当时的状态；当时完整性能测试有两项失败。

`v1.14.0-multipath-beta5` 在 `cd94d40a6` 基础上仅修正统计：路径挂接次数不再依赖本侧发送聚合激活，已报告的远端失败次数纳入关闭连接的累计值。调度、路径估计、v8 线协议和数据传输逻辑均不变。新增计数测试的五轮 race、正确性 race 回归（排除 `TestPerformanceHealthyLinks`）、vet 和 direct／代理子路径 TFO 集成测试再次通过。本次仅修正统计，未修改或重跑下文记录的历史性能失败项。

## 可复现的版本状态

- 主机：`nec`。
- sing-box 工作区：`/home/wusiyu/work/sing-box-multipath-beta5`，分支 `multipath-beta5`，基于提交 `ad10690649980a512f384db5f3278d541028f822`。
- HomeProxy 工作区：`/home/wusiyu/work/luci-app-homeproxy-multipath-beta5`，分支 `multipath-beta5`，基于提交 `2435b0f`。
- 在该历史检查点，改动尚未提交，也未创建 beta5 tag、推送或部署到生产环境。原工作区及其中原有的未跟踪产物均保留。
- 线协议为 v8，不兼容旧版本；状态 JSON 为 schema 3。`bandwidth_mbps` 不再影响运行时行为；为便于迁移，允许该字段存在，但忽略其值，并在每个节点初始化时打印一次警告。其他未知字段仍会被拒绝。
- Go 版本：现有 toolbox 环境中的 1.25.5。
- 源码指纹：对 `protocol/multipath` 下所有 Go 文件的 `sha256sum` 结果按文件路径排序，再对汇总结果计算 SHA-256：
  `73d738a75a2f1d731ead167a85aadcf4a4ebd7c7d773836dc0a2f969a781d4cf`。
- `option/multipath.go` 的 SHA-256：
  `b345ae3db8f9f3d9431ece1fb6e9c22a34c1ef8463f707fb9101266ebfd6a407`。

## 正确性与构建检查

最终保留的实现已通过：

```sh
go test -race -skip TestPerformanceHealthyLinks -count=1 -timeout 180s \
  ./protocol/multipath ./protocol/multipath/stream
go vet ./protocol/multipath ./protocol/multipath/stream ./option
# 在独立的 test 模块中执行：
go test -run '^TestMultipath' -count=1 -timeout 180s ./...
```

覆盖范围包括字节分片与重叠、部分 ACK 下的数据所有权、writer 对不可变数据的引用、任意到达顺序的重复重注入、FIN／半关闭、应用 deadline、接收内存压力与头部恢复、旧路径实例、辅助路径失败，以及对任一或两条路径的帧头、部分载荷和反馈施加延迟。集成模块使用 direct 和 Shadowsocks child 路径，覆盖父级 multipath 与 child 的 TFO 开关组合。这些测试覆盖有限场景，不等于证明所有网络事件时序都正确。

增量 DATA 测试还验证了：一个 64 KiB 映射先到达 777 字节后，在剩余部分尚未发送时，这 777 字节已经可被读取和确认。此时关闭辅助路径，再通过 leg0 重注入整个映射，应用接收到的字节和 EOF 仍完全正确。序号溢出的帧头会在交付载荷前被拒绝。这组专项测试重复执行五轮 race 检查并通过，随后又通过了完整正确性测试集。

Linux／amd64 静态开发构建使用 `release/DEFAULT_BUILD_TAGS_OTHERS` 中的全部构建标签、`release/LDFLAGS` 和 `CGO_ENABLED=0`，报告版本为 `1.14.0-multipath-beta5-dev`。`file` 已确认其为静态链接。

产物路径：

```text
/home/wusiyu/work/sing-box-beta5-compat-build.V7AUiC/sing-box-1.14.0-multipath-beta5-dev-linux-amd64
```

SHA-256：

```text
b59a8aefe032a81f02dbc39ab5a54dba32730ba128f85e1c8d7fb80861fd1b1d
```

最新重编译仅增加旧字段的迁移处理。专项测试重复执行三轮 race 检查，覆盖双端字段省略、数组、零、空数组、null 及其他合法 JSON 值：字段存在时忽略并警告，字段省略时不警告，运行时配置与不含该字段的基线一致。无关的未知字段仍然报错。编译后的二进制还通过了 `check`，并使用双端均含旧字段、仅监听回环地址的配置验证实际启动，每个节点恰好输出一条警告。下文性能数据来自本次解析兼容改动之前，本次未重新运行性能测试。

HomeProxy JavaScript 语法检查已通过；使用真实仿真产生的 schema 3 样本、模拟 LuCI 接口的渲染测试也已通过。该测试覆盖新计数器及提示、每秒轮询和拒绝旧 schema 文档。它**不是**实际 OpenWrt／rpcd 或浏览器集成测试。本次未构建 beta5 对应的 HomeProxy APK／IPK。

## 隔离环境中的真实 TCP／HY2 测试

测试工具目录：`/home/wusiyu/work/mp-beta5-sim.inRL1E`，主要文件为 `main.go`、`run-realnet.sh`。

每轮测试使用独立的用户及网络命名空间，不使用生产路由或服务。数据源和接收端运行在被测服务器进程之外，源站链路为 1 Gbps。服务端和客户端各分配两个 CPU 核心。

- leg0：下行 160 Mbps、上行 50 Mbps，RTT 为 60 ms。
- HY2：基础 RTT 为 120 ms，下行独立施加 20% UDP 丢包，底层链路为 1 Gbps。
- HY2 配置：上行 40 Mbps，下行 750 或 600 Mbps；stream 接收窗口 256 MB，connection 接收窗口 640 MB。ACK 上行限速 40 Mbps，不额外施加上行丢包。
- 已检查 GSO 分段：丢包作用于每个 UDP 数据报，而不是整批数据。
- 最新长时间测试：先预热总计 1 GiB，再测量总计 2 GiB 的传输。并发测试把总量分配到各流，所有流完成预热后才开始测量。
- 使用 CRC32C 校验接收载荷；下表速率为应用有效吞吐，不是包含重传等开销的链路字节速率。
- multipath 使用自动限额：全局预算 512 MiB，接收窗口／发送历史上限各为 224 MiB，不额外设置 chunk 数上限。这些是按需分配存储的上限，不是预留内存。

| `results/` 下的测试目录 | 模式 | Mbps | CRC |
| --- | --- | ---: | --- |
| `long750-hy2` | 单 HY2，下行配置 750 | 719.848 | 通过 |
| `automatic750-mp` | 聚合，整段映射接收版本 | 858.935 | 通过 |
| `automatic750-eight-mp` | 聚合，8 流，整段映射接收版本 | 837.594 | 8 流全部通过 |
| `baseline600-hy2` | 单 HY2，下行配置 600 | 590.320 | 通过 |
| `automatic600-mp` | 聚合，下行配置 600，整段映射接收版本 | 737.786 | 通过 |
| `incremental750-mp` | 最终增量接收版本，单流 | 734.298 | 通过 |
| `incremental750-repeat-mp` | 同一增量接收版本，重复测试 | 845.060 | 通过 |
| `incremental750-eight-mp` | 最终增量接收版本，8 流 | 760.083 | 8 流全部通过 |

不同测试轮次之间存在明显波动。不能把最高结果当作保证速率，也不能把早期整段映射接收版本的结果，当作最终增量接收版本的实测结果。

已检查的 `incremental750-mp` 状态样本中，没有重注入、路径停滞超时或内存压力事件。下行配置 600 的场景尚未在加入增量接收之后复测。这些结果来自隔离仿真，不代表用户路由器能够获得相同有效吞吐。

复现命令示例，脚本会创建隔离网络环境：

```sh
env EXTERNAL_ORIGIN=1 SERVER_CPU=2,3 CLIENT_CPU=4,5 \
  SIZE_MIB=2048 WARM_MIB=1024 DOWN_MBPS=750 LOSS_SEED=44 \
  unshare --user --map-root-user --net bash \
  /home/wusiyu/work/mp-beta5-sim.inRL1E/run-realnet.sh mp NEW_LABEL
```

使用新的测试标签，以保留已有结果。设置 `STREAMS=8` 可选择并发测试；将 `mp` 替换为 `hy2` 可运行单 HY2 对照。每个结果目录均保留日志、参数、性能 profile、流量控制计数器和状态样本。

## 仍未通过的性能测试

`TestPerformanceHealthyLinks` 保留原有 beta3 验收门槛，并显式配置 64 MiB 接收窗口／发送历史。它是确定性的可靠 FIFO 链路测试模型，不是 QUIC。最终保留的实现有以下两项未通过：

| 场景 | beta5 Mbps | 已记录的 beta3 Mbps | 验收下限 |
| --- | ---: | ---: | ---: |
| 8 流，160+600 Mbps，RTT 65／400 ms | 419.4 | 447.0 | 438.06 |
| 单流，1000+1000 Mbps，RTT 65／110 ms | 1793.8 | 1920.8 | 1882.384 |

该测试模型中的其他场景通过，包括 160+600 Mbps、RTT 65／110 ms 下的 1／8／32 流。健康链路场景没有记录到重注入或内存压力事件。

仅走主路径的冷响应测试，从 1 字节到 4 MiB 均通过，没有增加额外的首字节 RTT。上述两个聚合场景不能被描述为通过，不能删除，也不能通过降低门槛来掩盖；它们仍属于发布前必须处理的验收项。

## 已撤回的试验与负面结果

- 将 `queue_frames` 当作整条路径的在途映射数量上限，会限制大带宽时延积的 HY2 路径。现在它只限制尚未发送的应用数据。
- 对每次接收回执赋予相同权重，会让交付速率估计偏向丢包恢复后的突发释放。最终保留的估计器按经过的时间加权。
- 额外增加基于最小 RTT 的路径带宽时延积上限，使真实聚合吞吐降至约 155 Mbps，因此已撤回。可靠 child 本身已经控制底层报文的发送管线。
- 先选择预计排空时间最短的路径，再检查 Go writer 是否可用，使聚合吞吐降至约 152 Mbps。Go `Write` 阻塞不等同于 Linux 的 `sk_stream_memory_free` 条件，因此已撤回这种直接移植。
- 名为 `shared-window256-mp` 的试验因环境变量名称错误，实际仍使用旧的 64 MiB 配置。其 617.460 Mbps 结果**不能**作为 256 MiB 窗口的证据。修正后显式使用 256 MiB 的测试达到 843.533 Mbps；最终默认值改为从全局预算推导上限，而不是简单用一个更大的固定常数替换旧常数。
- 仅取消 leg0 的启动探测限制，改善了一项短时测试，却使 32 流模型退化到 164.7 Mbps：探测完成前，几乎所有数据就已被预先分配给 leg0。该试验已撤回，不包含在开发产物中。

尚未建立相对于单独 leg0／HY2 性能的无条件下界保证。
