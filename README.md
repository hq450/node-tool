# node-tool

`node-tool` 是一个使用 Zig 编写的轻量级本地节点库工具。

规划文档：

- [fancyss 未来待办：node-tool 规划](../../doc/todo/future_todo_node_tool.md)

它的职责边界是：

- 面向 `schema2` 节点库做结构化读写
- 为 shell 提供稳定的数据面接口
- 处理本地节点导出、导入、批量变更、排序、统计、预热计划

它不负责：

- 订阅下载
- 订阅格式识别与 URI 解析
- Xray API 控制
- `iptables/ipset/dnsmasq` 等运行时 orchestration

也就是说：

- 订阅输入仍然归 `sub-tool`
- 运行时热更新仍然归 `xapi-tool`
- geodata 资产仍然归 `geotool`
- `node-tool` 只处理“本地节点库”

## 当前版本

`0.1.0`

当前代码按 Zig `0.15.2` 编写并验证。

## 当前命令骨架

```bash
node-tool list
node-tool stat
node-tool find
node-tool node2json
node-tool json2node
node-tool add-node
node-tool delete-node
node-tool delete-nodes
node-tool warm-cache
node-tool reorder
node-tool plan
node-tool version
```

当前阶段：

- `version` 和帮助输出可用
- 其它命令先提供稳定 CLI 占位
- 真正的数据读写逻辑后续分阶段实现

## 第一阶段目标

优先实现以下命令：

- `node2json`
- `json2node`
- `add-node`
- `delete-node`
- `delete-nodes`
- `warm-cache`

强烈建议一起做：

- `list`
- `stat`
- `find`
- `reorder`
- `plan`

## 构建

直接构建：

```bash
zig build
```

构建后可执行文件位于：

```bash
./zig-out/bin/node-tool
```

生成多平台发布产物：

```bash
bash ./scripts/build-release.sh
```

默认目标：

- `x86_64`
- `armv5te`
- `armv7a`
- `armv7hf`
- `aarch64`

默认启用 `UPX` 压缩：

- `armv5te` 使用 `UPX 4.2.4`
- 其它目标使用 `UPX 5.0.2`

