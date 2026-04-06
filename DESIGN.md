# node-tool design

## 定位

`node-tool` 是一个面向 `fancyss schema2` 本地节点库的结构化 CLI。

它的核心目标不是替代整个 shell 层，而是把当前 shell 里最重、最脆弱、最依赖
`jq/awk/sed/base64/dbus list` 链式处理的“节点库数据面”抽离出来。

## 边界

### 负责

- 节点导出
- 节点导入
- 节点增删改查
- 来源/分组/身份视图
- 变更计划生成
- 节点缓存预热计划

### 不负责

- 订阅下载
- URI 订阅解析
- Xray API / 运行时热更新
- 防火墙 / DNS / 代理进程 orchestration

## 第一阶段命令

- `list`
- `stat`
- `find`
- `node2json`
- `json2node`
- `add-node`
- `delete-node`
- `delete-nodes`
- `warm-cache`
- `reorder`
- `plan`

## 数据视图

后续建议至少提供三类稳定视图：

- `schema2 raw`
- `canonical compare view`
- `display view`

## 当前实现阶段

当前仓库只建立项目骨架和命令框架。

优先顺序：

1. 先把 CLI 命令和帮助输出固定
2. 再实现只读命令：`list/stat/find/node2json`
3. 再实现写命令：`add/delete/json2node/reorder`
4. 最后实现 `plan/warm-cache`

