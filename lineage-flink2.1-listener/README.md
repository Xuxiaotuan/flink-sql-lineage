# Flink 2.1 Listener 血缘采集模块

这个模块用于本地和生产前验证 Flink 2.1 原生 lineage listener 链路：

```text
Flink 2.1 SQL Job
  -> JobStatusChangedListener
  -> JobCreatedEvent#lineageGraph()
  -> 提取 input/output 表、schema、connector options
  -> 回放到 flink-sql-lineage
  -> flink-sql-lineage 重新跑 Flink Planner
  -> 生成字段级血缘
```

它不是替代 `lineage-flink2.1.x` 的字段级解析插件。这个模块只负责从真实 Flink 任务运行时采集上下文；字段级血缘仍由主服务拿到 SQL 和 schema 后重新跑 planner 得到。

## 为什么不提交 Flink 整包

`pom.xml` 里的 Flink 依赖都是 `provided`。提交到 Git 的只有本模块源码、脚本和文档；本地 Flink 发行包仍然放在：

```text
~/software/flink/flink-2.1.0
```

打出来的 listener jar 只包含本模块 class 和 `META-INF/services`，不包含 Flink lib。

## OpenLineage 对齐点

本模块参考 OpenLineage Flink 2.x 集成的核心路径：

- 通过 `execution.job-status-changed-listeners` 注册 `JobStatusChangedListenerFactory`。
- Listener 处理 `JobCreatedEvent`。
- 从 `JobCreatedEvent#lineageGraph()` 读取 sources、sinks、relations。
- 针对 Flink SQL 的 `TableLineageDataset`，通过反射调用 `table()` 取 `CatalogBaseTable`，避免直接依赖 planner 内部类造成 classloader 问题。

差异是：OpenLineage 会把数据转换成 OpenLineage RunEvent；本模块写 JSONL 文件，方便回放到 `flink-sql-lineage`。为了重新跑 planner，本模块额外保留了表 options、字段 kind、summary 和 relations。

## 快速复现

先确保 `flink-sql-lineage` 后端和前端已启动：

```bash
scripts/dev/start-backend.sh
scripts/dev/start-web.sh
```

然后执行：

```bash
cd /Users/xujiawei/magic/workbench/flink-sql-lineage
lineage-flink2.1-listener/scripts/07_一键重现实验.sh
```

成功后打开：

```text
http://127.0.0.1:3001/#/job/list
```

查看最新 `监听器回放-*` 任务。
