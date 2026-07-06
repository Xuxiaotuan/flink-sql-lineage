# OpenLineage 参考说明

## 1. 为什么参考 OpenLineage

OpenLineage 当前 Flink 2.x 文档和主干代码已经支持 Flink 2.x 的 native lineage 机制。它的 Flink 2.x 集成方式和这个模块的方向一致：

```text
Flink JobStatusChangedListener
  -> JobCreatedEvent
  -> LineageGraph
  -> source/sink datasets
  -> 转成 OpenLineage RunEvent
```

官方文档里使用的配置：

```yaml
execution.job-status-changed-listeners: io.openlineage.flink.listener.OpenLineageJobStatusChangedListenerFactory
```

## 2. OpenLineage 关键实现思路

OpenLineage 的 Flink 2.x listener 里有几个关键类：

```text
io.openlineage.flink.listener.OpenLineageJobStatusChangedListenerFactory
io.openlineage.flink.listener.OpenLineageJobStatusChangedListener
io.openlineage.flink.converter.LineageGraphConverter
io.openlineage.flink.converter.OpenLineageDatasetExtractor
io.openlineage.flink.wrapper.TableLineageDatasetWrapper
```

核心流程：

```text
onEvent(JobStatusChangedEvent)
  -> 判断是否 JobCreatedEvent
  -> event.lineageGraph()
  -> LineageGraphConverter.convert(...)
  -> 提取 inputs / outputs
  -> emit OpenLineage START event
```

## 3. 对本实验最有价值的一点

一开始直接读 Flink 的：

```java
LineageDataset.facets()
```

在 `datagen -> blackhole` 的 Table SQL 实验里，拿到的是空 schema。

OpenLineage 的思路更深一层：它识别 Flink planner 内部的：

```text
org.apache.flink.table.planner.lineage.TableLineageDataset
```

然后通过 wrapper / reflection 调用：

```java
dataset.table()
```

拿到：

```text
CatalogBaseTable
```

这个模块也用了这个思路：

```text
LineageDataset
  -> 如果是 TableLineageDataset
  -> 反射调用 table()
  -> CatalogBaseTable
  -> getUnresolvedSchema()
  -> columns
```

所以即使 `facets` 是空，也能拿到 Table SQL 的 source/sink schema。

## 4. 和 OpenLineage 的区别

OpenLineage 的目标是发标准 OpenLineage 事件：

```text
RunEvent
Job
Run
InputDataset
OutputDataset
DatasetFacets
```

本模块的目标不是替代 OpenLineage，而是服务于 `flink-sql-lineage`：

```text
Flink Listener schema
  -> CREATE TABLE DDL
  -> flink-sql-lineage 重新跑 planner
  -> 字段级血缘图
```

也就是说：

- OpenLineage 更像通用血缘事件标准。
- 当前模块更像 `flink-sql-lineage` 的 schema 采集和回放适配层。

## 5. 生产落地建议

后续可以有两个方向：

### 方向 A：直接用 OpenLineage

部署 OpenLineage Flink listener，把事件发到 Marquez/DataHub/自研 collector。

适合：

- 你要跨系统统一血缘。
- 已经有 OpenLineage/Marquez/DataHub 生态。
- 更关注表级、任务级、运行状态级血缘。

### 方向 B：参考 OpenLineage 写自研 Listener

保留这个实验方向，把 Listener 事件发给 `flink-sql-lineage` collector。

适合：

- 你已经确定要用 `flink-sql-lineage` 做字段级 SQL 血缘。
- 你需要根据 Listener schema 生成临时 DDL。
- 你希望重新跑 Flink Planner 来拿 transform 表达式。

推荐当前先走方向 B，因为已经验证闭环：

```text
Listener schema -> DDL -> flink-sql-lineage -> 字段级血缘 SUCCESS
```

## 6. 参考资料

- OpenLineage Flink 2.x 文档：https://openlineage.io/docs/integrations/flink/flink2/
- OpenLineage GitHub：https://github.com/OpenLineage/OpenLineage
- OpenLineage Flink 2.x 源码目录：`integration/flink/flink2`
- Flink Job Status Changed Listener：https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/advanced/job_status_listener/
- Flink Data Lineage：https://nightlies.apache.org/flink/flink-docs-stable/docs/internals/data_lineage/
