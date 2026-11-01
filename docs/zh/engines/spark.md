# SeaTunnel 运行在 Spark 上

当你的团队已经稳定运行 Spark，并希望 SeaTunnel 作业融入现有的批处理或混合负载环境时，Spark 会是合适的选择。如果你只是从零开始评估 SeaTunnel，且没有必须使用 Spark 的前提，建议先从 [SeaTunnel Engine](./zeta/about.md) 开始。

## 从这里开始

如果你的目标是让 SeaTunnel 跑在 Spark 上，建议按下面顺序阅读：

- [引擎概览](./overview.md)
- [Spark 引擎快速开始](../getting-started/locally/quick-start-spark.md)
- [作业配置指南](../getting-started/job-configuration-guide.md)

## 什么时候选择 Spark

以下场景通常更适合使用 Spark：

- 团队已经在生产中运行 Spark 集群
- 周边任务主要以批处理为主
- 希望 SeaTunnel 与既有 Spark 生态和部署方式保持一致

## Spark 专属配置如何写

Spark 专属作业参数写在 `env` 块中，并使用 `spark.` 前缀。

示例：

```hocon
env {
  spark.app.name = "example"
  spark.sql.catalogImplementation = "hive"
  spark.executor.memory = "2g"
  spark.executor.instances = "2"
  spark.yarn.priority = "100"
  spark.dynamicAllocation.enabled = "false"
}
```

## 命令行示例

Spark on YARN 集群模式：

```shell
./bin/start-seatunnel-spark-3-connector-v2.sh --master yarn --deploy-mode cluster --config config/example.conf
```

Spark on YARN 客户端模式：

```shell
./bin/start-seatunnel-spark-3-connector-v2.sh --master yarn --deploy-mode client --config config/example.conf
```

## 最小示例作业

下面这个例子会在 Spark 上运行，并把生成的数据打印到控制台。

```hocon
env {
  parallelism = 1

  spark.app.name = "example"
  spark.sql.catalogImplementation = "hive"
  spark.executor.memory = "2g"
  spark.executor.instances = "1"
  spark.yarn.priority = "100"
  spark.dynamicAllocation.enabled = "false"
}

source {
  FakeSource {
    plugin_output = "fake"
    row.num = 16
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

transform {
  FieldMapper {
    plugin_input = "fake"
    plugin_output = "fake1"
    field_mapper = {
      age = age
      name = new_name
    }
  }
}

sink {
  Console {
    plugin_input = "fake1"
  }
}
```

如果你需要更多 transform 能力，继续查看 [Transforms 目录](../transforms) 和 [Transform 通用参数](../transforms/common-options/common-options.md)。

## 从源码仓库运行示例

如果你是在源码仓库里运行示例，对应模块是：

- `seatunnel-examples/seatunnel-spark-connector-v2-example`

示例入口类是：

- `org.apache.seatunnel.example.spark.v2.SeaTunnelApiExample`

## 下一步

- [Spark 引擎快速开始](../getting-started/locally/quick-start-spark.md)
- [Spark 转换层](../architecture/api-design/spark-translation-layer.md)
- [Transforms 目录](../transforms)
- 如果你还想和默认引擎对比，可回看 [SeaTunnel Engine](./zeta/about.md)


## 检查点恢复

为 STREAMING 作业配置独立的检查点目录，可以使用 Spark Structured Streaming：

```hocon
env {
  job.mode = "STREAMING"
  checkpoint.interval = 5000
  spark.checkpoint.location = "hdfs:///seatunnel/jobs/my-job"
}
```

使用相同目录重启未修改的作业。该目录必须持久化，并且驱动和所有执行器均可访问。恢复模式只允许一个 Sink 查询，多张表应通过其多表 Sink 路由；检查点保存在 `sink-0` 子目录中。恢复时须保持 Source 顺序、Sink 路由及并行度不变；Source 表结构从已完成的检查点元数据恢复。未配置此选项时，保留现有 Spark 批处理执行方式。

Source 将一个区间的数据与结束状态一起持久化，完成后才允许下游读取。任务重试会重放已完成区间；Spark 在 Sink 提交后通过提交回调通知 Source 检查点完成。该回调返回不代表连接器的异步外部位点提交已经完成。Spark 确认提交后，旧区间会被清理。此机制保留 Sink 原有投递语义，不会让非事务 Sink 获得精确一次保证。

运行中的 Spark 查询具有固定的分析结构。支持结构变更的 CDC Source 必须实现 `SupportSchemaChangeWatermarks`，在 DDL 前冻结旧结构数据，并在变更后的水位标记中持久化恢复结构和 Source 状态。驱动等待 Spark 完成该批次，停止旧查询，协调接收表结构，然后重新构建具有明确类型的 Source、Transform 和 Sink。恢复读取已完成提交的元数据，不采用尚未提交的 offset。Collector 本地结构恢复只刷新转换状态，不向下游重放 DDL。

结构协调需要 Sink 实现 `SupportSchemaEvolutionRecovery`。JDBC 将物理表结构与每一步 DDL 的结构比较，只执行尚未完成的语句；Console 没有需要修改的物理结构。重试不会重复已执行的 DDL。如果接收表结构发生分歧，或者破坏性 DDL 循环使进度无法判断（例如删除后以相同定义重新创建列），则在执行额外 DDL 前失败。仅靠相同的列定义，无法判断会重置数据的 DDL 是否执行成功。恢复模式拒绝重建表、清空 Sink 数据等破坏性保存模式。

对于没有表级结构元数据的旧区间日志，使用配置中的固定结构恢复。路由、不兼容的分析结构或并行度不一致会在输出记录前失败。旧版不完整微批实现生成的检查点不包含持久化区间日志，无法按此格式恢复；新作业应使用新目录。

恢复目录同一时间只能由一个活动的 driver/query 使用。日志读取、检查点确认和清理会保留 Spark 的 Hadoop 配置，包括 HA nameservice 配置。driver/query 重启时会清理旧运行代次留下的未完成临时目录，同时保留当前代次的任务尝试和已完成的批次。

当前恢复模式不保证 driver 崩溃后的端到端 exactly-once。需要恢复 writer state 的 sink（包括 JDBC XA）会在保存模式处理和 query 启动前被拒绝；它们需要单独的持久化 sink 恢复协议。

关闭结构变更传播时，已完成的 DATA 检查点恢复 Source 位点，查询分析保留实时发现的表结构。已经发布但尚未提交的区间保持不变；如果区间中已编码记录的结构与当前发现的结构不一致，恢复将失败，避免按错误布局解码记录。严格空的 DATA 区间可以重放原有位点和状态。存在待处理结构边界时，也不允许关闭传播。
