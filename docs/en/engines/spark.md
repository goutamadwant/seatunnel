# SeaTunnel With Spark

Apache Spark is the right choice when your team already runs Spark and wants SeaTunnel jobs to fit into that batch or mixed workload environment. If you are evaluating SeaTunnel from scratch and do not need Spark specifically, start with [SeaTunnel Engine](./zeta/about.md) first.

## Start Here

Use this path if you want to run SeaTunnel on Spark:

- [Engine Overview](./overview.md)
- [Quick Start With Spark](../getting-started/locally/quick-start-spark.md)
- [Job Configuration Guide](../getting-started/job-configuration-guide.md)

## When To Choose Spark

Spark is usually the right engine when:

- your organization already runs Spark clusters in production
- the surrounding workloads are mainly batch-oriented
- you want SeaTunnel to align with an existing Spark ecosystem and deployment model

## Spark-Specific Configuration

Spark-specific job options live in the `env` block and use the `spark.` prefix.

Example:

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

## Command Line Example

Spark on YARN cluster mode:

```shell
./bin/start-seatunnel-spark-3-connector-v2.sh --master yarn --deploy-mode cluster --config config/example.conf
```

Spark on YARN client mode:

```shell
./bin/start-seatunnel-spark-3-connector-v2.sh --master yarn --deploy-mode client --config config/example.conf
```

## Minimal Example Job

The example below runs on Spark and prints generated records to the console.

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

If you need more transform options, see [Transforms Catalog](../transforms) and [Transform Common Options](../transforms/common-options/common-options.md).

## Running From A Source Checkout

If you are running examples from the repository source tree, the example module is:

- `seatunnel-examples/seatunnel-spark-connector-v2-example`

The example entry point is:

- `org.apache.seatunnel.example.spark.v2.SeaTunnelApiExample`

## Next Steps

- [Quick Start With Spark](../getting-started/locally/quick-start-spark.md)
- [Spark Translation Layer](../architecture/api-design/spark-translation-layer.md)
- [Transforms Catalog](../transforms)
- [SeaTunnel Engine](./zeta/about.md) if you want to compare against the default engine


## Checkpoint recovery

Set a dedicated checkpoint directory to run a STREAMING job with Spark Structured Streaming:

```hocon
env {
  job.mode = "STREAMING"
  checkpoint.interval = 5000
  spark.checkpoint.location = "hdfs:///seatunnel/jobs/my-job"
}
```

Restart the unchanged job with the same directory. The directory must be durable and accessible to the driver and every executor. The recovery mode accepts one sink query; route multiple tables through its multi-table sink. Its checkpoint is stored in `sink-0`. Only one active driver/query may own a checkpoint directory. Spark Hadoop configuration, including configured HA nameservices, is carried to journal readers and checkpoint cleanup. Keep source ordering, sink routing and parallelism unchanged when restoring. Recovered source table schemas come from completed checkpoint metadata. Without this option, the existing Spark batch execution remains in use.

Source intervals persist their rows and end state together before downstream consumption. A retry replays the completed interval. Source checkpoint completion is notified from Spark’s commit callback after sink commit. This callback does not establish that asynchronous connector offset commits have completed externally. Older intervals are removed after Spark confirms commit. A restarted driver/query reclaims unfinished staging directories from earlier driver generations while preserving current-generation task attempts and completed intervals. This mode provides source replay with the sink's at-least-once write behavior. It does not establish exactly-once delivery across a driver crash. Sinks that require writer-state recovery, including JDBC XA sinks, are rejected before their save-mode handling or query startup until a durable Spark sink recovery protocol is available.

A running Spark query has a fixed analyzed schema. Schema-changing CDC sources must implement `SupportSchemaChangeWatermarks`: the source freezes old-schema work before the DDL block and publishes the recovered catalog and source state at the after-change watermark. The driver waits for Spark's completed epoch, stops the old query, reconciles the receiving schema and rebuilds the source, transforms and sink as typed datasets. It reads completed commit metadata rather than pending offsets, including after a driver restart. Collector-local schema restoration only refreshes conversion state; it does not emit downstream DDL.

Schema reconciliation requires a sink implementing `SupportSchemaEvolutionRecovery`. JDBC checks the physical receiver schema against each DDL prefix before applying the remaining statements; the console sink has no physical schema to change. A retry after already-applied DDL must not repeat it. A divergent receiver or an ambiguous destructive DDL cycle (for example, dropping and recreating an identical column) fails before additional DDL. Automatic recovery cannot infer whether data-resetting DDL completed from matching column declarations alone. Destructive save modes such as schema recreation and dropping sink data are rejected in recovery mode.

The checkpoint reader accepts legacy interval journals without table-level schema metadata using the configured fixed schema. Changing routing, an incompatible analyzed schema or parallelism fails restore before emitting records. Checkpoints made by the older incomplete micro-batch implementation do not contain the durable interval journal and cannot be restored with this format; use a new directory for a new job.

When schema propagation is disabled, a completed DATA checkpoint restores source offsets while query analysis retains live-discovered catalogs. An already published pending interval remains immutable: if it contains typed rows whose encoded schema differs from live discovery, recovery rejects it rather than decoding rows with the wrong layout. An exact empty DATA interval can replay its unchanged offset and state. Pending schema boundaries also reject disabling propagation.
