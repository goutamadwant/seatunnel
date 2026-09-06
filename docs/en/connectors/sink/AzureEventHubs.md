import ChangeLog from '../changelog/connector-azure-event-hubs.md';

# AzureEventHubs

> Azure Event Hubs sink connector

## Description

Publishes SeaTunnel rows to one existing Azure Event Hub using the native Azure AMQP producer.
It reuses the Azure Event Hubs source connector's client dependency and connection-string configuration.
The Kafka connector remains an alternative for deployments using the Event Hubs Kafka-compatible endpoint.

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)

Only INSERT rows are accepted. UPDATE_BEFORE, UPDATE_AFTER and DELETE rows fail the task instead of silently losing their operation semantics.

## Options

| name | type | required | default value |
| --- | --- | --- | --- |
| connection_string | string | yes | - |
| event_hub_name | string | yes | - |
| format | enum | no | json |
| field_delimiter | string | no | , |
| batch_size | int | no | 100 |
| partition_id | string | no | - |
| partition_key | string | no | - |
| common-options | | no | - |

### connection_string [string]

Azure namespace connection string with permission to send to the selected Event Hub.
Configure `event_hub_name` separately; `EntityPath` in the connection string is rejected.
The option is masked by SeaTunnel configuration desensitization. Do not commit real credentials to job files.

V1 supports connection strings only, not Microsoft Entra ID or managed identity.
For the official emulator, use its namespace connection string with `UseDevelopmentEmulator=true`.
The emulator cannot validate live Azure authentication, authorization or service quotas.

### event_hub_name [string]

Existing Event Hub receiving events. The connector does not create namespaces, hubs or partitions.

### format [enum]

`json` serializes each row as a JSON object with schema field names.
`text` uses the existing SeaTunnel delimited-text serializer in schema field order.

### field_delimiter [string]

Nonempty delimiter when `format = text`. Text format is not a general escaping/CSV contract;
use JSON when field values can contain the delimiter.

### batch_size [int]

Positive maximum event count in a writer's batch.
The Azure SDK also enforces the negotiated AMQP batch byte limit through `EventDataBatch.tryAdd`.
A full batch is sent synchronously before more events are accepted. An individual event that
cannot fit an empty batch fails the task; it is never split or dropped.

Each writer retains at most one pending SDK batch, plus the event being serialized.
This bounds retained batches, not the size of an input row allocated upstream.

### partition_id [string]

Optional fixed destination partition. Use only when the pipeline deliberately targets a known partition.
Cannot be combined with `partition_key`.

### partition_key [string]

Optional static key used by Event Hubs to route this writer's batches to the same partition.
This is a literal value, not a field name or row expression. Cannot be combined with `partition_id`.
The key must be nonblank and at most 128 UTF-16 code units, matching the Azure Java SDK limit.
Without either option, the SDK chooses partitions.
A fixed partition or key can limit throughput; parallel writers do not establish a global row order.

### common options

See [Sink Common Options](../common-options/sink-common-options.md).

## Delivery And Failure Behavior

Checkpoint preparation and final batch commit synchronously flush partial batches.
Streaming jobs require checkpointing so low-volume partial batches are not buffered indefinitely.
There is no connector timer; checkpoint frequency determines the flush opportunity when batches do not fill.

Delivery is at-least-once with checkpoint recovery. A send acknowledged before a job failure can be replayed
from the previous checkpoint. The connector does not provide transactions, exactly-once delivery or deduplication.

The Azure SDK owns AMQP retries. When batch creation or sending fails, the failure is retained and subsequent
writes/checkpoints fail. Cleanup closes the producer without retrying a failed batch whose delivery is uncertain.
The connector does not expose custom retry settings in V1. Authentication errors, throttling after SDK retry
exhaustion and invalid partitions fail the task rather than silently discarding events.
The SDK defaults use a one-minute attempt timeout and up to three retries. A synchronous flush can therefore
delay checkpoint preparation or cancellation for minutes during a service outage; there is no separate
connector-level total deadline. Size the job's checkpoint timeout accordingly.

## Task Example

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 1
    schema {
      fields {
        event_id = string
        event_type = string
      }
    }
    rows = [{kind = INSERT, fields = ["order-101", "created"]}]
  }
}

sink {
  AzureEventHubs {
    connection_string = "Endpoint=sb://my-namespace.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey=..."
    event_hub_name = "events"
    format = json
    batch_size = 100
  }
}
```

For a streaming source, set `job.mode = "STREAMING"` and enable `checkpoint.interval` in `env`.

## Changelog

<ChangeLog />
