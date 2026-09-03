# Avro 格式

Avro 在流式数据处理管道中非常流行。现在seatunnel在kafka连接器中支持Avro格式

:::note 不支持查询 Confluent Schema Registry

该格式不会访问 schema registry，也不会根据 schema ID 查询 schema。Kafka Source 在设置 `strip_schema_registry_header = true` 并通过 `avro_schema` 提供匹配的 writer schema 后，可以消费带有 5 字节 Confluent 线上格式头的 Avro 消息。schema ID 仅被视为不透明元数据。默认不剥离头部，因此原始 Avro 消息的现有行为保持不变。

:::

# 怎样用

## Kafka 使用示例

- 模拟随机生成数据源,并以 Avro 的格式 写入 Kafka 的实例

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 90
    schema = {
      fields {
        c_map = "map<string, string>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_bytes = bytes
        c_date = date
        c_decimal = "decimal(38, 18)"
        c_timestamp = timestamp
        c_row = {
          c_map = "map<string, string>"
          c_array = "array<int>"
          c_string = string
          c_boolean = boolean
          c_tinyint = tinyint
          c_smallint = smallint
          c_int = int
          c_bigint = bigint
          c_float = float
          c_double = double
          c_bytes = bytes
          c_date = date
          c_decimal = "decimal(38, 18)"
          c_timestamp = timestamp
        }
      }
    }
    plugin_output = "fake"
  }
}

sink {
  Kafka {
    bootstrap.servers = "kafkaCluster:9092"
    topic = "test_avro_topic_fake_source"
    format = avro
  }
}
```

- 从 kafka 读取 avro 格式的数据并打印到控制台的示例

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Kafka {
    bootstrap.servers = "kafkaCluster:9092"
    topic = "test_avro_topic"
    plugin_output = "kafka_table"
    start_mode = "earliest"
    format = avro
    format_error_handle_way = skip
    schema = {
      fields {
        id = bigint
        c_map = "map<string, smallint>"
        c_array = "array<tinyint>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(2, 1)"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
  }
}

sink {
  Console {
    plugin_input = "kafka_table"
  }
}
```
