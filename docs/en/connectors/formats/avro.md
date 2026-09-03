# Avro format

Avro is very popular in streaming data pipeline. Now seatunnel supports Avro format in kafka connector.

:::note Confluent Schema Registry lookup is not supported

This format does not contact a schema registry or resolve a schema ID. Kafka Source can consume Avro messages with the 5-byte Confluent wire-format header when `strip_schema_registry_header = true` and the matching writer schema is supplied through `avro_schema`. The schema ID is treated as opaque metadata. Header stripping is disabled by default, so plain Avro messages keep their existing behavior.

:::

# How To Use

## Kafka uses example

- This is an example to generate data from fake source and sink to kafka with avro format.

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

- This is an example read data from kafka with avro format and print to console.

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
