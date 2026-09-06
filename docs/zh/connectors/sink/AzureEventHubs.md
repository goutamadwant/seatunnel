<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

import ChangeLog from '../changelog/connector-azure-event-hubs.md';

# AzureEventHubs

> Azure Event Hubs Sink 连接器

## 描述

使用原生 Azure AMQP 生产者将 SeaTunnel 行写入一个已存在的 Event Hub。
复用 Azure Event Hubs Source 的客户端依赖与连接字符串配置。
使用 Kafka 协议的部署仍可选择现有 Kafka 连接器连接 Event Hubs 的兼容端点。

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要功能

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)

仅接受 INSERT 行。UPDATE_BEFORE、UPDATE_AFTER 和 DELETE 会导致任务失败，避免静默丢失操作语义。

## 配置项

| 名称 | 类型 | 必填 | 默认值 |
| --- | --- | --- | --- |
| connection_string | string | 是 | - |
| event_hub_name | string | 是 | - |
| format | enum | 否 | json |
| field_delimiter | string | 否 | , |
| batch_size | int | 否 | 100 |
| partition_id | string | 否 | - |
| partition_key | string | 否 | - |
| common-options | | 否 | - |

### connection_string [string]

具有目标 Event Hub 发送权限的 Azure 命名空间连接字符串。
必须单独配置 `event_hub_name`，不允许连接字符串包含 `EntityPath`。
SeaTunnel 会对该配置脱敏；不要将真实凭据提交到配置文件中。

首个版本仅支持连接字符串，不支持 Microsoft Entra ID 或托管身份。
使用官方模拟器时，在连接字符串中设置 `UseDevelopmentEmulator=true`。
模拟器不能验证真实 Azure 的认证、授权或服务配额。

### event_hub_name [string]

接收事件的现有 Event Hub。连接器不会创建命名空间、Event Hub 或分区。

### format [enum]

`json` 将每行序列化为包含 schema 字段名的 JSON 对象。
`text` 复用 SeaTunnel 文本序列化器，按 schema 字段顺序输出。

### field_delimiter [string]

`format = text` 时使用的非空分隔符。文本格式不提供通用转义或 CSV 语义；
字段值可能包含分隔符时，请使用 JSON。

### batch_size [int]

每个 writer 批次中事件数量的正整数上限。
Azure SDK 同时通过 `EventDataBatch.tryAdd` 检查协商的 AMQP 字节大小限制。
批次达到限制后，同步发送并等待确认，再接受更多事件。
单个事件无法装入空批次时任务失败，不会拆分或丢弃事件。

每个 writer 最多保留一个 SDK 批次以及当前正在序列化的事件。
该限制不限制上游已分配的单行数据大小。

### partition_id [string]

可选的固定目标分区，仅用于需要明确指定已知分区的场景。
不能与 `partition_key` 同时配置。

### partition_key [string]

可选的固定分区键，由 Event Hubs 将批次映射到同一分区。
这是字面量，不是字段名或行表达式。不能与 `partition_id` 同时配置。
分区键不能为空白，长度不能超过 128 个 UTF-16 代码单元，与 Azure Java SDK 的限制一致。
两者均未配置时，由 SDK 选择分区。
固定分区或键可能限制吞吐量；多个并行 writer 不保证全局行顺序。

### 通用配置

参见 [Sink 通用配置](../common-options/sink-common-options.md)。

## 投递与失败语义

准备 checkpoint 和最终批处理提交时，会同步发送尚未满的批次。
流作业必须启用 checkpoint，避免低流量时部分批次一直留在缓冲区。
连接器没有额外定时器；批次未满时，checkpoint 频率决定发送机会。

启用 checkpoint 恢复时提供至少一次投递。发送已确认但作业失败时，
恢复上一个 checkpoint 可能重放事件。不提供事务、精确一次或去重保证。

AMQP 重试由 Azure SDK 负责。创建批次或发送失败后，连接器保留失败状态，
后续写入和 checkpoint 均失败。清理时关闭生产者，不重新发送投递结果不确定的失败批次。
V1 不暴露自定义重试配置。认证失败、SDK 重试耗尽后的限流或无效分区都会使任务失败，不会静默丢弃事件。
SDK 默认单次尝试超时为一分钟，最多重试三次。服务不可用时，同步发送可能使 checkpoint
准备或取消等待数分钟；连接器没有额外的整体截止时间。请据此配置作业 checkpoint 超时。

## 作业示例

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

流作业应在 `env` 中设置 `job.mode = "STREAMING"` 并启用 `checkpoint.interval`。

## 变更日志

<ChangeLog />
