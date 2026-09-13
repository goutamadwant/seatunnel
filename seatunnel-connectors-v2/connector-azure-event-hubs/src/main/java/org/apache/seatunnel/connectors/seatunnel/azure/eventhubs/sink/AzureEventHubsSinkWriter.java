/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.sink;

import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsMessageFormat;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.exception.AzureEventHubsConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.exception.AzureEventHubsConnectorException;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.format.json.JsonSerializationSchema;
import org.apache.seatunnel.format.text.TextSerializationSchema;

import com.azure.core.amqp.exception.AmqpErrorCondition;
import com.azure.core.amqp.exception.AmqpException;
import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.azure.messaging.eventhubs.models.CreateBatchOptions;

import java.util.Optional;

/**
 * Buffers at most one SDK byte-bounded batch and sends synchronously. There is no timer: a partial
 * batch waits for the next checkpoint or final commit. SDK retries may occur within one send, but a
 * failed operation is terminal for this writer and is never retried by a later flush or close.
 */
public class AzureEventHubsSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private final SerializationSchema serializationSchema;
    private final AzureEventHubsSinkConfig config;
    private final EventHubProducerClient producer;

    private EventDataBatch batch;
    private AzureEventHubsConnectorException failure;
    private boolean closed;

    AzureEventHubsSinkWriter(
            SeaTunnelRowType rowType,
            AzureEventHubsSinkConfig config,
            EventHubsProducerFactory producerFactory) {
        this.config = config;
        // Initialize serialization before opening the client so schema errors cannot leak it.
        this.serializationSchema = createSerializationSchema(rowType, config);
        this.producer = producerFactory.create(config);
    }

    /**
     * Uses SDK retry defaults (one-minute try timeout, up to three retries), not an outer retry.
     */
    static EventHubProducerClient createProducer(AzureEventHubsSinkConfig config) {
        try {
            return new EventHubClientBuilder()
                    .connectionString(config.getConnectionString(), config.getEventHubName())
                    .buildProducerClient();
        } catch (RuntimeException e) {
            throw new AzureEventHubsConnectorException(
                    AzureEventHubsConnectorErrorCode.CONNECTION_FAILED,
                    "Failed to create Azure Event Hubs producer",
                    e);
        }
    }

    /** Accepts inserts only; flushes a full batch before retrying the event in a fresh batch. */
    @Override
    public synchronized void write(SeaTunnelRow row) {
        checkOpen();
        try {
            if (row.getRowKind() != RowKind.INSERT) {
                throw new AzureEventHubsConnectorException(
                        AzureEventHubsConnectorErrorCode.WRITE_FAILED,
                        "Azure Event Hubs sink supports INSERT rows only, received "
                                + row.getRowKind());
            }
            EventData event = new EventData(serializationSchema.serialize(row));
            if (batch == null) {
                batch = createBatch();
            }
            if (!tryAdd(event)) {
                if (batch.getCount() == 0) {
                    throw oversizedEvent(null);
                }
                flush();
                batch = createBatch();
                if (!tryAdd(event)) {
                    throw oversizedEvent(null);
                }
            }
            if (batch.getCount() >= config.getBatchSize()) {
                flush();
            }
        } catch (RuntimeException e) {
            throw rememberFailure(e);
        }
    }

    /** Waits for acknowledgement before checkpoint preparation or bounded-job final commit. */
    @Override
    public synchronized Optional<Void> prepareCommit() {
        flush();
        return Optional.empty();
    }

    private EventDataBatch createBatch() {
        CreateBatchOptions options = new CreateBatchOptions();
        if (config.getPartitionKey() != null) {
            options.setPartitionKey(config.getPartitionKey());
        } else if (config.getPartitionId() != null) {
            options.setPartitionId(config.getPartitionId());
        }
        return producer.createBatch(options);
    }

    private boolean tryAdd(EventData event) {
        try {
            return batch.tryAdd(event);
        } catch (AmqpException e) {
            // The SDK throws rather than returning false when the event itself exceeds the limit.
            if (e.getErrorCondition() == AmqpErrorCondition.LINK_PAYLOAD_SIZE_EXCEEDED) {
                throw oversizedEvent(e);
            }
            throw e;
        }
    }

    private AzureEventHubsConnectorException oversizedEvent(Throwable cause) {
        return new AzureEventHubsConnectorException(
                AzureEventHubsConnectorErrorCode.MESSAGE_TOO_LARGE,
                "Serialized event cannot fit in an empty Azure Event Hubs batch with a limit of "
                        + batch.getMaxSizeInBytes()
                        + " bytes",
                cause);
    }

    private void flush() {
        checkOpen();
        try {
            if (batch != null && batch.getCount() > 0) {
                producer.send(batch);
                batch = null;
            }
        } catch (RuntimeException e) {
            throw rememberFailure(e);
        }
    }

    /**
     * Flushes a healthy writer and always closes its producer. A failed batch is not retried; the
     * original failure is preserved with cleanup failures suppressed. Repeated close is a no-op.
     */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        AzureEventHubsConnectorException closeFailure = null;
        try {
            flush();
        } catch (AzureEventHubsConnectorException e) {
            closeFailure = e;
        } finally {
            closed = true;
            batch = null;
            try {
                producer.close();
            } catch (RuntimeException e) {
                if (closeFailure == null) {
                    closeFailure =
                            new AzureEventHubsConnectorException(
                                    AzureEventHubsConnectorErrorCode.CLOSE_FAILED,
                                    "Failed to close Azure Event Hubs producer",
                                    e);
                } else if (closeFailure != e) {
                    closeFailure.addSuppressed(e);
                }
            }
        }
        if (closeFailure != null) {
            throw closeFailure;
        }
    }

    private void checkOpen() {
        if (failure != null) {
            throw failure;
        }
        if (closed) {
            throw new IllegalStateException("Azure Event Hubs sink writer is closed");
        }
    }

    private AzureEventHubsConnectorException rememberFailure(RuntimeException cause) {
        if (failure == null) {
            failure =
                    cause instanceof AzureEventHubsConnectorException
                            ? (AzureEventHubsConnectorException) cause
                            : new AzureEventHubsConnectorException(
                                    AzureEventHubsConnectorErrorCode.WRITE_FAILED,
                                    "Failed to write Azure Event Hubs event",
                                    cause);
        }
        return failure;
    }

    private static SerializationSchema createSerializationSchema(
            SeaTunnelRowType rowType, AzureEventHubsSinkConfig config) {
        if (config.getFormat() == AzureEventHubsMessageFormat.JSON) {
            return new JsonSerializationSchema(rowType);
        }
        return TextSerializationSchema.builder()
                .seaTunnelRowType(rowType)
                .delimiter(config.getFieldDelimiter())
                .build();
    }
}
