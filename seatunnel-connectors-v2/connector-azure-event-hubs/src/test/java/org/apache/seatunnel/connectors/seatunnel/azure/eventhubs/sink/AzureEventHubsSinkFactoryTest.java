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

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.exception.AzureEventHubsConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.azure.messaging.eventhubs.EventHubProducerClient;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.lang.invoke.SerializedLambda;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

class AzureEventHubsSinkFactoryTest {

    @Test
    void factoryExposesRequiredAndOptionalSinkOptions() {
        AzureEventHubsSinkFactory factory = new AzureEventHubsSinkFactory();

        Assertions.assertEquals("AzureEventHubs", factory.factoryIdentifier());
        Assertions.assertEquals(
                Arrays.asList("connection_string", "event_hub_name"),
                factory.optionRule().getRequiredOptions().stream()
                        .flatMap(required -> required.getOptions().stream())
                        .map(option -> option.key())
                        .collect(Collectors.toList()));
        Assertions.assertEquals(
                Arrays.asList(
                        AzureEventHubsSinkOptions.FORMAT,
                        AzureEventHubsSinkOptions.FIELD_DELIMITER,
                        AzureEventHubsSinkOptions.BATCH_SIZE,
                        AzureEventHubsSinkOptions.PARTITION_KEY,
                        AzureEventHubsSinkOptions.PARTITION_ID),
                factory.optionRule().getOptionalOptions());
    }

    @Test
    void factoryCreatesSerializableSinkWithoutOpeningAConnection() throws Exception {
        CatalogTable table = CatalogTableUtil.buildSimpleTextTable();
        AzureEventHubsSink sink =
                (AzureEventHubsSink)
                        new AzureEventHubsSinkFactory()
                                .createSink(context(table, options()))
                                .createSink();

        AzureEventHubsSink restored =
                SerializationUtils.deserialize(SerializationUtils.serialize(sink));

        Assertions.assertEquals("AzureEventHubs", restored.getPluginName());
        Assertions.assertEquals(
                table.getSeaTunnelRowType(),
                restored.getWriteCatalogTable().get().getSeaTunnelRowType());
        Assertions.assertSame(table, sink.getWriteCatalogTable().get());
        Assertions.assertFalse(restored.createCommitter().isPresent());
        Assertions.assertFalse(restored.createAggregatedCommitter().isPresent());
        Assertions.assertFalse(restored.getWriterStateSerializer().isPresent());
    }

    @Test
    void defaultProducerFactoryDoesNotSerializeLambdaDescriptors() throws Exception {
        AzureEventHubsSink sink =
                new AzureEventHubsSink(config(), CatalogTableUtil.buildSimpleTextTable());
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                ObjectOutputStream output =
                        new ObjectOutputStream(bytes) {
                            @Override
                            protected void annotateClass(Class<?> type) {
                                Assertions.assertNotEquals(
                                        SerializedLambda.class,
                                        type,
                                        "The sink must not serialize SDK lambda descriptors that shading can invalidate");
                            }
                        }) {
            output.writeObject(sink);
            output.flush();
            AzureEventHubsSink restored = SerializationUtils.deserialize(bytes.toByteArray());
            Assertions.assertEquals(sink.getPluginName(), restored.getPluginName());
        }
    }

    @Test
    void factoryRejectsInvalidConfigurationBeforeDispatch() {
        Map<String, Object> options = options();
        options.put("partition_id", "0");
        options.put("partition_key", "seatunnel");
        TableSinkFactoryContext context = context(CatalogTableUtil.buildSimpleTextTable(), options);

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new AzureEventHubsSinkFactory().createSink(context));
    }

    @Test
    void sinkRequiresCheckpointingForStreamingButAllowsBatchJobs() {
        AzureEventHubsSink sink =
                new AzureEventHubsSink(config(), CatalogTableUtil.buildSimpleTextTable());

        AzureEventHubsConnectorException failure =
                Assertions.assertThrows(
                        AzureEventHubsConnectorException.class,
                        () ->
                                sink.setJobContext(
                                        new JobContext()
                                                .setJobMode(JobMode.STREAMING)
                                                .setEnableCheckpoint(false)));
        Assertions.assertTrue(failure.getMessage().contains("requires checkpointing"));
        Assertions.assertDoesNotThrow(
                () ->
                        sink.setJobContext(
                                new JobContext()
                                        .setJobMode(JobMode.STREAMING)
                                        .setEnableCheckpoint(true)));
        Assertions.assertDoesNotThrow(
                () ->
                        sink.setJobContext(
                                new JobContext()
                                        .setJobMode(JobMode.BATCH)
                                        .setEnableCheckpoint(false)));
    }

    @Test
    void eachWriterAndRestoredWriterOwnsOneProducer() throws Exception {
        EventHubsProducerFactory producerFactory = Mockito.mock(EventHubsProducerFactory.class);
        EventHubProducerClient first = Mockito.mock(EventHubProducerClient.class);
        EventHubProducerClient restored = Mockito.mock(EventHubProducerClient.class);
        AzureEventHubsSinkConfig config = config();
        Mockito.when(producerFactory.create(config)).thenReturn(first, restored);
        AzureEventHubsSink sink =
                new AzureEventHubsSink(
                        config, CatalogTableUtil.buildSimpleTextTable(), producerFactory);
        Mockito.verifyNoInteractions(producerFactory);

        SinkWriter<SeaTunnelRow, Void, Void> writer =
                sink.createWriter(Mockito.mock(SinkWriter.Context.class));
        writer.prepareCommit();
        writer.prepareCommit();
        writer.close();
        SinkWriter<SeaTunnelRow, Void, Void> restoredWriter =
                sink.restoreWriter(Mockito.mock(SinkWriter.Context.class), Collections.emptyList());
        restoredWriter.close();

        Mockito.verify(producerFactory, Mockito.times(2)).create(config);
        Mockito.verify(first).close();
        Mockito.verify(restored).close();
        Mockito.verifyNoMoreInteractions(first, restored);
    }

    @Test
    void invalidSdkConnectionConfigurationIsWrapped() {
        Map<String, Object> options = options();
        options.put("connection_string", "not-a-connection-string");
        AzureEventHubsSinkConfig config =
                AzureEventHubsSinkConfig.from(ReadonlyConfig.fromMap(options));

        AzureEventHubsConnectorException failure =
                Assertions.assertThrows(
                        AzureEventHubsConnectorException.class,
                        () -> AzureEventHubsSinkWriter.createProducer(config));

        Assertions.assertNotNull(failure.getCause());
        Assertions.assertTrue(failure.getMessage().contains("Failed to create"));
    }

    private TableSinkFactoryContext context(CatalogTable table, Map<String, Object> options) {
        return new TableSinkFactoryContext(
                table, ReadonlyConfig.fromMap(options), getClass().getClassLoader());
    }

    private AzureEventHubsSinkConfig config() {
        return AzureEventHubsSinkConfig.from(ReadonlyConfig.fromMap(options()));
    }

    private Map<String, Object> options() {
        Map<String, Object> options = new HashMap<>();
        options.put(
                "connection_string",
                "Endpoint=sb://example/;SharedAccessKeyName=name;SharedAccessKey=key;");
        options.put("event_hub_name", "events");
        return options;
    }
}
