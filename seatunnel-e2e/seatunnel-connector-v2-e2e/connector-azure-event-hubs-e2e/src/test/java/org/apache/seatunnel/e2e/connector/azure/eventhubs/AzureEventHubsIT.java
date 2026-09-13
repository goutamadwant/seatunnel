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
package org.apache.seatunnel.e2e.connector.azure.eventhubs;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.exception.AzureEventHubsConnectorException;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.sink.AzureEventHubsSink;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubConsumerClient;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.azure.messaging.eventhubs.PartitionProperties;
import com.azure.messaging.eventhubs.models.CreateBatchOptions;
import com.azure.messaging.eventhubs.models.EventPosition;
import com.azure.messaging.eventhubs.models.PartitionEvent;
import com.azure.messaging.eventhubs.models.SendOptions;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

@Slf4j
public class AzureEventHubsIT extends TestSuiteBase implements TestResource {

    private static final String EVENT_HUBS_IMAGE =
            "mcr.microsoft.com/azure-messaging/eventhubs-emulator:2.2.1";
    private static final String AZURITE_IMAGE = "mcr.microsoft.com/azure-storage/azurite:3.35.0";
    private static final String EVENT_HUBS_HOST = "eventhubs-emulator";
    private static final String AZURITE_HOST = "azurite";
    private static final int AMQP_PORT = 5672;
    private static final String EVENT_HUB_NAME = "events";
    private static final String JOB_CONFIG = "/eventhubs/azure_event_hubs_to_console.conf";
    private static final String PARTITION_0_EVENT = "eventhubs-partition-0";
    private static final String PARTITION_1_EVENT = "eventhubs-partition-1";
    private static final String SHARED_ACCESS_KEY = "SAS_KEY_VALUE";

    private GenericContainer<?> azurite;
    private GenericContainer<?> emulator;
    private EventHubProducerClient producer;

    @BeforeAll
    @Override
    public void startUp() {
        DockerImageName azuriteImage = DockerImageName.parse(AZURITE_IMAGE);
        azurite =
                new GenericContainer<>(azuriteImage)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(AZURITE_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                azuriteImage.asCanonicalNameString())));
        Startables.deepStart(Stream.of(azurite)).join();

        DockerImageName emulatorImage = DockerImageName.parse(EVENT_HUBS_IMAGE);
        emulator =
                new GenericContainer<>(emulatorImage)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(EVENT_HUBS_HOST)
                        .withExposedPorts(AMQP_PORT)
                        .withEnv("BLOB_SERVER", AZURITE_HOST)
                        .withEnv("METADATA_SERVER", AZURITE_HOST)
                        .withEnv("ACCEPT_EULA", "Y")
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("eventhubs/Config.json"),
                                "/Eventhubs_Emulator/ConfigFiles/Config.json")
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                emulatorImage.asCanonicalNameString())))
                        .waitingFor(
                                new HostPortWaitStrategy()
                                        .withStartupTimeout(Duration.ofMinutes(3)));
        Startables.deepStart(Stream.of(emulator)).join();

        producer =
                new EventHubClientBuilder()
                        .connectionString(hostConnectionString(), EVENT_HUB_NAME)
                        .buildProducerClient();
        await().atMost(60, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .ignoreExceptions()
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        2,
                                        producer.getPartitionIds().stream().count(),
                                        "Event Hubs emulator partitions are not ready"));
        SeaTunnelContainer.enableAzureSdkReactorThreadExemption();
    }

    @AfterAll
    @Override
    public void tearDown() {
        try {
            if (producer != null) {
                producer.close();
            }
            if (emulator != null) {
                emulator.close();
            }
            if (azurite != null) {
                azurite.close();
            }
        } finally {
            SeaTunnelContainer.disableAzureSdkReactorThreadExemption();
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason =
                    "The source checkpoint assertion uses the Zeta REST job status and server logs")
    public void testReadsAllPartitionsAndFlushesSinkOnCheckpoint(TestContainer container)
            throws Exception {
        sendToPartition("0", PARTITION_0_EVENT);
        sendToPartition("1", PARTITION_1_EVENT);

        String jobId = String.valueOf(JobIdGenerator.newJobId());
        CompletableFuture<Container.ExecResult> jobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.executeJob(JOB_CONFIG, jobId);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        try {
            await().atMost(60, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertJobStillRunning(jobFuture);
                                Assertions.assertEquals("RUNNING", container.getJobStatus(jobId));
                            });
            await().atMost(60, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertJobStillRunning(jobFuture);
                                String logs = container.getServerLogs();
                                Assertions.assertTrue(
                                        logs.contains(PARTITION_0_EVENT),
                                        "Partition 0 event was not emitted by the source");
                                Assertions.assertTrue(
                                        logs.contains(PARTITION_1_EVENT),
                                        "Partition 1 event was not emitted by the source");
                            });
            await().atMost(60, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertJobStillRunning(jobFuture);
                                Assertions.assertTrue(
                                        container.getCompletedCheckpointCount(jobId) > 0,
                                        "No checkpoint completed after the events were emitted");
                            });
            // The batch threshold is larger than the input. These sends require a checkpoint flush.
            try (EventHubConsumerClient consumer =
                    new EventHubClientBuilder()
                            .connectionString(hostConnectionString(), "sink-checkpoint")
                            .consumerGroup("seatunnel")
                            .buildConsumerClient()) {
                Set<String> received = new HashSet<>();
                consumer.receiveFromPartition(
                                "0", 2, EventPosition.earliest(), Duration.ofSeconds(30))
                        .forEach(
                                event ->
                                        received.add(
                                                JsonUtils.parseObject(
                                                                event.getData().getBodyAsString())
                                                        .get("event_id")
                                                        .asText()));
                Assertions.assertEquals(
                        new HashSet<>(Arrays.asList(PARTITION_0_EVENT, PARTITION_1_EVENT)),
                        received);
            }
            assertJobStillRunning(jobFuture);
            Assertions.assertEquals("RUNNING", container.getJobStatus(jobId));
        } finally {
            if (!jobFuture.isDone()) {
                Container.ExecResult cancelResult = container.cancelJob(jobId);
                Assertions.assertEquals(0, cancelResult.getExitCode(), cancelResult.getStderr());
            }
        }

        Container.ExecResult jobResult = jobFuture.get(120, TimeUnit.SECONDS);
        Assertions.assertEquals(0, jobResult.getExitCode(), jobResult.getStderr());
    }

    private void sendToPartition(String partitionId, String eventId) {
        String body = "{\"event_id\":\"" + eventId + "\",\"event_type\":\"created\"}";
        producer.send(
                Collections.singletonList(new EventData(body)),
                new SendOptions().setPartitionId(partitionId));
    }

    @TestTemplate
    public void testSinkJsonBatchesAndFinalFlush(TestContainer container) throws Exception {
        List<PartitionEvent> events =
                runSinkAndRead(container, "sink-json", "/eventhubs/fake_to_event_hubs_json.conf");
        List<String> names = new ArrayList<>();
        for (PartitionEvent event : events) {
            Assertions.assertEquals("0", event.getPartitionContext().getPartitionId());
            names.add(
                    JsonUtils.parseObject(event.getData().getBodyAsString()).get("name").asText());
            Assertions.assertEquals(
                    30,
                    JsonUtils.parseObject(event.getData().getBodyAsString()).get("age").asInt());
        }
        Assertions.assertEquals(Arrays.asList("alice", "bob", "carol"), names);
    }

    @Test
    public void testWriterUsesNegotiatedByteLimit() throws Exception {
        String hub = "sink-bytes";
        try (EventHubProducerClient limitClient =
                        new EventHubClientBuilder()
                                .connectionString(hostConnectionString(), hub)
                                .buildProducerClient();
                EventHubConsumerClient consumer =
                        new EventHubClientBuilder()
                                .connectionString(hostConnectionString(), hub)
                                .consumerGroup("seatunnel")
                                .buildConsumerClient()) {
            EventDataBatch probe =
                    limitClient.createBatch(new CreateBatchOptions().setPartitionId("0"));
            int limit = probe.getMaxSizeInBytes();
            char[] chars = new char[limit / 2 + 64];
            Arrays.fill(chars, 'a');
            String body = new String(chars);
            Arrays.fill(chars, 'c');
            String secondBody = new String(chars);
            Assertions.assertTrue(probe.tryAdd(new EventData(body)));
            Assertions.assertFalse(
                    probe.tryAdd(new EventData(secondBody)),
                    "Two events must exceed the real batch limit");

            Map<String, Object> options = new HashMap<>();
            options.put("connection_string", hostConnectionString());
            options.put("event_hub_name", hub);
            options.put("format", "text");
            options.put("partition_id", "0");
            AzureEventHubsSink sink =
                    new AzureEventHubsSink(
                            AzureEventHubsSinkConfig.from(ReadonlyConfig.fromMap(options)),
                            CatalogTableUtil.buildSimpleTextTable());
            SinkWriter<SeaTunnelRow, Void, Void> batchWriter = sink.createWriter(null);
            try {
                batchWriter.write(new SeaTunnelRow(new Object[] {body}));
                batchWriter.write(new SeaTunnelRow(new Object[] {secondBody}));
                batchWriter.prepareCommit();
            } finally {
                batchWriter.close();
            }
            List<String> received = new ArrayList<>();
            consumer.receiveFromPartition("0", 3, EventPosition.earliest(), Duration.ofSeconds(15))
                    .forEach(event -> received.add(event.getData().getBodyAsString()));
            Assertions.assertEquals(Arrays.asList(body, secondBody), received);

            char[] oversized = new char[limit + 1];
            Arrays.fill(oversized, 'b');
            SinkWriter<SeaTunnelRow, Void, Void> writer = sink.createWriter(null);
            try {
                AzureEventHubsConnectorException failure =
                        Assertions.assertThrows(
                                AzureEventHubsConnectorException.class,
                                () ->
                                        writer.write(
                                                new SeaTunnelRow(
                                                        new Object[] {new String(oversized)})));
                Assertions.assertTrue(failure.getMessage().contains("cannot fit"));
            } finally {
                // A terminal writer surfaces its original failure while releasing the producer.
                Assertions.assertThrows(AzureEventHubsConnectorException.class, writer::close);
            }
            Assertions.assertEquals(
                    1, consumer.getPartitionProperties("0").getLastEnqueuedSequenceNumber());
        }
    }

    @TestTemplate
    public void testSinkTextPartitionKey(TestContainer container) throws Exception {
        List<PartitionEvent> events =
                runSinkAndRead(container, "sink-text", "/eventhubs/fake_to_event_hubs_text.conf");
        List<String> bodies = new ArrayList<>();
        for (PartitionEvent event : events) {
            Assertions.assertEquals("seatunnel", event.getData().getPartitionKey());
            Assertions.assertEquals(
                    events.get(0).getPartitionContext().getPartitionId(),
                    event.getPartitionContext().getPartitionId());
            bodies.add(event.getData().getBodyAsString());
        }
        Assertions.assertEquals(Arrays.asList("alice|30", "bob|30", "carol|30"), bodies);
    }

    private List<PartitionEvent> runSinkAndRead(TestContainer container, String hub, String config)
            throws Exception {
        try (EventHubConsumerClient consumer =
                new EventHubClientBuilder()
                        .connectionString(hostConnectionString(), hub)
                        .consumerGroup("seatunnel")
                        .buildConsumerClient()) {
            Map<String, EventPosition> starts = new HashMap<>();
            for (String partition : consumer.getPartitionIds()) {
                PartitionProperties properties = consumer.getPartitionProperties(partition);
                starts.put(
                        partition,
                        properties.isEmpty()
                                ? EventPosition.earliest()
                                : EventPosition.fromSequenceNumber(
                                        properties.getLastEnqueuedSequenceNumber(), false));
            }
            Container.ExecResult result = container.executeJob(config);
            Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
            List<PartitionEvent> events = new ArrayList<>();
            // Read after the captured offsets so each engine invocation verifies only its own job.
            for (Map.Entry<String, EventPosition> start : starts.entrySet()) {
                consumer.receiveFromPartition(
                                start.getKey(), 4, start.getValue(), Duration.ofSeconds(15))
                        .forEach(events::add);
            }
            Assertions.assertEquals(
                    3, events.size(), "Missing events or unexpected duplicate sends");
            return events;
        }
    }

    private void assertJobStillRunning(CompletableFuture<Container.ExecResult> jobFuture)
            throws Exception {
        if (jobFuture.isDone()) {
            Container.ExecResult result = jobFuture.get();
            Assertions.fail("Streaming source job terminated early:\n" + result.getStderr());
        }
    }

    private String hostConnectionString() {
        return "Endpoint=sb://"
                + emulator.getHost()
                + ":"
                + emulator.getMappedPort(AMQP_PORT)
                + ";SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey="
                + SHARED_ACCESS_KEY
                + ";UseDevelopmentEmulator=true;";
    }
}
