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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.exception.AzureEventHubsConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.exception.AzureEventHubsConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.azure.core.amqp.exception.AmqpErrorCondition;
import com.azure.core.amqp.exception.AmqpException;
import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.azure.messaging.eventhubs.models.CreateBatchOptions;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

class AzureEventHubsSinkWriterTest {

    private static final SeaTunnelRowType ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"id", "name"},
                    new SeaTunnelDataType<?>[] {BasicType.LONG_TYPE, BasicType.STRING_TYPE});

    @Test
    void idleWriterDoesNotCreateOrSendEmptyBatchesAndCloseIsIdempotent() throws Exception {
        EventHubProducerClient producer = Mockito.mock(EventHubProducerClient.class);
        AzureEventHubsSinkWriter writer = writer(producer, options());

        Assertions.assertFalse(writer.prepareCommit().isPresent());
        Assertions.assertFalse(writer.prepareCommit(1L).isPresent());
        Assertions.assertTrue(writer.snapshotState(1L).isEmpty());
        writer.close();
        writer.close();

        Mockito.verify(producer).close();
        Mockito.verifyNoMoreInteractions(producer);
        Assertions.assertThrows(IllegalStateException.class, () -> writer.write(row("late")));
        Assertions.assertThrows(IllegalStateException.class, writer::prepareCommit);
    }

    @Test
    void countFlushAndFinalCommitSendFullBatchAndTailOnce() {
        EventHubProducerClient producer = Mockito.mock(EventHubProducerClient.class);
        TestBatch full = new TestBatch(1024);
        TestBatch tail = new TestBatch(1024);
        Mockito.when(producer.createBatch(Mockito.any(CreateBatchOptions.class)))
                .thenReturn(full.batch, tail.batch);
        Map<String, Object> options = options();
        options.put("batch_size", 2);
        AzureEventHubsSinkWriter writer = writer(producer, options);

        writer.write(row("first"));
        Mockito.verify(producer, Mockito.never()).send(Mockito.any(EventDataBatch.class));
        writer.write(row("second"));
        Mockito.verify(producer).send(full.batch);
        writer.write(row("third"));
        Mockito.verify(producer, Mockito.never()).send(tail.batch);
        writer.prepareCommit();
        writer.prepareCommit();
        writer.close();

        InOrder order = Mockito.inOrder(producer);
        order.verify(producer).createBatch(Mockito.any(CreateBatchOptions.class));
        order.verify(producer).send(full.batch);
        order.verify(producer).createBatch(Mockito.any(CreateBatchOptions.class));
        order.verify(producer).send(tail.batch);
        order.verify(producer).close();
        order.verifyNoMoreInteractions();
        Assertions.assertEquals(2, full.events.size());
        Assertions.assertEquals(1, tail.events.size());
    }

    @Test
    void byteRolloverSendsBeforeAllocatingNextBatchAndReusesTheSerializedEvent() {
        EventHubProducerClient producer = Mockito.mock(EventHubProducerClient.class);
        TestBatch first = new TestBatch(40);
        TestBatch second = new TestBatch(40);
        Mockito.when(producer.createBatch(Mockito.any(CreateBatchOptions.class)))
                .thenReturn(first.batch, second.batch);
        Map<String, Object> options = options();
        options.put("format", "text");
        AzureEventHubsSinkWriter writer = writer(producer, options);

        writer.write(row("\u6f22"));
        writer.write(row("\u6f22"));

        Assertions.assertEquals(1, first.events.size());
        Assertions.assertEquals(1, second.events.size());
        Assertions.assertSame(first.attempts.get(1), second.attempts.get(0));
        InOrder order = Mockito.inOrder(producer);
        order.verify(producer).createBatch(Mockito.any(CreateBatchOptions.class));
        order.verify(producer).send(first.batch);
        order.verify(producer).createBatch(Mockito.any(CreateBatchOptions.class));
        order.verifyNoMoreInteractions();
        writer.prepareCommit();
        writer.close();
        Mockito.verify(producer).send(second.batch);
    }

    @Test
    void checkpointPreparationFlushesPartialBatchesBeforeEmptySnapshots() throws Exception {
        EventHubProducerClient producer = Mockito.mock(EventHubProducerClient.class);
        TestBatch checkpoint = new TestBatch(1024);
        TestBatch snapshot = new TestBatch(1024);
        Mockito.when(producer.createBatch(Mockito.any(CreateBatchOptions.class)))
                .thenReturn(checkpoint.batch, snapshot.batch);
        AzureEventHubsSinkWriter writer = writer(producer, options());

        writer.write(row("checkpoint"));
        Assertions.assertFalse(writer.prepareCommit(7L).isPresent());
        Assertions.assertTrue(writer.snapshotState(7L).isEmpty());
        Mockito.verify(producer).send(checkpoint.batch);
        writer.write(row("snapshot"));
        Assertions.assertFalse(writer.prepareCommit(8L).isPresent());
        Assertions.assertTrue(writer.snapshotState(8L).isEmpty());
        writer.prepareCommit();
        writer.close();

        Mockito.verify(producer).send(snapshot.batch);
        Mockito.verify(producer, Mockito.times(2)).send(Mockito.any(EventDataBatch.class));
    }

    @Test
    void commitWaitsUntilSynchronousSendReturns() throws Exception {
        EventHubProducerClient producer = Mockito.mock(EventHubProducerClient.class);
        TestBatch batch = new TestBatch(1024);
        Mockito.when(producer.createBatch(Mockito.any(CreateBatchOptions.class)))
                .thenReturn(batch.batch);
        CountDownLatch sendEntered = new CountDownLatch(1);
        CountDownLatch acknowledge = new CountDownLatch(1);
        Mockito.doAnswer(
                        invocation -> {
                            sendEntered.countDown();
                            Assertions.assertTrue(acknowledge.await(5, TimeUnit.SECONDS));
                            return null;
                        })
                .when(producer)
                .send(batch.batch);
        AzureEventHubsSinkWriter writer = writer(producer, options());
        writer.write(row("pending"));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> commit = executor.submit(() -> writer.prepareCommit());
            Assertions.assertTrue(sendEntered.await(5, TimeUnit.SECONDS));
            Assertions.assertFalse(commit.isDone());
            acknowledge.countDown();
            commit.get(5, TimeUnit.SECONDS);
        } finally {
            acknowledge.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            writer.close();
        }
        Mockito.verify(producer).send(batch.batch);
    }

    @Test
    void serializesJsonAndTextUsingExistingSchemas() {
        for (String format : new String[] {"json", "text"}) {
            EventHubProducerClient producer = Mockito.mock(EventHubProducerClient.class);
            TestBatch batch = new TestBatch(1024);
            Mockito.when(producer.createBatch(Mockito.any(CreateBatchOptions.class)))
                    .thenReturn(batch.batch);
            Map<String, Object> options = options();
            options.put("format", format);
            options.put("field_delimiter", "|");
            AzureEventHubsSinkWriter writer = writer(producer, options);
            writer.write(row("\u6f22"));
            writer.close();

            String expected = "json".equals(format) ? "{\"id\":1,\"name\":\"\u6f22\"}" : "1|\u6f22";
            Assertions.assertArrayEquals(
                    expected.getBytes(StandardCharsets.UTF_8), batch.events.get(0).getBody());
        }
    }

    @Test
    void staticRoutingIsAppliedToEveryNewBatch() {
        for (String routing : new String[] {"partition_key", "partition_id", "default"}) {
            EventHubProducerClient producer = Mockito.mock(EventHubProducerClient.class);
            Mockito.when(producer.createBatch(Mockito.any(CreateBatchOptions.class)))
                    .thenAnswer(ignored -> new TestBatch(1024).batch);
            Map<String, Object> options = options();
            options.put("batch_size", 1);
            if (!"default".equals(routing)) {
                options.put(routing, "partition_key".equals(routing) ? "seatunnel" : "0");
            }
            AzureEventHubsSinkWriter writer = writer(producer, options);
            writer.write(row("first"));
            writer.write(row("second"));
            writer.close();

            ArgumentCaptor<CreateBatchOptions> captor =
                    ArgumentCaptor.forClass(CreateBatchOptions.class);
            Mockito.verify(producer, Mockito.times(2)).createBatch(captor.capture());
            for (CreateBatchOptions batchOptions : captor.getAllValues()) {
                Assertions.assertEquals(
                        options.get("partition_key"), batchOptions.getPartitionKey());
                Assertions.assertEquals(options.get("partition_id"), batchOptions.getPartitionId());
                Assertions.assertEquals(0, batchOptions.getMaximumSizeInBytes());
            }
        }
    }

    @Test
    void rejectsEveryNonInsertBeforeCreatingABatch() {
        for (RowKind kind :
                new RowKind[] {RowKind.DELETE, RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER}) {
            EventHubProducerClient producer = Mockito.mock(EventHubProducerClient.class);
            AzureEventHubsSinkWriter writer = writer(producer, options());
            SeaTunnelRow row = row("change");
            row.setRowKind(kind);

            AzureEventHubsConnectorException failure =
                    Assertions.assertThrows(
                            AzureEventHubsConnectorException.class, () -> writer.write(row));
            Assertions.assertTrue(failure.getMessage().contains("INSERT rows only"));
            assertStickyAndClose(writer, failure);
            Mockito.verify(producer).close();
            Mockito.verifyNoMoreInteractions(producer);
        }
    }

    @Test
    void serializationFailureDoesNotSendEarlierBufferedRowsInClose() {
        EventHubProducerClient producer = Mockito.mock(EventHubProducerClient.class);
        TestBatch batch = new TestBatch(1024);
        Mockito.when(producer.createBatch(Mockito.any(CreateBatchOptions.class)))
                .thenReturn(batch.batch);
        AzureEventHubsSinkWriter writer = writer(producer, options());
        writer.write(row("valid"));
        SeaTunnelRow invalid = new SeaTunnelRow(new Object[0]);

        AzureEventHubsConnectorException failure =
                Assertions.assertThrows(
                        AzureEventHubsConnectorException.class, () -> writer.write(invalid));

        Assertions.assertNotNull(failure.getCause());
        assertStickyAndClose(writer, failure);
        Mockito.verify(producer, Mockito.never()).send(Mockito.any(EventDataBatch.class));
        Mockito.verify(producer).close();
    }

    @Test
    void oversizedFirstEventReturningFalseIsAttemptedOnceAndNeverSent() {
        EventHubProducerClient producer = Mockito.mock(EventHubProducerClient.class);
        TestBatch batch = new TestBatch(1);
        Mockito.when(producer.createBatch(Mockito.any(CreateBatchOptions.class)))
                .thenReturn(batch.batch);
        AzureEventHubsSinkWriter writer = writer(producer, options());

        AzureEventHubsConnectorException failure =
                Assertions.assertThrows(
                        AzureEventHubsConnectorException.class, () -> writer.write(row("large")));

        Assertions.assertEquals(
                AzureEventHubsConnectorErrorCode.MESSAGE_TOO_LARGE,
                failure.getSeaTunnelErrorCode());
        Assertions.assertEquals(1, batch.attempts.size());
        assertStickyAndClose(writer, failure);
        Mockito.verify(producer, Mockito.never()).send(Mockito.any(EventDataBatch.class));
        Mockito.verify(producer).createBatch(Mockito.any(CreateBatchOptions.class));
        Mockito.verify(producer).close();
    }

    @Test
    void oversizedSdkExceptionIsReportedWithOriginalCauseAndNoRetry() {
        EventHubProducerClient producer = Mockito.mock(EventHubProducerClient.class);
        EventDataBatch batch = Mockito.mock(EventDataBatch.class);
        AmqpException cause =
                new AmqpException(
                        false, AmqpErrorCondition.LINK_PAYLOAD_SIZE_EXCEEDED, "too large", null);
        Mockito.when(producer.createBatch(Mockito.any(CreateBatchOptions.class))).thenReturn(batch);
        Mockito.when(batch.tryAdd(Mockito.any(EventData.class))).thenThrow(cause);
        Mockito.when(batch.getMaxSizeInBytes()).thenReturn(1024);
        AzureEventHubsSinkWriter writer = writer(producer, options());

        AzureEventHubsConnectorException failure =
                Assertions.assertThrows(
                        AzureEventHubsConnectorException.class, () -> writer.write(row("large")));

        Assertions.assertEquals(
                AzureEventHubsConnectorErrorCode.MESSAGE_TOO_LARGE,
                failure.getSeaTunnelErrorCode());
        Assertions.assertSame(cause, failure.getCause());
        assertStickyAndClose(writer, failure);
        Mockito.verify(batch).tryAdd(Mockito.any(EventData.class));
        Mockito.verify(producer, Mockito.never()).send(Mockito.any(EventDataBatch.class));
        Mockito.verify(producer).close();
    }

    @Test
    void oversizedEventAfterRolloverFailsWithoutSendingOrRetryingTheEmptyBatch() {
        for (boolean sdkThrows : new boolean[] {false, true}) {
            EventHubProducerClient producer = Mockito.mock(EventHubProducerClient.class);
            TestBatch first = new TestBatch(1024);
            EventDataBatch empty = Mockito.mock(EventDataBatch.class);
            Mockito.when(producer.createBatch(Mockito.any(CreateBatchOptions.class)))
                    .thenReturn(first.batch, empty);
            Mockito.when(empty.getMaxSizeInBytes()).thenReturn(1024);
            if (sdkThrows) {
                Mockito.when(empty.tryAdd(Mockito.any(EventData.class)))
                        .thenThrow(
                                new AmqpException(
                                        false,
                                        AmqpErrorCondition.LINK_PAYLOAD_SIZE_EXCEEDED,
                                        "too large",
                                        null));
            }
            AzureEventHubsSinkWriter writer = writer(producer, options());
            writer.write(row("first"));
            Mockito.doReturn(false).when(first.batch).tryAdd(Mockito.any(EventData.class));

            AzureEventHubsConnectorException failure =
                    Assertions.assertThrows(
                            AzureEventHubsConnectorException.class,
                            () -> writer.write(row("oversized")));

            Assertions.assertEquals(
                    AzureEventHubsConnectorErrorCode.MESSAGE_TOO_LARGE,
                    failure.getSeaTunnelErrorCode());
            assertStickyAndClose(writer, failure);
            Mockito.verify(producer).send(first.batch);
            Mockito.verify(producer, Mockito.times(1)).send(Mockito.any(EventDataBatch.class));
            Mockito.verify(empty).tryAdd(Mockito.any(EventData.class));
            Mockito.verify(producer, Mockito.times(2))
                    .createBatch(Mockito.any(CreateBatchOptions.class));
            Mockito.verify(producer).close();
        }
    }

    @Test
    void batchCreationFailureIsStickyAndProducerStillCloses() {
        EventHubProducerClient producer = Mockito.mock(EventHubProducerClient.class);
        RuntimeException cause = new IllegalStateException("link failed");
        Mockito.when(producer.createBatch(Mockito.any(CreateBatchOptions.class))).thenThrow(cause);
        AzureEventHubsSinkWriter writer = writer(producer, options());

        AzureEventHubsConnectorException failure =
                Assertions.assertThrows(
                        AzureEventHubsConnectorException.class, () -> writer.write(row("event")));

        Assertions.assertSame(cause, failure.getCause());
        assertStickyAndClose(writer, failure);
        Mockito.verify(producer).createBatch(Mockito.any(CreateBatchOptions.class));
        Mockito.verify(producer, Mockito.never()).send(Mockito.any(EventDataBatch.class));
        Mockito.verify(producer).close();
    }

    @Test
    void nonSizeBatchErrorIsNotMistakenForCapacityRollover() {
        EventHubProducerClient producer = Mockito.mock(EventHubProducerClient.class);
        EventDataBatch batch = Mockito.mock(EventDataBatch.class);
        AmqpException cause =
                new AmqpException(false, AmqpErrorCondition.UNAUTHORIZED_ACCESS, "denied", null);
        Mockito.when(producer.createBatch(Mockito.any(CreateBatchOptions.class))).thenReturn(batch);
        Mockito.when(batch.tryAdd(Mockito.any(EventData.class))).thenThrow(cause);
        AzureEventHubsSinkWriter writer = writer(producer, options());

        AzureEventHubsConnectorException failure =
                Assertions.assertThrows(
                        AzureEventHubsConnectorException.class, () -> writer.write(row("event")));

        Assertions.assertEquals(
                AzureEventHubsConnectorErrorCode.WRITE_FAILED, failure.getSeaTunnelErrorCode());
        Assertions.assertSame(cause, failure.getCause());
        assertStickyAndClose(writer, failure);
        Mockito.verify(producer).createBatch(Mockito.any(CreateBatchOptions.class));
        Mockito.verify(producer, Mockito.never()).send(Mockito.any(EventDataBatch.class));
    }

    @Test
    void countSendFailureIsStickyAndCleanupFailureIsSuppressed() {
        EventHubProducerClient producer = Mockito.mock(EventHubProducerClient.class);
        TestBatch batch = new TestBatch(1024);
        Mockito.when(producer.createBatch(Mockito.any(CreateBatchOptions.class)))
                .thenReturn(batch.batch);
        RuntimeException sendFailure = new IllegalStateException("send failed");
        RuntimeException cleanupFailure = new IllegalStateException("close failed");
        Mockito.doThrow(sendFailure).when(producer).send(batch.batch);
        Mockito.doThrow(cleanupFailure).when(producer).close();
        Map<String, Object> options = options();
        options.put("batch_size", 1);
        AzureEventHubsSinkWriter writer = writer(producer, options);

        AzureEventHubsConnectorException failure =
                Assertions.assertThrows(
                        AzureEventHubsConnectorException.class, () -> writer.write(row("event")));

        Assertions.assertSame(sendFailure, failure.getCause());
        assertStickyAndClose(writer, failure);
        Assertions.assertArrayEquals(new Throwable[] {cleanupFailure}, failure.getSuppressed());
        Mockito.verify(producer).send(batch.batch);
        Mockito.verify(producer).close();
    }

    @Test
    void byteRolloverSendFailureDoesNotAllocateAnotherBatch() {
        EventHubProducerClient producer = Mockito.mock(EventHubProducerClient.class);
        TestBatch batch = new TestBatch(50);
        Mockito.when(producer.createBatch(Mockito.any(CreateBatchOptions.class)))
                .thenReturn(batch.batch);
        Mockito.doThrow(new IllegalStateException("send failed")).when(producer).send(batch.batch);
        AzureEventHubsSinkWriter writer = writer(producer, options());
        writer.write(row("a"));

        AzureEventHubsConnectorException failure =
                Assertions.assertThrows(
                        AzureEventHubsConnectorException.class, () -> writer.write(row("b")));

        assertStickyAndClose(writer, failure);
        Mockito.verify(producer).createBatch(Mockito.any(CreateBatchOptions.class));
        Mockito.verify(producer).send(batch.batch);
        Mockito.verify(producer).close();
    }

    @Test
    void checkpointFailureCannotBeHiddenByLaterCommitOrClose() throws Exception {
        EventHubProducerClient producer = Mockito.mock(EventHubProducerClient.class);
        TestBatch batch = new TestBatch(1024);
        Mockito.when(producer.createBatch(Mockito.any(CreateBatchOptions.class)))
                .thenReturn(batch.batch);
        Mockito.doThrow(new IllegalStateException("send failed")).when(producer).send(batch.batch);
        AzureEventHubsSinkWriter writer = writer(producer, options());
        writer.write(row("event"));

        AzureEventHubsConnectorException failure =
                Assertions.assertThrows(
                        AzureEventHubsConnectorException.class, () -> writer.prepareCommit(1L));

        assertStickyAndClose(writer, failure);
        Mockito.verify(producer).send(batch.batch);
        Mockito.verify(producer).close();
    }

    @Test
    void closeFlushFailureStillClosesOnceWithoutRetryOnRepeatedClose() {
        EventHubProducerClient producer = Mockito.mock(EventHubProducerClient.class);
        TestBatch batch = new TestBatch(1024);
        Mockito.when(producer.createBatch(Mockito.any(CreateBatchOptions.class)))
                .thenReturn(batch.batch);
        Mockito.doThrow(new IllegalStateException("send failed")).when(producer).send(batch.batch);
        AzureEventHubsSinkWriter writer = writer(producer, options());
        writer.write(row("event"));

        Assertions.assertThrows(AzureEventHubsConnectorException.class, writer::close);
        writer.close();

        Mockito.verify(producer).send(batch.batch);
        Mockito.verify(producer).close();
    }

    @Test
    void cleanupOnlyFailureDoesNotResendAnAcknowledgedBatch() {
        EventHubProducerClient producer = Mockito.mock(EventHubProducerClient.class);
        TestBatch batch = new TestBatch(1024);
        Mockito.when(producer.createBatch(Mockito.any(CreateBatchOptions.class)))
                .thenReturn(batch.batch);
        RuntimeException cause = new IllegalStateException("close failed");
        Mockito.doThrow(cause).when(producer).close();
        AzureEventHubsSinkWriter writer = writer(producer, options());
        writer.write(row("event"));

        AzureEventHubsConnectorException failure =
                Assertions.assertThrows(AzureEventHubsConnectorException.class, writer::close);
        writer.close();

        Assertions.assertEquals(
                AzureEventHubsConnectorErrorCode.CLOSE_FAILED, failure.getSeaTunnelErrorCode());
        Assertions.assertSame(cause, failure.getCause());
        Mockito.verify(producer).send(batch.batch);
        Mockito.verify(producer).close();
    }

    @Test
    void schemaInitializationPrecedesProducerCreation() {
        EventHubsProducerFactory factory = Mockito.mock(EventHubsProducerFactory.class);
        Assertions.assertThrows(
                RuntimeException.class,
                () ->
                        new AzureEventHubsSinkWriter(
                                null,
                                AzureEventHubsSinkConfig.from(ReadonlyConfig.fromMap(options())),
                                factory));
        Mockito.verifyNoInteractions(factory);
    }

    private void assertStickyAndClose(
            AzureEventHubsSinkWriter writer, AzureEventHubsConnectorException failure) {
        Assertions.assertSame(
                failure,
                Assertions.assertThrows(
                        AzureEventHubsConnectorException.class, () -> writer.write(row("later"))));
        Assertions.assertSame(
                failure,
                Assertions.assertThrows(
                        AzureEventHubsConnectorException.class, writer::prepareCommit));
        Assertions.assertSame(
                failure,
                Assertions.assertThrows(
                        AzureEventHubsConnectorException.class, () -> writer.prepareCommit(2L)));
        Assertions.assertSame(
                failure,
                Assertions.assertThrows(AzureEventHubsConnectorException.class, writer::close));
        Assertions.assertDoesNotThrow(writer::close);
    }

    private AzureEventHubsSinkWriter writer(
            EventHubProducerClient producer, Map<String, Object> options) {
        return new AzureEventHubsSinkWriter(
                ROW_TYPE,
                AzureEventHubsSinkConfig.from(ReadonlyConfig.fromMap(options)),
                ignored -> producer);
    }

    private SeaTunnelRow row(String name) {
        return new SeaTunnelRow(new Object[] {1L, name});
    }

    private Map<String, Object> options() {
        Map<String, Object> options = new HashMap<>();
        options.put(
                "connection_string",
                "Endpoint=sb://example/;SharedAccessKeyName=name;SharedAccessKey=key;");
        options.put("event_hub_name", "events");
        return options;
    }

    /** Models SDK byte admission deterministically without connecting to Azure. */
    private static class TestBatch {
        private final EventDataBatch batch = Mockito.mock(EventDataBatch.class);
        private final List<EventData> events = new ArrayList<>();
        private final List<EventData> attempts = new ArrayList<>();
        private int size;

        private TestBatch(int maximumBytes) {
            Mockito.when(batch.getCount()).thenAnswer(ignored -> events.size());
            Mockito.when(batch.getMaxSizeInBytes()).thenReturn(maximumBytes);
            Mockito.when(batch.getSizeInBytes()).thenAnswer(ignored -> size);
            Mockito.when(batch.tryAdd(Mockito.any(EventData.class)))
                    .thenAnswer(
                            invocation -> {
                                EventData event = invocation.getArgument(0);
                                attempts.add(event);
                                int eventBytes = event.getBody().length + 16;
                                if (size + eventBytes > maximumBytes) {
                                    return false;
                                }
                                size += eventBytes;
                                events.add(event);
                                return true;
                            });
        }
    }
}
