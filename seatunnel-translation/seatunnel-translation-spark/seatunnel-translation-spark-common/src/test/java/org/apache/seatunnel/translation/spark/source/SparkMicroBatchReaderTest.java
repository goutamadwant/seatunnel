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

package org.apache.seatunnel.translation.spark.source;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportCoordinate;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.translation.spark.execution.MultiTableManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SparkMicroBatchReaderTest {
    @TempDir Path checkpoint;
    static final SeaTunnelRowType TYPE =
            new SeaTunnelRowType(new String[] {"id"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});

    static CatalogTable table() {
        return CatalogTableUtil.getCatalogTable("test", "db", "schema", "records", TYPE);
    }

    private void writeSparkLog(String relative, String content) throws Exception {
        java.nio.file.Path path = checkpoint.resolve(relative);
        java.nio.file.Files.createDirectories(path.getParent());
        java.nio.file.Files.write(path, content.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private void publishDriverState(long id, SparkSourceCheckpoint.Phase phase) throws Exception {
        java.nio.file.Path path =
                checkpoint.resolve("sources/0/seatunnel-batches/0/" + id + "/state");
        java.nio.file.Files.createDirectories(path.getParent());
        try (java.io.DataOutputStream output =
                new java.io.DataOutputStream(java.nio.file.Files.newOutputStream(path))) {
            new SparkSourceCheckpoint(
                            1,
                            new MultiTableManager(new CatalogTable[] {table()})
                                    .getTableSchema()
                                    .json(),
                            Collections.emptyMap(),
                            Collections.singletonList(table()),
                            Collections.singletonList(table()),
                            Collections.emptyList(),
                            phase)
                    .write(output);
        }
    }

    private java.util.Optional<SparkCommittedSourceState> committedState() throws Exception {
        return SparkCommittedSourceState.load(
                new org.apache.hadoop.conf.Configuration(), checkpoint.toString(), 0, 1, "");
    }

    @Test
    void disabledSchemaCanReplayOnlyAnEmptyOldLayoutIntervalWithoutChangingItsBytes()
            throws Exception {
        SchemaSource source = new SchemaSource();
        source.restoreCheckpointSchema = false;
        java.nio.file.Path batch = checkpoint.resolve("seatunnel-batches/0/1");
        java.nio.file.Files.createDirectories(batch);
        SparkSourceCheckpoint old =
                new SparkSourceCheckpoint(
                        1,
                        new MultiTableManager(new CatalogTable[] {table()}).getTableSchema().json(),
                        Collections.emptyMap(),
                        Collections.singletonList(table()),
                        Collections.singletonList(table()),
                        Collections.emptyList(),
                        SparkSourceCheckpoint.Phase.DATA);
        try (java.io.DataOutputStream out =
                new java.io.DataOutputStream(
                        java.nio.file.Files.newOutputStream(batch.resolve("state")))) {
            old.write(out);
        }
        byte[] metadata = java.nio.file.Files.readAllBytes(batch.resolve("state"));
        for (byte[] bytes :
                new byte[][] {
                    {0, 0, 0, 1}, {-1, -1, -1}, {-1, -1, -1, -1, 0}, {0, 0, 0, 0, -1, -1, -1, -1}
                }) {
            java.nio.file.Files.write(batch.resolve("rows"), bytes);
            try (SparkMicroBatchReader reader = schemaReader(source, 1)) {
                assertThrows(IOException.class, reader::next);
            }
            org.junit.jupiter.api.Assertions.assertArrayEquals(
                    bytes, java.nio.file.Files.readAllBytes(batch.resolve("rows")));
        }
        byte[] empty = new byte[] {-1, -1, -1, -1};
        java.nio.file.Files.write(batch.resolve("rows"), empty);
        try (SparkMicroBatchReader reader = schemaReader(source, 1)) {
            assertFalse(reader.next());
        }
        org.junit.jupiter.api.Assertions.assertArrayEquals(
                empty, java.nio.file.Files.readAllBytes(batch.resolve("rows")));
        org.junit.jupiter.api.Assertions.assertArrayEquals(
                metadata, java.nio.file.Files.readAllBytes(batch.resolve("state")));
        SparkSourceCheckpoint legacy =
                new SparkSourceCheckpoint(
                        1,
                        new MultiTableManager(new CatalogTable[] {table()}).getTableSchema().json(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        SparkSourceCheckpoint.Phase.DATA);
        legacy.validateLiveSchemaPredecessor(
                1, new MultiTableManager(new CatalogTable[] {table()}));
        assertThrows(
                IOException.class,
                () ->
                        legacy.validateLiveSchemaPredecessor(
                                1, new MultiTableManager(new CatalogTable[] {evolvedTable()})));
    }

    @Test
    void driverIgnoresPendingSchemaOffsetUntilSparkCommits() throws Exception {
        publishDriverState(1, SparkSourceCheckpoint.Phase.DATA);
        publishDriverState(2, SparkSourceCheckpoint.Phase.AFTER_SCHEMA_CHANGE);
        writeSparkLog("offsets/0", "v1\n{}\n{\"checkpointId\":1}\n");
        writeSparkLog("commits/0", "v1\n{}\n");
        writeSparkLog("offsets/1", "v1\n{}\n{\"checkpointId\":2}\n");
        SparkCommittedSourceState before = committedState().get();
        assertEquals(0, before.getEpoch());
        assertEquals(1, before.getSourceCheckpointId());
        assertEquals(SparkSourceCheckpoint.Phase.DATA, before.getCheckpoint().getPhase());
        writeSparkLog("commits/1", "v1\n{}\n");
        SparkCommittedSourceState after = committedState().get();
        assertEquals(1, after.getEpoch());
        assertEquals(2, after.getSourceCheckpointId());
        assertEquals(
                SparkSourceCheckpoint.Phase.AFTER_SCHEMA_CHANGE, after.getCheckpoint().getPhase());
    }

    @Test
    void driverDoesNotTreatOffsetIntentAsCompletedWork() throws Exception {
        writeSparkLog("offsets/99", "v1\n{}\n{\"checkpointId\":100}\n");
        assertFalse(committedState().isPresent());
    }

    @Test
    void driverFailsClosedWhenCommittedSourceJournalIsMissing() throws Exception {
        writeSparkLog("offsets/0", "v1\n{}\n{\"checkpointId\":1}\n");
        writeSparkLog("commits/0", "v1\n{}\n");
        assertThrows(IOException.class, this::committedState);
    }

    @Test
    void driverRejectsChangedSourceTopologyAndMalformedOffsets() throws Exception {
        publishDriverState(1, SparkSourceCheckpoint.Phase.DATA);
        writeSparkLog("commits/0", "v1\n{}\n");
        for (String offset :
                Arrays.asList(
                        "{\"checkpointId\":1.5}",
                        "{\"checkpointId\":\"1\"}",
                        "{\"checkpointId\":0}",
                        "null")) {
            writeSparkLog("offsets/0", "v1\n{}\n" + offset + "\n");
            assertThrows(IOException.class, this::committedState);
        }
        writeSparkLog("offsets/0", "v1\n{}\n{\"checkpointId\":1}\n{\"checkpointId\":1}\n");
        assertThrows(IOException.class, this::committedState);
        writeSparkLog("commits/0", "v2\n{}\n");
        assertThrows(IOException.class, this::committedState);
    }

    private void publishCommittedDriverBoundary() throws Exception {
        java.nio.file.Path path = checkpoint.resolve("sources/0/seatunnel-batches/0/2/state");
        java.nio.file.Files.createDirectories(path.getParent());
        org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent event =
                org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent.add(
                        table().getTableId(), evolvedTable().getTableSchema().getColumns().get(1));
        event.setChangeAfter(evolvedTable());
        try (java.io.DataOutputStream output =
                new java.io.DataOutputStream(java.nio.file.Files.newOutputStream(path))) {
            new SparkSourceCheckpoint(
                            1,
                            new MultiTableManager(new CatalogTable[] {table()})
                                    .getTableSchema()
                                    .json(),
                            Collections.emptyMap(),
                            Collections.singletonList(table()),
                            Collections.singletonList(evolvedTable()),
                            Collections.singletonList(event),
                            SparkSourceCheckpoint.Phase.AFTER_SCHEMA_CHANGE)
                    .write(output);
        }
        writeSparkLog("offsets/1", "v1\n{}\n{\"checkpointId\":2}\n");
        writeSparkLog("commits/1", "v1\n{}\n");
    }

    private org.apache.spark.sql.streaming.StreamingQuery queryFor(
            java.util.concurrent.Callable<Boolean> await, Runnable stop) {
        return (org.apache.spark.sql.streaming.StreamingQuery)
                java.lang.reflect.Proxy.newProxyInstance(
                        getClass().getClassLoader(),
                        new Class<?>[] {org.apache.spark.sql.streaming.StreamingQuery.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("awaitTermination")) return await.call();
                            if (method.getName().equals("stop")) {
                                stop.run();
                                return null;
                            }
                            throw new UnsupportedOperationException(method.getName());
                        });
    }

    @Test
    void coordinatorStopsOldTasksBeforeReconcilingAndRebuilding() throws Exception {
        List<String> calls = new ArrayList<>();
        SparkSchemaBoundaryCoordinator coordinator = new SparkSchemaBoundaryCoordinator();
        coordinator.run(
                new org.apache.hadoop.conf.Configuration(),
                checkpoint.toString(),
                "",
                Collections.singletonList(table()),
                new SparkSchemaBoundaryCoordinator.QueryFactory() {
                    public org.apache.spark.sql.streaming.StreamingQuery start(
                            List<CatalogTable> catalogs) {
                        boolean old =
                                catalogs.get(0).getTableSchema().equals(table().getTableSchema());
                        calls.add(old ? "start-old" : "start-new");
                        return queryFor(
                                () -> {
                                    if (old) {
                                        publishCommittedDriverBoundary();
                                        return false;
                                    }
                                    return true;
                                },
                                () -> calls.add(old ? "stop-old" : "stop-new"));
                    }

                    public void reconcile(SparkCommittedSourceState boundary) {
                        assertEquals(1, boundary.getEpoch());
                        assertEquals(2, boundary.getSourceCheckpointId());
                        calls.add("reconcile");
                    }
                });
        assertEquals(
                Arrays.asList("start-old", "stop-old", "reconcile", "start-new", "stop-new"),
                calls);
    }

    @Test
    void coordinatorRestoresCommittedCatalogBeforeFirstQueryAnalysis() throws Exception {
        publishCommittedDriverBoundary();
        List<String> calls = new ArrayList<>();
        new SparkSchemaBoundaryCoordinator()
                .run(
                        new org.apache.hadoop.conf.Configuration(),
                        checkpoint.toString(),
                        "",
                        Collections.singletonList(table()),
                        new SparkSchemaBoundaryCoordinator.QueryFactory() {
                            public org.apache.spark.sql.streaming.StreamingQuery start(
                                    List<CatalogTable> catalogs) {
                                assertTrue(
                                        SparkSourceCatalog.sameSchemas(
                                                Collections.singletonList(evolvedTable()),
                                                catalogs));
                                calls.add("start-new");
                                return queryFor(() -> true, () -> calls.add("stop-new"));
                            }

                            public void reconcile(SparkCommittedSourceState boundary) {
                                calls.add("reconcile");
                            }
                        });
        assertEquals(Arrays.asList("reconcile", "start-new", "stop-new"), calls);
    }

    @Test
    void failedSchemaReconciliationCannotStartNewTasks() throws Exception {
        List<String> calls = new ArrayList<>();
        IOException expected = new IOException("physical schema is not reconciled");
        Exception failure =
                assertThrows(
                        Exception.class,
                        () ->
                                new SparkSchemaBoundaryCoordinator()
                                        .run(
                                                new org.apache.hadoop.conf.Configuration(),
                                                checkpoint.toString(),
                                                "",
                                                Collections.singletonList(table()),
                                                new SparkSchemaBoundaryCoordinator.QueryFactory() {
                                                    public org.apache.spark.sql.streaming
                                                                    .StreamingQuery
                                                            start(List<CatalogTable> catalogs) {
                                                        calls.add("start");
                                                        return queryFor(
                                                                () -> {
                                                                    publishCommittedDriverBoundary();
                                                                    return false;
                                                                },
                                                                () -> calls.add("stop"));
                                                    }

                                                    public void reconcile(
                                                            SparkCommittedSourceState boundary)
                                                            throws Exception {
                                                        throw expected;
                                                    }
                                                }));
        assertTrue(failure == expected);
        assertEquals(Arrays.asList("start", "stop"), calls);
    }

    @Test
    void coordinatorPreservesExecutionFailureWhenStoppingAlsoFails() {
        IllegalStateException expected = new IllegalStateException("epoch failed");
        IllegalStateException close = new IllegalStateException("close failed");
        Exception failure =
                assertThrows(
                        Exception.class,
                        () ->
                                new SparkSchemaBoundaryCoordinator()
                                        .run(
                                                new org.apache.hadoop.conf.Configuration(),
                                                checkpoint.toString(),
                                                "",
                                                Collections.singletonList(table()),
                                                new SparkSchemaBoundaryCoordinator.QueryFactory() {
                                                    public org.apache.spark.sql.streaming
                                                                    .StreamingQuery
                                                            start(List<CatalogTable> catalogs) {
                                                        return queryFor(
                                                                () -> {
                                                                    throw expected;
                                                                },
                                                                () -> {
                                                                    throw close;
                                                                });
                                                    }

                                                    public void reconcile(
                                                            SparkCommittedSourceState boundary) {
                                                        throw new AssertionError(
                                                                "No committed boundary");
                                                    }
                                                }));
        assertTrue(failure == expected);
        assertEquals(Collections.singletonList(close), Arrays.asList(failure.getSuppressed()));
    }

    @Test
    void cancelledCoordinatorCannotReconcileOrStartAQuery() throws Exception {
        publishCommittedDriverBoundary();
        SparkSchemaBoundaryCoordinator coordinator = new SparkSchemaBoundaryCoordinator();
        coordinator.requestStop();
        coordinator.run(
                new org.apache.hadoop.conf.Configuration(),
                checkpoint.toString(),
                "",
                Collections.singletonList(table()),
                new SparkSchemaBoundaryCoordinator.QueryFactory() {
                    public org.apache.spark.sql.streaming.StreamingQuery start(
                            List<CatalogTable> catalogs) {
                        throw new AssertionError("Cancelled start");
                    }

                    public void reconcile(SparkCommittedSourceState boundary) {
                        throw new AssertionError("Cancelled physical DDL");
                    }
                });
    }

    @Test
    void cancellationWhileAwaitingBoundaryStopsWithoutPhysicalDdl() throws Exception {
        SparkSchemaBoundaryCoordinator coordinator = new SparkSchemaBoundaryCoordinator();
        List<String> calls = new ArrayList<>();
        coordinator.run(
                new org.apache.hadoop.conf.Configuration(),
                checkpoint.toString(),
                "",
                Collections.singletonList(table()),
                new SparkSchemaBoundaryCoordinator.QueryFactory() {
                    public org.apache.spark.sql.streaming.StreamingQuery start(
                            List<CatalogTable> catalogs) {
                        calls.add("start");
                        return queryFor(
                                () -> {
                                    publishCommittedDriverBoundary();
                                    coordinator.requestStop();
                                    return false;
                                },
                                () -> calls.add("stop"));
                    }

                    public void reconcile(SparkCommittedSourceState boundary) {
                        throw new AssertionError("Cancelled physical DDL");
                    }
                });
        assertEquals(Arrays.asList("start", "stop"), calls);
    }

    @Test
    void identicalExecutionAndStopFailurePreservesTheOriginal() {
        IllegalStateException expected =
                new IllegalStateException("same failure from query and stop");
        Exception failure =
                assertThrows(
                        Exception.class,
                        () ->
                                new SparkSchemaBoundaryCoordinator()
                                        .run(
                                                new org.apache.hadoop.conf.Configuration(),
                                                checkpoint.toString(),
                                                "",
                                                Collections.singletonList(table()),
                                                new SparkSchemaBoundaryCoordinator.QueryFactory() {
                                                    public org.apache.spark.sql.streaming
                                                                    .StreamingQuery
                                                            start(List<CatalogTable> catalogs) {
                                                        return queryFor(
                                                                () -> {
                                                                    throw expected;
                                                                },
                                                                () -> {
                                                                    throw expected;
                                                                });
                                                    }

                                                    public void reconcile(
                                                            SparkCommittedSourceState boundary) {
                                                        throw new AssertionError(
                                                                "No committed boundary");
                                                    }
                                                }));
        assertTrue(failure == expected);
        assertEquals(0, failure.getSuppressed().length);
    }

    public static class ConfiguredCheckpointFileSystem
            extends org.apache.hadoop.fs.RawLocalFileSystem {
        @Override
        public String getScheme() {
            return "configured-checkpoint";
        }

        @Override
        public java.net.URI getUri() {
            return java.net.URI.create("configured-checkpoint:///");
        }
    }

    @Test
    void checkpointReadsAndCommitRetainConfiguredFilesystemSettings() throws Exception {
        org.apache.hadoop.conf.Configuration configuration =
                new org.apache.hadoop.conf.Configuration();
        configuration.set(
                "fs.configured-checkpoint.impl", ConfiguredCheckpointFileSystem.class.getName());
        String root = "configured-checkpoint:///";
        String location = "configured-checkpoint://" + checkpoint.toAbsolutePath();
        TestSource source = new TestSource();
        for (int id = 1; id <= 4; id++) {
            try (SparkMicroBatchReader reader =
                    new SparkMicroBatchReader(
                            source,
                            1,
                            "configured",
                            0,
                            id,
                            100,
                            location,
                            root,
                            "",
                            Collections.emptyMap(),
                            new MultiTableManager(new CatalogTable[] {table()}),
                            configuration)) {
                while (reader.next()) {
                    reader.get();
                }
            }
        }
        try (SparkMicroBatchReader acknowledgment =
                new SparkMicroBatchReader(
                        source,
                        1,
                        "configured",
                        0,
                        4,
                        100,
                        location,
                        root,
                        "",
                        Collections.emptyMap(),
                        new MultiTableManager(new CatalogTable[] {table()}),
                        configuration)) {
            acknowledgment.acknowledge();
        }
        SparkMicroBatchReader.commit(location, 4, root, "", configuration);
        assertFalse(java.nio.file.Files.exists(checkpoint.resolve("seatunnel-batches/0/1")));
        assertTrue(java.nio.file.Files.exists(checkpoint.resolve("seatunnel-batches/0/3")));
        assertTrue(java.nio.file.Files.exists(checkpoint.resolve("seatunnel-batches/0/4")));
        assertFalse(source.acknowledged.isEmpty());
    }

    @Test
    void restartReclaimsPreviousApplicationStagingAndRetainsCurrentAttempts() throws Exception {
        Path root = checkpoint.resolve("seatunnel-batches/0");
        java.nio.file.Files.createDirectories(root);
        String current =
                java.util
                        .UUID
                        .nameUUIDFromBytes(
                                "test-job".getBytes(java.nio.charset.StandardCharsets.UTF_8))
                        .toString();
        String previous =
                java.util
                        .UUID
                        .nameUUIDFromBytes(
                                "previous-job".getBytes(java.nio.charset.StandardCharsets.UTF_8))
                        .toString();
        Path currentAttempt = root.resolve(".1-" + current + "-" + java.util.UUID.randomUUID());
        Path abandoned = root.resolve(".1-" + previous + "-" + java.util.UUID.randomUUID());
        Path retired = root.resolve(".retired-" + java.util.UUID.randomUUID());
        for (Path directory : Arrays.asList(currentAttempt, abandoned, retired)) {
            java.nio.file.Files.createDirectories(directory);
            java.nio.file.Files.write(directory.resolve("rows"), new byte[64 * 1024]);
        }
        assertEquals(Arrays.asList(1, 2, 3), read(new TestSource(), 1, 1));
        assertTrue(java.nio.file.Files.exists(currentAttempt.resolve("rows")));
        assertFalse(java.nio.file.Files.exists(abandoned));
        assertFalse(java.nio.file.Files.exists(retired));
        assertTrue(java.nio.file.Files.exists(root.resolve("1/rows")));
    }

    @Test
    void newDriverGenerationFencesOldStageEvenWithSameApplicationId() throws Exception {
        TestSource oldSource = new TestSource();
        oldSource.beforeSnapshot =
                () -> {
                    TestSource replacement = new TestSource();
                    replacement.limit = 6;
                    try (SparkMicroBatchReader reader =
                            new SparkMicroBatchReader(
                                    replacement,
                                    1,
                                    "test-job",
                                    0,
                                    1,
                                    100,
                                    checkpoint.toUri().toString(),
                                    "file:///",
                                    "",
                                    Collections.emptyMap(),
                                    new MultiTableManager(new CatalogTable[] {table()}),
                                    new org.apache.hadoop.conf.Configuration(),
                                    "new-driver-generation")) {
                        while (reader.next()) {
                            reader.get();
                        }
                    } catch (Exception error) {
                        throw new RuntimeException(error);
                    }
                };
        assertThrows(IOException.class, () -> read(oldSource, 1, 1));
        TestSource replay = new TestSource();
        replay.failOpen = true;
        assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6), read(replay, 1, 1));
        assertTrue(oldSource.acknowledged.isEmpty());
    }

    @Test
    void reclaimedAttemptCannotPublishRecreatedMetadataDirectory() throws Exception {
        TestSource source = new TestSource();
        source.beforeSnapshot =
                () -> {
                    try (java.util.stream.Stream<Path> entries =
                            java.nio.file.Files.list(checkpoint.resolve("seatunnel-batches/0"))) {
                        Path attempt =
                                entries.filter(p -> p.getFileName().toString().startsWith("."))
                                        .findFirst()
                                        .get();
                        Path retired =
                                attempt.resolveSibling(".retired-" + java.util.UUID.randomUUID());
                        java.nio.file.Files.move(
                                attempt, retired, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
                        java.nio.file.Files.createDirectories(attempt);
                    } catch (IOException error) {
                        throw new java.io.UncheckedIOException(error);
                    }
                };
        assertThrows(IOException.class, () -> read(source, 1, 1));
        assertFalse(java.nio.file.Files.exists(checkpoint.resolve("seatunnel-batches/0/1")));
        assertTrue(source.acknowledged.isEmpty());
    }

    @Test
    void parallelRetryReplaysRowsAndSuccessorRestoresState() throws Exception {
        retry(false, 1);
    }

    @Test
    void coordinatedRetryRestoresEveryReader() throws Exception {
        retry(true, 2);
    }

    private void retry(boolean coordinated, int parallelism) throws Exception {
        TestSource source = coordinated ? new CoordinatedTestSource() : new TestSource();
        List<Integer> first = read(source, parallelism, 1);
        assertEquals(parallelism * 3, first.size());
        assertTrue(source.acknowledged.isEmpty(), "staging must not acknowledge reader offsets");
        source.limit = 6;
        source.failOpen = true;
        assertEquals(
                first, read(source, parallelism, 1), "retry must replay without reopening source");
        source.failOpen = false;
        List<Integer> next = read(source, parallelism, 2);
        assertEquals(parallelism * 3, next.size());
        for (int value : next) {
            assertTrue(value % 100 >= 4);
        }
        assertTrue(
                source.acknowledged.isEmpty(), "reading a successor must not acknowledge offsets");
        try (SparkMicroBatchReader committed = reader(source, parallelism, 1)) {
            committed.acknowledge();
        }
        assertEquals(parallelism, source.acknowledged.size());
        assertTrue(source.acknowledged.stream().allMatch(id -> id == 1L));
    }

    @Test
    void failedReaderCannotPublishACompletedInterval() throws Exception {
        TestSource source = new CoordinatedTestSource();
        source.failPoll = true;
        assertThrows(IOException.class, () -> read(source, 2, 1));
        assertFalse(java.nio.file.Files.exists(checkpoint.resolve("seatunnel-batches/0/1")));
        source.failPoll = false;
        assertEquals(6, read(source, 2, 1).size());
    }

    @Test
    void readerFailureCannotBeHiddenByAnEnumeratorWaitingForClose() throws Exception {
        TestSource source =
                new CoordinatedTestSource() {
                    @Override
                    public SourceSplitEnumerator<TestSplit, Integer> createEnumerator(
                            SourceSplitEnumerator.Context<TestSplit> context) {
                        java.util.concurrent.CountDownLatch closed =
                                new java.util.concurrent.CountDownLatch(1);
                        return new SourceSplitEnumerator<TestSplit, Integer>() {
                            @Override
                            public void open() {}

                            @Override
                            public void run() throws Exception {
                                closed.await();
                            }

                            @Override
                            public void close() {
                                closed.countDown();
                            }

                            @Override
                            public void addSplitsBack(List<TestSplit> splits, int id) {
                                context.assignSplit(id, splits);
                            }

                            @Override
                            public int currentUnassignedSplitSize() {
                                return 0;
                            }

                            @Override
                            public void handleSplitRequest(int id) {}

                            @Override
                            public void registerReader(int id) {
                                context.assignSplit(id, new TestSplit(0));
                            }

                            @Override
                            public Integer snapshotState(long id) {
                                return 0;
                            }

                            @Override
                            public void notifyCheckpointComplete(long id) {}
                        };
                    }
                };
        source.failPoll = true;
        assertThrows(IOException.class, () -> read(source, 2, 1));
        assertFalse(java.nio.file.Files.exists(checkpoint.resolve("seatunnel-batches/0/1")));
    }

    @Test
    void allReadersAndEnumeratorCloseEvenWhenOneCloseFails() throws Exception {
        TestSource source = new CoordinatedTestSource();
        source.failClose = true;
        assertThrows(IOException.class, () -> read(source, 2, 1));
        assertEquals(2, source.closedReaders.get());
        assertEquals(1, source.closedEnumerators.get());
        assertFalse(java.nio.file.Files.exists(checkpoint.resolve("seatunnel-batches/0/1")));
    }

    @Test
    void pollFailureRemainsThePrimaryFailureWhenCloseAlsoFails() throws Exception {
        TestSource source = new CoordinatedTestSource();
        source.failPoll = true;
        source.failClose = true;
        IOException failure = assertThrows(IOException.class, () -> read(source, 2, 1));
        assertTrue(failure.getMessage().contains("interval failed"));
        assertTrue(failure.getSuppressed().length > 0);
        assertEquals(2, source.closedReaders.get());
        assertEquals(1, source.closedEnumerators.get());
    }

    @Test
    void diskWriteFailureCannotDeadlockAFullHandoverOrPublishState() throws Exception {
        TestSource source = new TestSource();
        source.fast = true;
        source.limit = 50000;
        org.junit.jupiter.api.Assertions.assertTimeoutPreemptively(
                java.time.Duration.ofSeconds(10),
                () -> {
                    try (SparkMicroBatchReader reader =
                            new SparkMicroBatchReader(
                                    source,
                                    1,
                                    "disk-failure",
                                    0,
                                    1,
                                    150,
                                    "checkpoint-failure://" + checkpoint.toAbsolutePath(),
                                    "checkpoint-failure:///",
                                    "",
                                    Collections.emptyMap(),
                                    new MultiTableManager(new CatalogTable[] {table()}))) {
                        assertThrows(IOException.class, reader::next);
                    }
                });
        assertEquals(1, source.closedReaders.get());
        assertTrue(source.acknowledged.isEmpty());
        assertFalse(java.nio.file.Files.exists(checkpoint.resolve("seatunnel-batches/0/1")));
    }

    @Test
    void missingPredecessorFailsInsteadOfStartingFromScratch() throws Exception {
        assertThrows(IOException.class, () -> read(new TestSource(), 1, 2));
    }

    @Test
    void differentParallelismCannotSilentlyReuseState() throws Exception {
        read(new CoordinatedTestSource(), 2, 1);
        assertThrows(IOException.class, () -> read(new CoordinatedTestSource(), 1, 1));
    }

    @Test
    void committedIntervalsArePrunedWithoutLosingRestoreState() throws Exception {
        TestSource source = new TestSource();
        read(source, 1, 1);
        source.limit = 6;
        read(source, 1, 2);
        source.limit = 9;
        read(source, 1, 3);
        SparkMicroBatchReader.commit(checkpoint.toUri().toString(), 3, "file:///", "");
        assertFalse(java.nio.file.Files.exists(checkpoint.resolve("seatunnel-batches/0/1")));
        assertTrue(java.nio.file.Files.exists(checkpoint.resolve("seatunnel-batches/0/2/state")));
        assertTrue(java.nio.file.Files.exists(checkpoint.resolve("seatunnel-batches/0/3/state")));
        source.limit = 12;
        assertEquals(Arrays.asList(10, 11, 12), read(source, 1, 4));
    }

    @Test
    void speculativeAttemptsBothReplayTheSinglePublishedWinner() throws Exception {
        TestSource first = new TestSource();
        TestSource second = new TestSource();
        second.limit = 6;
        java.util.concurrent.ExecutorService attempts =
                java.util.concurrent.Executors.newFixedThreadPool(2);
        try {
            java.util.concurrent.Future<List<Integer>> a = attempts.submit(() -> read(first, 1, 1));
            java.util.concurrent.Future<List<Integer>> b =
                    attempts.submit(() -> read(second, 1, 1));
            List<Integer> rows = a.get(10, java.util.concurrent.TimeUnit.SECONDS);
            assertEquals(rows, b.get(10, java.util.concurrent.TimeUnit.SECONDS));
            assertTrue(rows.size() == 3 || rows.size() == 6);
            try (java.util.stream.Stream<Path> entries =
                    java.nio.file.Files.list(checkpoint.resolve("seatunnel-batches/0/1"))) {
                assertFalse(
                        entries.anyMatch(java.nio.file.Files::isDirectory),
                        "losing attempts must not nest inside the winner");
            }
        } finally {
            attempts.shutdownNow();
        }
    }

    @Test
    void cancellationReachesReaderBlockedInOpen() throws Exception {
        java.util.concurrent.CountDownLatch opening = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch released = new java.util.concurrent.CountDownLatch(1);
        TestSource source =
                new TestSource() {
                    @Override
                    public SourceReader<SeaTunnelRow, TestSplit> createReader(
                            SourceReader.Context context) {
                        SourceReader<SeaTunnelRow, TestSplit> delegate =
                                super.createReader(context);
                        return new SourceReader<SeaTunnelRow, TestSplit>() {
                            @Override
                            public void open() throws Exception {
                                opening.countDown();
                                released.await();
                            }

                            @Override
                            public void cancel() {
                                released.countDown();
                            }

                            @Override
                            public void close() throws IOException {
                                delegate.close();
                            }

                            @Override
                            public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
                                delegate.pollNext(output);
                            }

                            @Override
                            public List<TestSplit> snapshotState(long id) throws Exception {
                                return delegate.snapshotState(id);
                            }

                            @Override
                            public void addSplits(List<TestSplit> splits) {
                                delegate.addSplits(splits);
                            }

                            @Override
                            public void handleNoMoreSplits() {}

                            @Override
                            public void notifyCheckpointComplete(long id) {}
                        };
                    }
                };
        SparkMicroBatchReader reader = reader(source, 1, 1);
        java.util.concurrent.ExecutorService task =
                java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            java.util.concurrent.Future<?> result =
                    task.submit(() -> assertThrows(IOException.class, reader::next));
            assertTrue(opening.await(5, java.util.concurrent.TimeUnit.SECONDS));
            reader.close();
            result.get(5, java.util.concurrent.TimeUnit.SECONDS);
            assertEquals(1, source.closedReaders.get());
            assertFalse(java.nio.file.Files.exists(checkpoint.resolve("seatunnel-batches/0/1")));
        } finally {
            released.countDown();
            task.shutdownNow();
            reader.close();
        }
    }

    @Test
    void checkpointCompletionRetriesNotifyOnlySavedStateWithoutPolling() throws Exception {
        TestSource source = new TestSource();
        read(source, 1, 1);
        source.failPoll = true;
        try (SparkMicroBatchReader committed = reader(source, 1, 1)) {
            committed.acknowledge();
        }
        try (SparkMicroBatchReader committed = reader(source, 1, 1)) {
            committed.acknowledge();
        }
        assertEquals(Arrays.asList(1L, 1L), source.acknowledged);
    }

    @Test
    void commitCancellationRetainsSparkRecognizedInterruption() {
        InterruptedException interrupted = new InterruptedException("cancel");
        try {
            RuntimeException failure = SparkMicroBatchReader.checkpointFailure(interrupted);
            assertTrue(Thread.currentThread().isInterrupted());
            assertTrue(
                    org.apache.spark.sql.execution.streaming.StreamExecution
                            .isInterruptionException(failure, null));
            assertEquals(interrupted, failure.getCause().getCause());
        } finally {
            Thread.interrupted();
        }
        assertFalse(
                org.apache.spark.sql.execution.streaming.StreamExecution.isInterruptionException(
                        SparkMicroBatchReader.checkpointFailure(new IOException("disk failure")),
                        null));
    }

    @Test
    void socketTimeoutRemainsAFailureAndDoesNotSetInterruptFlag() {
        RuntimeException failure =
                SparkMicroBatchReader.checkpointFailure(
                        new IOException(
                                "checkpoint IO", new java.net.SocketTimeoutException("timeout")));
        assertFalse(Thread.currentThread().isInterrupted());
        assertTrue(failure instanceof IllegalStateException);
        assertFalse(
                org.apache.spark.sql.execution.streaming.StreamExecution.isInterruptionException(
                        failure, null));
    }

    @Test
    void localPublicationCannotCopyAnAttemptInsideAnExistingWinner() throws Exception {
        Path attempt = java.nio.file.Files.createDirectory(checkpoint.resolve(".1-attempt"));
        Path completed = java.nio.file.Files.createDirectory(checkpoint.resolve("1"));
        java.nio.file.Files.write(
                attempt.resolve("rows"),
                "loser rows".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        java.nio.file.Files.write(
                completed.resolve("rows"),
                "winner rows".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        java.nio.file.Files.write(
                completed.resolve("state"),
                "winner state".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertThrows(
                IOException.class, () -> SparkMicroBatchReader.publishLocal(attempt, completed));
        assertEquals(
                "winner rows",
                new String(
                        java.nio.file.Files.readAllBytes(completed.resolve("rows")),
                        java.nio.charset.StandardCharsets.UTF_8));
        assertEquals(
                "winner state",
                new String(
                        java.nio.file.Files.readAllBytes(completed.resolve("state")),
                        java.nio.charset.StandardCharsets.UTF_8));
        assertFalse(java.nio.file.Files.exists(completed.resolve(attempt.getFileName())));
        assertTrue(java.nio.file.Files.exists(attempt.resolve("rows")));
    }

    @Test
    void closeBeforeInitializeIsSafe() throws Exception {
        reader(new TestSource(), 1, 1).close();
    }

    @Test
    void cancelledReaderCannotReopenAndPublish() throws Exception {
        SparkMicroBatchReader reader = reader(new TestSource(), 1, 1);
        reader.close();
        assertThrows(IOException.class, reader::next);
    }

    private List<Integer> read(TestSource source, int parallelism, int id) throws Exception {
        List<Integer> values = new ArrayList<>();
        try (SparkMicroBatchReader reader = reader(source, parallelism, id)) {
            while (reader.next()) {
                values.add(reader.get().getInt(2));
            }
        }
        Collections.sort(values);
        return values;
    }

    private SparkMicroBatchReader reader(TestSource source, int parallelism, int id) {
        return new SparkMicroBatchReader(
                source,
                parallelism,
                "test-job",
                0,
                id,
                2000,
                checkpoint.toUri().toString(),
                "file:///",
                "",
                Collections.emptyMap(),
                new MultiTableManager(new CatalogTable[] {table()}));
    }

    @Test
    void schemaWatermarkSealsBeforeDdlOffsetIsAccepted() throws Exception {
        SchemaSource source = new SchemaSource();
        assertEquals(Collections.singletonList(1), read(source, 1, 1));
        assertEquals(1, source.lastSnapshot);
        assertTrue(source.acknowledged.isEmpty());
    }

    @Test
    void schemaTransitionJournalsDdlAndResumesOnlyWithReanalyzedRows() throws Exception {
        SchemaSource source = new SchemaSource();
        assertEquals(Collections.singletonList(1), read(source, 1, 1));
        assertEquals(SparkSourceCheckpoint.Phase.BEFORE_SCHEMA_CHANGE, metadata(1).getPhase());
        assertTrue(read(source, 1, 2).isEmpty());
        SparkSourceCheckpoint transition = metadata(2);
        assertEquals(SparkSourceCheckpoint.Phase.AFTER_SCHEMA_CHANGE, transition.getPhase());
        assertEquals(1, transition.getEvents().size());
        assertEquals(2, source.lastSnapshot);
        assertEquals(TYPE, transition.getBeforeTables().get(0).getSeaTunnelRowType());
        assertEquals(
                evolvedTable().getSeaTunnelRowType(),
                transition.getTables().get(0).getSeaTunnelRowType());
        assertTrue(read(source, 1, 2).isEmpty());
        assertEquals(1, metadata(2).getEvents().size());
        org.apache.hadoop.conf.Configuration configuration =
                new org.apache.hadoop.conf.Configuration();
        assertTrue(
                SparkSourceCheckpoint.shouldPause(
                        configuration,
                        checkpoint.toUri().toString(),
                        2,
                        "",
                        new MultiTableManager(new CatalogTable[] {table()})));
        assertFalse(
                SparkSourceCheckpoint.shouldPause(
                        configuration,
                        checkpoint.toUri().toString(),
                        2,
                        "",
                        new MultiTableManager(new CatalogTable[] {evolvedTable()})));
        assertFalse(
                SparkSourceCheckpoint.shouldPause(
                        configuration,
                        checkpoint.toUri().toString(),
                        0,
                        "",
                        new MultiTableManager(new CatalogTable[] {table()})));
        assertThrows(IOException.class, () -> read(source, 1, 3));
        assertFalse(java.nio.file.Files.exists(checkpoint.resolve("seatunnel-batches/0/3")));
        source.restoreEvent = true;
        try (SparkMicroBatchReader reader = schemaReader(source, 3)) {
            assertTrue(reader.next());
            assertEquals(4, reader.get().numFields());
            assertEquals(3, reader.get().getInt(2));
            assertEquals("restored", reader.get().getUTF8String(3).toString());
            assertFalse(reader.next());
        }
        assertEquals(SparkSourceCheckpoint.Phase.DATA, metadata(3).getPhase());
        assertTrue(metadata(3).getEvents().isEmpty());
        assertTrue(source.acknowledged.isEmpty());
    }

    @Test
    void incompleteSchemaBlockIsNotPublishedAndCanRetryFromBeforeBoundary() throws Exception {
        SchemaSource source = new SchemaSource();
        assertEquals(Collections.singletonList(1), read(source, 1, 1));
        source.skipAfterWatermark = true;
        assertThrows(IOException.class, () -> read(source, 1, 2));
        assertFalse(java.nio.file.Files.exists(checkpoint.resolve("seatunnel-batches/0/2")));
        source.skipAfterWatermark = false;
        assertTrue(read(source, 1, 2).isEmpty());
        assertEquals(1, metadata(2).getEvents().size());
    }

    @Test
    void failedSchemaBoundarySnapshotCannotFallBackToAnAdvancedSnapshot() throws Exception {
        SchemaSource source = new SchemaSource();
        source.failBoundarySnapshot = true;
        IOException failure = assertThrows(IOException.class, () -> read(source, 1, 1));
        assertTrue(failure.toString().contains("source") || failure.getCause() != null);
        assertEquals(1, source.snapshotCalls.get());
        assertFalse(java.nio.file.Files.exists(checkpoint.resolve("seatunnel-batches/0/1")));
        source.failBoundarySnapshot = false;
        assertEquals(Collections.singletonList(1), read(source, 1, 1));
    }

    @Test
    void allInitialAssignmentsFinishBeforeASchemaReaderCanPoll() throws Exception {
        java.util.concurrent.CountDownLatch polled = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.atomic.AtomicBoolean assigning =
                new java.util.concurrent.atomic.AtomicBoolean();
        java.util.concurrent.atomic.AtomicBoolean earlyPoll =
                new java.util.concurrent.atomic.AtomicBoolean();
        SchemaSource source =
                new SchemaSource() {
                    @Override
                    public SourceReader<SeaTunnelRow, TestSplit> createReader(
                            SourceReader.Context context) {
                        SourceReader<SeaTunnelRow, TestSplit> delegate =
                                super.createReader(context);
                        return new SourceReader<SeaTunnelRow, TestSplit>() {
                            public void open() throws Exception {
                                delegate.open();
                            }

                            public void close() throws IOException {
                                delegate.close();
                            }

                            public void cancel() {
                                delegate.cancel();
                            }

                            public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
                                if (assigning.get()) earlyPoll.set(true);
                                polled.countDown();
                                delegate.pollNext(output);
                            }

                            public List<TestSplit> snapshotState(long id) throws Exception {
                                return delegate.snapshotState(id);
                            }

                            public void addSplits(List<TestSplit> splits) {
                                delegate.addSplits(splits);
                            }

                            public void handleNoMoreSplits() {
                                delegate.handleNoMoreSplits();
                            }

                            public void notifyCheckpointComplete(long id) throws Exception {
                                delegate.notifyCheckpointComplete(id);
                            }
                        };
                    }

                    @Override
                    public SourceSplitEnumerator<TestSplit, Integer> createEnumerator(
                            SourceSplitEnumerator.Context<TestSplit> context) {
                        return new SourceSplitEnumerator<TestSplit, Integer>() {
                            public void open() {}

                            public void close() {}

                            public void run() throws Exception {
                                assigning.set(true);
                                context.assignSplit(0, new TestSplit(0));
                                polled.await(150, java.util.concurrent.TimeUnit.MILLISECONDS);
                                assigning.set(false);
                            }

                            public void addSplitsBack(List<TestSplit> splits, int id) {
                                context.assignSplit(id, splits);
                            }

                            public int currentUnassignedSplitSize() {
                                return 0;
                            }

                            public void handleSplitRequest(int id) {}

                            public void registerReader(int id) {}

                            public Integer snapshotState(long id) {
                                return 0;
                            }

                            public void notifyCheckpointComplete(long id) {}
                        };
                    }
                };
        assertEquals(Collections.singletonList(1), read(source, 1, 1));
        assertFalse(earlyPoll.get());
    }

    @Test
    void requestOnlyEnumeratorAssignsEveryReaderBeforeSchemaPollingStarts() throws Exception {
        java.util.Set<Integer> requested = java.util.concurrent.ConcurrentHashMap.newKeySet();
        java.util.concurrent.atomic.AtomicBoolean early =
                new java.util.concurrent.atomic.AtomicBoolean();
        SchemaSource source =
                new SchemaSource() {
                    @Override
                    public SourceSplitEnumerator<TestSplit, Integer> createEnumerator(
                            SourceSplitEnumerator.Context<TestSplit> context) {
                        return new SourceSplitEnumerator<TestSplit, Integer>() {
                            public void open() {}

                            public void close() {}

                            public void registerReader(int id) {}

                            public void handleSplitRequest(int id) {
                                requested.add(id);
                            }

                            public void run() {
                                for (int id : requested) context.assignSplit(id, new TestSplit(0));
                            }

                            public void addSplitsBack(List<TestSplit> splits, int id) {
                                context.assignSplit(id, splits);
                            }

                            public int currentUnassignedSplitSize() {
                                return 0;
                            }

                            public Integer snapshotState(long id) {
                                return 0;
                            }

                            public void notifyCheckpointComplete(long id) {}
                        };
                    }
                };
        source.onPoll =
                () -> {
                    if (requested.size() != 2) early.set(true);
                };
        assertEquals(Collections.singletonList(1), read(source, 2, 1));
        assertFalse(early.get());
        java.util.Map<Integer, List<byte[]>> state = metadata(1).getState();
        for (int id = 0; id < 2; id++) {
            assertEquals(1, state.get(id).size());
            TestSplit restored =
                    org.apache.seatunnel.common.utils.SerializationUtils.deserialize(
                            state.get(id).get(0));
            assertTrue(restored.next >= 0);
        }
    }

    @Test
    void legacyJournalWithoutCatalogMetadataRetainsItsSchemaValidation() throws Exception {
        java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        try (java.io.DataOutputStream output = new java.io.DataOutputStream(bytes)) {
            output.writeInt(1);
            output.writeInt(1);
            output.writeUTF(
                    new MultiTableManager(new CatalogTable[] {table()}).getTableSchema().json());
            byte[] state =
                    org.apache.seatunnel.common.utils.SerializationUtils.serialize(
                            new java.util.HashMap<Integer, List<byte[]>>());
            output.writeInt(state.length);
            output.write(state);
        }
        SparkSourceCheckpoint legacy =
                SparkSourceCheckpoint.read(
                        new java.io.DataInputStream(
                                new java.io.ByteArrayInputStream(bytes.toByteArray())));
        assertEquals(SparkSourceCheckpoint.Phase.DATA, legacy.getPhase());
        assertTrue(legacy.getTables().isEmpty());
        legacy.validate(
                1,
                new MultiTableManager(new CatalogTable[] {table()}).getTableSchema().json(),
                true);
        assertThrows(
                IOException.class,
                () ->
                        legacy.validate(
                                1,
                                new MultiTableManager(new CatalogTable[] {evolvedTable()})
                                        .getTableSchema()
                                        .json(),
                                true));
    }

    @Test
    void identicalMergedShapeCannotHideUnrestoredMultiTableLayouts() throws Exception {
        CatalogTable unchanged =
                CatalogTableUtil.getCatalogTable("test", "db", "schema", "other", TYPE);
        CatalogTable renamed =
                CatalogTableUtil.getCatalogTable(
                        "test",
                        "db",
                        "schema",
                        "records",
                        new SeaTunnelRowType(
                                new String[] {"renamed"},
                                new SeaTunnelDataType[] {BasicType.INT_TYPE}));
        MultiTableManager oldManager =
                new MultiTableManager(new CatalogTable[] {table(), unchanged});
        MultiTableManager newManager =
                new MultiTableManager(new CatalogTable[] {renamed, unchanged});
        assertEquals(oldManager.getTableSchema(), newManager.getTableSchema());
        SparkSourceCheckpoint transition =
                new SparkSourceCheckpoint(
                        1,
                        oldManager.getTableSchema().json(),
                        Collections.emptyMap(),
                        Arrays.asList(table(), unchanged),
                        Arrays.asList(renamed, unchanged),
                        Collections.emptyList(),
                        SparkSourceCheckpoint.Phase.AFTER_SCHEMA_CHANGE);
        assertThrows(IOException.class, () -> transition.validate(1, oldManager, false));
        transition.validate(1, newManager, false);
    }

    @Test
    void acknowledgementChecksCatalogsAndAllowsOnlyEitherSideOfACompleteBoundary()
            throws Exception {
        MultiTableManager before = new MultiTableManager(new CatalogTable[] {table()});
        MultiTableManager after = new MultiTableManager(new CatalogTable[] {evolvedTable()});
        SparkSourceCheckpoint data =
                new SparkSourceCheckpoint(
                        1,
                        before.getTableSchema().json(),
                        Collections.emptyMap(),
                        Collections.singletonList(table()),
                        Collections.singletonList(table()),
                        Collections.emptyList(),
                        SparkSourceCheckpoint.Phase.DATA);
        assertThrows(IOException.class, () -> data.validateAcknowledgement(1, after));
        SparkSourceCheckpoint boundary =
                new SparkSourceCheckpoint(
                        1,
                        before.getTableSchema().json(),
                        Collections.emptyMap(),
                        Collections.singletonList(table()),
                        Collections.singletonList(evolvedTable()),
                        Collections.emptyList(),
                        SparkSourceCheckpoint.Phase.AFTER_SCHEMA_CHANGE);
        boundary.validateAcknowledgement(1, before);
        boundary.validateAcknowledgement(1, after);
        assertThrows(IOException.class, () -> boundary.validateAcknowledgement(2, after));
        CatalogTable other =
                CatalogTableUtil.getCatalogTable("test", "db", "schema", "other", TYPE);
        assertThrows(
                IOException.class,
                () ->
                        boundary.validateAcknowledgement(
                                1, new MultiTableManager(new CatalogTable[] {other})));
    }

    @Test
    void legacyProducedTypeSourceKeepsWorkingWithoutCatalogSpi() throws Exception {
        TestSource source =
                new TestSource() {
                    @Override
                    public List<CatalogTable> getProducedCatalogTables() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public org.apache.seatunnel.api.table.type.SeaTunnelDataType getProducedType() {
                        return TYPE;
                    }
                };
        assertEquals(Arrays.asList(1, 2, 3), read(source, 1, 1));
    }

    @Test
    void localRestoreEventRefreshesCollectorWithoutPublishingDdl() throws Exception {
        TestSource source = collectorSource(Collections.singletonList(table()), true);
        assertEquals(Collections.singletonList(42), read(source, 1, 1));
        assertEquals(SparkSourceCheckpoint.Phase.DATA, metadata(1).getPhase());
        assertTrue(metadata(1).getEvents().isEmpty());
        assertEquals(TYPE, metadata(1).getTables().get(0).getSeaTunnelRowType());
        assertEquals(1, source.closedReaders.get());
    }

    @Test
    void emptyLegacyCollectorMetadataKeepsAnalyzedConverter() throws Exception {
        TestSource source = collectorSource(Collections.emptyList(), false);
        assertEquals(Collections.singletonList(42), read(source, 1, 1));
        assertTrue(metadata(1).getEvents().isEmpty());
        assertEquals(TYPE, metadata(1).getTables().get(0).getSeaTunnelRowType());
        assertEquals(1, source.closedReaders.get());
    }

    @Test
    void restoredCollectorRejectsChangedAnalyzedShapeBeforePublishingRows() throws Exception {
        TestSource source = collectorSource(Collections.singletonList(evolvedTable()), false);
        assertThrows(IOException.class, () -> read(source, 1, 1));
        assertFalse(java.nio.file.Files.exists(checkpoint.resolve("seatunnel-batches/0/1")));
        assertEquals(1, source.closedReaders.get());
    }

    @Test
    void restoredCollectorRejectsSameMergedShapeWithDifferentTableLayout() throws Exception {
        CatalogTable renamed =
                CatalogTableUtil.getCatalogTable(
                        "test",
                        "db",
                        "schema",
                        "records",
                        new SeaTunnelRowType(
                                new String[] {"renamed"},
                                new SeaTunnelDataType[] {BasicType.INT_TYPE}));
        CatalogTable other =
                CatalogTableUtil.getCatalogTable("test", "db", "schema", "other", TYPE);
        MultiTableManager analyzed = new MultiTableManager(new CatalogTable[] {table(), other});
        MultiTableManager restored = new MultiTableManager(new CatalogTable[] {renamed, other});
        assertEquals(analyzed.getTableSchema(), restored.getTableSchema());
        TestSource source = collectorSource(Arrays.asList(renamed, other), false);
        try (SparkMicroBatchReader reader =
                new SparkMicroBatchReader(
                        source,
                        1,
                        "test-job",
                        0,
                        1,
                        2000,
                        checkpoint.toUri().toString(),
                        "file:///",
                        "",
                        Collections.emptyMap(),
                        analyzed)) {
            assertThrows(IOException.class, reader::next);
        }
        assertFalse(java.nio.file.Files.exists(checkpoint.resolve("seatunnel-batches/0/1")));
        assertEquals(1, source.closedReaders.get());
    }

    private TestSource collectorSource(List<CatalogTable> restoredTables, boolean restoreEvent) {
        return new TestSource() {
            @Override
            public SourceReader<SeaTunnelRow, TestSplit> createReader(
                    SourceReader.Context context) {
                return new SourceReader<SeaTunnelRow, TestSplit>() {
                    boolean emitted;

                    public void open() {}

                    public void close() {
                        closedReaders.incrementAndGet();
                    }

                    public void addSplits(List<TestSplit> splits) {}

                    public void handleNoMoreSplits() {}

                    public void notifyCheckpointComplete(long id) {
                        acknowledged.add(id);
                    }

                    public List<TestSplit> snapshotState(long id) {
                        return Collections.singletonList(new TestSplit(emitted ? 1 : 0));
                    }

                    public void pollNext(Collector<SeaTunnelRow> output) {
                        synchronized (output.getCheckpointLock()) {
                            if (!emitted) {
                                output.restoreSchema(restoredTables);
                                if (restoreEvent) {
                                    output.collect(
                                            new org.apache.seatunnel.api.table.schema.event
                                                    .RestoreTableSchemaEvent(table()));
                                }
                                output.collect(new SeaTunnelRow(new Object[] {42}));
                                emitted = true;
                            }
                        }
                        context.signalNoMoreElement();
                    }
                };
            }
        };
    }

    private SparkSourceCheckpoint metadata(int id) throws Exception {
        try (java.io.DataInputStream input =
                new java.io.DataInputStream(
                        java.nio.file.Files.newInputStream(
                                checkpoint.resolve("seatunnel-batches/0/" + id + "/state")))) {
            return SparkSourceCheckpoint.read(input);
        }
    }

    private SparkMicroBatchReader schemaReader(SchemaSource source, int id) {
        return new SparkMicroBatchReader(
                source,
                1,
                "test-job",
                0,
                id,
                2000,
                checkpoint.toUri().toString(),
                "file:///",
                "",
                Collections.emptyMap(),
                new MultiTableManager(new CatalogTable[] {evolvedTable()}));
    }

    static CatalogTable evolvedTable() {
        return CatalogTableUtil.getCatalogTable(
                "test",
                "db",
                "schema",
                "records",
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE}));
    }

    static class SchemaSource extends CoordinatedTestSource
            implements org.apache.seatunnel.api.source.SupportSchemaChangeWatermarks {
        boolean restoreCheckpointSchema = true;

        @Override
        public boolean restoreCheckpointSchema() {
            return restoreCheckpointSchema;
        }

        volatile int lastSnapshot;
        volatile boolean skipAfterWatermark;
        volatile boolean restoreEvent;
        volatile boolean failBoundarySnapshot;
        Runnable onPoll = () -> {};
        final java.util.concurrent.atomic.AtomicInteger snapshotCalls =
                new java.util.concurrent.atomic.AtomicInteger();

        @Override
        public SourceReader<SeaTunnelRow, TestSplit> createReader(SourceReader.Context context) {
            return new SourceReader<SeaTunnelRow, TestSplit>() {
                int next = -1;

                public void open() {}

                public void close() {}

                public void addSplits(List<TestSplit> splits) {
                    next = splits.get(0).next;
                }

                public void handleNoMoreSplits() {}

                public void notifyCheckpointComplete(long id) {
                    acknowledged.add(id);
                }

                public List<TestSplit> snapshotState(long id) {
                    snapshotCalls.incrementAndGet();
                    if (failBoundarySnapshot && next == 1)
                        throw new IllegalStateException("injected boundary snapshot failure");
                    lastSnapshot = next;
                    return Collections.singletonList(new TestSplit(next));
                }

                public void pollNext(Collector<SeaTunnelRow> output) {
                    onPoll.run();
                    synchronized (output.getCheckpointLock()) {
                        if (next < 0) return;
                        output.restoreSchema(
                                Collections.singletonList(next < 2 ? table() : evolvedTable()));
                        if (restoreEvent && next >= 2)
                            output.collect(
                                    new org.apache.seatunnel.api.table.schema.event
                                            .RestoreTableSchemaEvent(evolvedTable()));
                        if (next == 0) {
                            next = 1;
                            output.collect(new SeaTunnelRow(new Object[] {1}));
                        }
                        if (next == 1) {
                            output.markSchemaChangeBeforeCheckpoint();
                            next = 2;
                            org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent
                                    event =
                                            org.apache.seatunnel.api.table.schema.event
                                                    .AlterTableAddColumnEvent.add(
                                                    table().getTableId(),
                                                    evolvedTable()
                                                            .getTableSchema()
                                                            .getColumns()
                                                            .get(1));
                            event.setChangeAfter(evolvedTable());
                            output.collect(event);
                            if (!skipAfterWatermark) output.markSchemaChangeAfterCheckpoint();
                        }
                        if (next == 2) {
                            next = 3;
                            output.collect(new SeaTunnelRow(new Object[] {3, "restored"}));
                        }
                        context.signalNoMoreElement();
                    }
                }
            };
        }
    }

    static class TestSource implements SeaTunnelSource<SeaTunnelRow, TestSplit, Integer> {
        volatile int limit = 3;
        volatile boolean failOpen;
        volatile boolean failPoll;
        volatile boolean failClose;
        volatile boolean fast;
        transient Runnable beforeSnapshot;
        final java.util.concurrent.atomic.AtomicInteger closedReaders =
                new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.atomic.AtomicInteger closedEnumerators =
                new java.util.concurrent.atomic.AtomicInteger();
        final List<Long> acknowledged = Collections.synchronizedList(new ArrayList<>());

        @Override
        public String getPluginName() {
            return "test-recovery";
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.UNBOUNDED;
        }

        @Override
        public List<CatalogTable> getProducedCatalogTables() {
            return Collections.singletonList(table());
        }

        @Override
        public SourceReader<SeaTunnelRow, TestSplit> createReader(SourceReader.Context context) {
            return new SourceReader<SeaTunnelRow, TestSplit>() {
                int next = -1;
                boolean emitted;

                @Override
                public void open() {
                    if (failOpen) throw new IllegalStateException("open failure");
                }

                @Override
                public void close() throws IOException {
                    closedReaders.incrementAndGet();
                    if (failClose) throw new IOException("close failure");
                }

                @Override
                public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
                    if (failPoll) throw new IOException("poll failure");
                    synchronized (output.getCheckpointLock()) {
                        if (next >= 0 && next < limit) {
                            output.collect(
                                    new SeaTunnelRow(
                                            new Object[] {
                                                context.getIndexOfSubtask() * 100 + ++next
                                            }));
                        }
                    }
                    if (next >= limit) {
                        context.signalNoMoreElement();
                    }
                    if (!fast) {
                        Thread.sleep(1);
                    }
                }

                @Override
                public List<TestSplit> snapshotState(long id) {
                    if (beforeSnapshot != null) {
                        beforeSnapshot.run();
                    }
                    return Collections.singletonList(new TestSplit(next));
                }

                @Override
                public void addSplits(List<TestSplit> splits) {
                    next = splits.get(0).next;
                }

                @Override
                public void handleNoMoreSplits() {}

                @Override
                public void notifyCheckpointComplete(long id) {
                    acknowledged.add(id);
                }
            };
        }

        @Override
        public SourceSplitEnumerator<TestSplit, Integer> createEnumerator(
                SourceSplitEnumerator.Context<TestSplit> context) {
            return enumerator(context, false);
        }

        @Override
        public SourceSplitEnumerator<TestSplit, Integer> restoreEnumerator(
                SourceSplitEnumerator.Context<TestSplit> context, Integer state) {
            return enumerator(context, true);
        }

        private SourceSplitEnumerator<TestSplit, Integer> enumerator(
                SourceSplitEnumerator.Context<TestSplit> context, boolean restored) {
            return new SourceSplitEnumerator<TestSplit, Integer>() {
                @Override
                public void open() {}

                @Override
                public void run() {}

                @Override
                public void close() {
                    closedEnumerators.incrementAndGet();
                }

                @Override
                public void registerReader(int id) {
                    if (!restored) context.assignSplit(id, new TestSplit(0));
                }

                @Override
                public void addSplitsBack(List<TestSplit> splits, int id) {
                    context.assignSplit(id, splits);
                }

                @Override
                public int currentUnassignedSplitSize() {
                    return 0;
                }

                @Override
                public void handleSplitRequest(int id) {}

                @Override
                public Integer snapshotState(long id) {
                    return 0;
                }

                @Override
                public void notifyCheckpointComplete(long id) {}
            };
        }
    }

    static class CoordinatedTestSource extends TestSource implements SupportCoordinate {}

    static final class TestSplit implements SourceSplit {
        final int next;

        TestSplit(int next) {
            this.next = next;
        }

        @Override
        public String splitId() {
            return "test";
        }
    }
}
