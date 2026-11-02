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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.translation.spark.execution.MultiTableManager;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.streaming.StreamingQuery;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataOutputStream;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SparkSchemaBoundaryCoordinatorTest {
    @TempDir Path root;
    final List<String> calls = new ArrayList<>();

    List<CatalogTable> tables() {
        return Collections.singletonList(
                CatalogTableUtil.getCatalogTable(
                        "test",
                        "db",
                        null,
                        "records",
                        new SeaTunnelRowType(
                                new String[] {"id"},
                                new SeaTunnelDataType[] {BasicType.INT_TYPE})));
    }

    void log(String name, String value) throws Exception {
        Path p = root.resolve(name);
        Files.createDirectories(p.getParent());
        Files.write(p, value.getBytes(StandardCharsets.UTF_8));
    }

    void publish(long id, boolean committed) throws Exception {
        CatalogTable table = tables().get(0);
        String name = table.getTableSchema().getColumns().get(0).getName();
        List<SchemaChangeEvent> events =
                Arrays.asList(
                        new AlterTableDropColumnEvent(table.getTableId(), name),
                        AlterTableAddColumnEvent.add(
                                table.getTableId(), table.getTableSchema().getColumns().get(0)));
        Path p = root.resolve("sources/0/seatunnel-batches/0/" + id + "/state");
        Files.createDirectories(p.getParent());
        try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(p))) {
            new SparkSourceCheckpoint(
                            1,
                            new MultiTableManager(tables().toArray(new CatalogTable[0]))
                                    .getTableSchema()
                                    .json(),
                            Collections.emptyMap(),
                            tables(),
                            tables(),
                            events,
                            SparkSourceCheckpoint.Phase.AFTER_SCHEMA_CHANGE)
                    .write(out);
        }
        log("offsets/" + id, "v1\n{}\n{\"checkpointId\":" + id + "}\n");
        if (committed) log("commits/" + id, "v1\n{}\n");
    }

    StreamingQuery query(Callable<Boolean> await, Runnable stop) {
        return (StreamingQuery)
                Proxy.newProxyInstance(
                        getClass().getClassLoader(),
                        new Class<?>[] {StreamingQuery.class},
                        (p, m, a) -> {
                            if (m.getName().equals("awaitTermination")) return await.call();
                            if (m.getName().equals("stop")) {
                                stop.run();
                                return null;
                            }
                            throw new AssertionError(m.getName());
                        });
    }

    abstract class Factory implements SparkSchemaBoundaryCoordinator.QueryFactory {
        public StreamingQuery start(List<CatalogTable> catalogs) {
            throw new AssertionError("lost applied boundary");
        }

        public void reconcile(SparkCommittedSourceState b) throws Exception {
            calls.add("reconcile:" + b.getSourceCheckpointId());
        }
    }

    void run(Factory f) throws Exception {
        new SparkSchemaBoundaryCoordinator()
                .run(new Configuration(), root.toString(), "", tables(), f);
    }

    @Test
    public void disabledSourceKeepsLiveCatalogsWithoutChangingCommittedOrPendingJournals()
            throws Exception {
        List<CatalogTable> live =
                Collections.singletonList(
                        CatalogTableUtil.getCatalogTable(
                                "test",
                                "db",
                                null,
                                "records",
                                new SeaTunnelRowType(
                                        new String[] {"id", "name"},
                                        new SeaTunnelDataType[] {
                                            BasicType.INT_TYPE, BasicType.STRING_TYPE
                                        })));
        SparkSourceCheckpoint old =
                new SparkSourceCheckpoint(
                        1,
                        new MultiTableManager(tables().toArray(new CatalogTable[0]))
                                .getTableSchema()
                                .json(),
                        Collections.emptyMap(),
                        tables(),
                        tables(),
                        Collections.emptyList(),
                        SparkSourceCheckpoint.Phase.DATA);
        Path state = root.resolve("sources/0/seatunnel-batches/0/1/state");
        Files.createDirectories(state.getParent());
        try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(state))) {
            old.write(out);
        }
        log("commits/0", "v1\n{}\n");
        log("offsets/0", "v1\n{}\n{\"checkpointId\":1}\n");
        Path pending = root.resolve("sources/0/seatunnel-batches/0/2/state");
        Files.createDirectories(pending.getParent());
        Files.copy(state, pending);
        byte[] before = Files.readAllBytes(pending);
        new SparkSchemaBoundaryCoordinator()
                .run(
                        new Configuration(),
                        root.toString(),
                        "",
                        live,
                        new Factory() {
                            public boolean restoreCheckpointSchema() {
                                return false;
                            }

                            public StreamingQuery start(List<CatalogTable> catalogs, long applied) {
                                assertTrue(SparkSourceCatalog.sameSchemas(live, catalogs));
                                assertEquals(-1, applied);
                                return query(() -> true, () -> calls.add("stop"));
                            }
                        });
        assertEquals(Collections.singletonList("stop"), calls);
        org.junit.jupiter.api.Assertions.assertArrayEquals(before, Files.readAllBytes(pending));
        MultiTableManager manager = new MultiTableManager(live.toArray(new CatalogTable[0]));
        old.validateLiveSchemaPredecessor(1, manager);
        assertThrows(java.io.IOException.class, () -> old.validate(1, manager, true));
        assertThrows(
                java.io.IOException.class, () -> old.validateLiveSchemaPredecessor(2, manager));
        CatalogTable other =
                CatalogTableUtil.getCatalogTable(
                        "test", "db", null, "other", live.get(0).getSeaTunnelRowType());
        assertThrows(
                java.io.IOException.class,
                () ->
                        old.validateLiveSchemaPredecessor(
                                1, new MultiTableManager(new CatalogTable[] {other})));
    }

    @Test
    public void disabledSourceRejectsPendingBoundaryBeforeQueryOrDdl() throws Exception {
        publish(2, true);
        assertThrows(
                IllegalStateException.class,
                () ->
                        run(
                                new Factory() {
                                    public boolean restoreCheckpointSchema() {
                                        return false;
                                    }

                                    public StreamingQuery start(
                                            List<CatalogTable> catalogs, long applied) {
                                        calls.add("start");
                                        return null;
                                    }
                                }));
        assertTrue(calls.isEmpty());
    }

    @Test
    public void equalSchemaPendingBoundaryPausesUntilItsExactIdentityApplied() throws Exception {
        publish(2, false);
        MultiTableManager analyzed = new MultiTableManager(tables().toArray(new CatalogTable[0]));
        String source = root.resolve("sources/0").toString();
        Configuration conf = new Configuration();
        assertTrue(SparkSourceCheckpoint.shouldPause(conf, source, 2, "", analyzed, -1));
        assertTrue(SparkSourceCheckpoint.shouldPause(conf, source, 2, "", analyzed, 1));
        assertFalse(SparkSourceCheckpoint.shouldPause(conf, source, 2, "", analyzed, 2));
        assertFalse(SparkCommittedSourceState.load(conf, root.toString(), 0, 1, "").isPresent());
    }

    @Test
    public void committedEqualSchemaRebuildsOnceAndNewerEqualSchemaRebuildsAgain()
            throws Exception {
        AtomicInteger starts = new AtomicInteger();
        run(
                new Factory() {
                    public StreamingQuery start(List<CatalogTable> catalogs, long applied) {
                        int n = starts.incrementAndGet();
                        calls.add("start:" + applied);
                        assertTrue(SparkSourceCatalog.sameSchemas(catalogs, tables()));
                        AtomicInteger polls = new AtomicInteger();
                        return query(
                                () -> {
                                    int poll = polls.incrementAndGet();
                                    if (n == 1) {
                                        publish(2, true);
                                        return false;
                                    }
                                    if (n == 2) {
                                        if (poll == 1) return false;
                                        publish(3, true);
                                        return false;
                                    }
                                    return true;
                                },
                                () -> calls.add("stop:" + applied));
                    }
                });
        assertEquals(
                Arrays.asList(
                        "start:-1",
                        "stop:-1",
                        "reconcile:2",
                        "start:2",
                        "stop:2",
                        "reconcile:3",
                        "start:3",
                        "stop:3"),
                calls);
    }

    @Test
    public void failedReconcileDoesNotAdvanceIdentityAndRestartRetriesBeforeStart()
            throws Exception {
        RuntimeException failure = new IllegalStateException("DDL ambiguity");
        assertSame(
                failure,
                assertThrows(
                        RuntimeException.class,
                        () ->
                                run(
                                        new Factory() {
                                            public StreamingQuery start(
                                                    List<CatalogTable> catalogs, long applied) {
                                                calls.add("start:" + applied);
                                                return query(
                                                        () -> {
                                                            publish(2, true);
                                                            return false;
                                                        },
                                                        () -> calls.add("stop:" + applied));
                                            }

                                            public void reconcile(SparkCommittedSourceState b) {
                                                calls.add("failed:" + b.getSourceCheckpointId());
                                                throw failure;
                                            }
                                        })));
        run(
                new Factory() {
                    public StreamingQuery start(List<CatalogTable> catalogs, long applied) {
                        assertEquals(2, applied);
                        calls.add("restart:" + applied);
                        return query(() -> true, () -> calls.add("stop-restart"));
                    }
                });
        assertEquals(
                Arrays.asList(
                        "start:-1",
                        "stop:-1",
                        "failed:2",
                        "reconcile:2",
                        "restart:2",
                        "stop-restart"),
                calls);
    }

    @Test
    public void failedReplacementStartReconcilesSameIdentityAgainAfterDriverRestart()
            throws Exception {
        publish(2, true);
        RuntimeException failure = new IllegalStateException("replacement start");
        assertSame(
                failure,
                assertThrows(
                        RuntimeException.class,
                        () ->
                                run(
                                        new Factory() {
                                            public StreamingQuery start(
                                                    List<CatalogTable> catalogs, long applied) {
                                                assertEquals(2, applied);
                                                calls.add("failed-start:" + applied);
                                                throw failure;
                                            }
                                        })));
        run(
                new Factory() {
                    public StreamingQuery start(List<CatalogTable> catalogs, long applied) {
                        calls.add("start:" + applied);
                        return query(() -> true, () -> calls.add("stop"));
                    }
                });
        assertEquals(
                Arrays.asList("reconcile:2", "failed-start:2", "reconcile:2", "start:2", "stop"),
                calls);
    }
}
