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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.reader.fetch;

import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.offset.BinlogOffset;

import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.relational.TableId;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MySqlSourceFetchTaskContextTest {
    private Configuration configuration() {
        return Configuration.create()
                .with("database.server.name", "split-test")
                .with("database.hostname", "localhost")
                .with("database.user", "test")
                .with("database.server.id", 2000)
                .with("table.include.list", "db.a,db.b")
                .build();
    }

    @Test
    void schemaResumeKeepsPendingDdlAndSkipsCompletedDdl() {
        MySqlSourceFetchTaskContext context =
                org.mockito.Mockito.mock(
                        MySqlSourceFetchTaskContext.class, org.mockito.Mockito.CALLS_REAL_METHODS);
        for (String gtids : new String[] {null, "24bc7850-2c16-11e6-a073-0242ac110002:1-80"}) {
            BinlogOffset before = schemaOffset(200, gtids, 223344);
            BinlogOffset ddl = schemaOffset(300, gtids, 223344);
            BinlogOffset after = schemaOffset(400, gtids, 223344);
            SourceRecord record = schemaRecord(ddl);
            assertTrue(context.shouldEmitSchemaChange(record, before));
            assertFalse(context.shouldEmitSchemaChange(record, ddl));
            assertFalse(context.shouldEmitSchemaChange(record, after));
            assertTrue(context.shouldEmitSchemaChange(record, new BinlogOffset(123L)));
        }
    }

    @Test
    void schemaResumeDoesNotComparePhysicalPositionsAcrossServers() {
        MySqlSourceFetchTaskContext context =
                org.mockito.Mockito.mock(
                        MySqlSourceFetchTaskContext.class, org.mockito.Mockito.CALLS_REAL_METHODS);
        String gtids = "24bc7850-2c16-11e6-a073-0242ac110002:1-80";
        assertTrue(
                context.shouldEmitSchemaChange(
                        schemaRecord(schemaOffset(200, gtids, 2)), schemaOffset(300, gtids, 1)));
    }

    private static BinlogOffset schemaOffset(long position, String gtids, int server) {
        return new BinlogOffset("mysql-bin.000003", position, 0, 0, 0, gtids, server);
    }

    private static SourceRecord schemaRecord(BinlogOffset offset) {
        return new SourceRecord(Collections.emptyMap(), offset.getOffset(), "schema", null, null);
    }

    @Test
    void incrementalCaptureMatchesTheSplitHistoryOwnership() {
        TableId a = new TableId("db", null, "a");
        TableId b = new TableId("db", null, "b");
        Configuration original = configuration();
        MySqlConnectorConfig scoped =
                MySqlSourceFetchTaskContext.createSplitConnectorConfig(
                        original,
                        new IncrementalSplit(
                                "a",
                                Collections.singletonList(a),
                                null,
                                null,
                                Collections.emptyList()));
        assertTrue(scoped.getTableFilters().dataCollectionFilter().isIncluded(a));
        assertFalse(scoped.getTableFilters().dataCollectionFilter().isIncluded(b));
        assertTrue(
                new MySqlConnectorConfig(original)
                        .getTableFilters()
                        .dataCollectionFilter()
                        .isIncluded(b));
    }

    @Test
    void splitCaptureTreatsTableNamesAsLiterals() {
        TableId owned = new TableId("db", null, "a.b+$");
        MySqlConnectorConfig scoped =
                MySqlSourceFetchTaskContext.createSplitConnectorConfig(
                        configuration(),
                        new IncrementalSplit(
                                "literal",
                                Collections.singletonList(owned),
                                null,
                                null,
                                Collections.emptyList()));
        assertTrue(scoped.getTableFilters().dataCollectionFilter().isIncluded(owned));
        assertFalse(
                scoped.getTableFilters()
                        .dataCollectionFilter()
                        .isIncluded(new TableId("db", null, "axb")));
    }

    @Test
    void splitCaptureEscapesCommaSeparatorsInsideLiteralNames() {
        TableId owned = new TableId("db", null, "a,b");
        Configuration original =
                configuration().edit().with("table.include.list", "db[.]a.*").build();
        assertTrue(
                new MySqlConnectorConfig(original)
                        .getTableFilters()
                        .dataCollectionFilter()
                        .isIncluded(owned));
        MySqlConnectorConfig scoped =
                MySqlSourceFetchTaskContext.createSplitConnectorConfig(
                        original,
                        new IncrementalSplit(
                                "comma",
                                Collections.singletonList(owned),
                                null,
                                null,
                                Collections.emptyList()));
        assertTrue(scoped.getTableFilters().dataCollectionFilter().isIncluded(owned));
        assertFalse(
                scoped.getTableFilters()
                        .dataCollectionFilter()
                        .isIncluded(new TableId("db", null, "a")));
        assertFalse(
                scoped.getTableFilters()
                        .dataCollectionFilter()
                        .isIncluded(new TableId("db", null, "b")));
    }

    @Test
    void snapshotCapturePreservesTheExistingConnectorFilter() {
        MySqlConnectorConfig config =
                MySqlSourceFetchTaskContext.createSplitConnectorConfig(
                        configuration(),
                        new SnapshotSplit(
                                "snapshot",
                                new TableId("db", null, "a"),
                                null,
                                null,
                                null,
                                null,
                                null));
        assertTrue(
                config.getTableFilters()
                        .dataCollectionFilter()
                        .isIncluded(new TableId("db", null, "a")));
        assertTrue(
                config.getTableFilters()
                        .dataCollectionFilter()
                        .isIncluded(new TableId("db", null, "b")));
    }
}
