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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.reader;

import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.IncrementalSourceStreamFetcher;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.exception.PostgresConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.PostgresRelationSchemaChangeResolver;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.PostgresRelationSchemaRecord;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.offset.LsnOffset;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ServerInfo;
import io.debezium.connector.postgresql.spi.OffsetState;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PostgresSourceFetchTaskContextTest {

    private static final String SLOT_NAME = "seatunnel";
    private static final String PLUGIN_NAME = "pgoutput";

    private final PostgresConnectorConfig connectorConfig =
            new PostgresConnectorConfig(
                    Configuration.create()
                            .with(PostgresConnectorConfig.SLOT_NAME, SLOT_NAME)
                            .with(PostgresConnectorConfig.PLUGIN_NAME, PLUGIN_NAME)
                            .with("database.server.name", "postgres_cdc_source")
                            .with("database.dbname", "testdb")
                            .build());

    @Test
    public void schemaHistoryOwnershipKeepsSchemaAndQuotedTableNames() throws Exception {
        PostgresSourceFetchTaskContext context =
                mock(PostgresSourceFetchTaskContext.class, org.mockito.Mockito.CALLS_REAL_METHODS);
        when(context.getDbzConnectorConfig()).thenReturn(connectorConfig);
        Assertions.assertEquals(
                new TableId("testdb", "public", "a.b"),
                context.parseSchemaChangeTableId("\"public\".\"a.b\""));
        Assertions.assertEquals(
                new TableId("testdb", "other", "a.b"),
                context.parseSchemaChangeTableId("\"other\".\"a.b\""));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> context.parseSchemaChangeTableId("\"otherdb\".\"public\".\"a.b\""));
        SourceRecord relation =
                PostgresRelationSchemaRecord.create(
                        Table.editor().tableId(new TableId(null, "public", "a.b")).create(),
                        Collections.emptyMap(),
                        Collections.singletonMap("lsn", 123L),
                        "relation");
        Struct value = (Struct) relation.value();
        List<Struct> changes = value.getArray("tableChanges");
        Assertions.assertEquals(
                new TableId("testdb", "public", "a.b"),
                context.parseSchemaChangeTableId(changes.get(0).getString("id")));
        Assertions.assertTrue(
                context.shouldEmitSchemaChange(
                        relation, new LsnOffset(Collections.singletonMap("lsn", "123"))));
        org.mockito.Mockito.doNothing().when(context).close();
        try (SchemaOwnershipProbe probe = new SchemaOwnershipProbe(context, relation)) {
            Assertions.assertTrue(probe.owns(new TableId("testdb", "public", "a.b")));
            Assertions.assertFalse(probe.owns(new TableId("testdb", "other", "a.b")));
            Assertions.assertFalse(probe.owns(new TableId("testdb", "public", "sibling")));
        }
    }

    private static final class SchemaOwnershipProbe implements AutoCloseable {
        private final IncrementalSourceStreamFetcher fetcher;
        private final SourceRecord relation;

        SchemaOwnershipProbe(PostgresSourceFetchTaskContext context, SourceRecord relation)
                throws Exception {
            this.relation = relation;
            fetcher =
                    new IncrementalSourceStreamFetcher(
                            context, 0, new PostgresRelationSchemaChangeResolver());
            Field cut = fetcher.getClass().getDeclaredField("splitStartWatermark");
            cut.setAccessible(true);
            cut.set(fetcher, new LsnOffset(Collections.singletonMap("lsn", "123")));
        }

        boolean owns(TableId table) throws Exception {
            Field split = fetcher.getClass().getDeclaredField("currentIncrementalSplit");
            split.setAccessible(true);
            split.set(
                    fetcher,
                    new IncrementalSplit(
                            "relation",
                            Collections.singletonList(table),
                            null,
                            null,
                            Collections.emptyList()));
            Method emit = fetcher.getClass().getDeclaredMethod("shouldEmit", SourceRecord.class);
            emit.setAccessible(true);
            return (boolean) emit.invoke(fetcher, relation);
        }

        @Override
        public void close() {
            fetcher.close();
        }
    }

    @Test
    public void testSnapshotterIsInitializedBeforeItIsAsked() throws SQLException {
        SlotState slotState = new SlotState(null, null, 0L, true);
        PostgresConnection dataConnection = connectionReturningSlot(null);
        when(dataConnection.getReplicationSlotState(SLOT_NAME, PLUGIN_NAME)).thenReturn(slotState);
        InitRequiredSnapshotter snapshotter = new InitRequiredSnapshotter(true);

        Assertions.assertSame(
                slotState,
                PostgresSourceFetchTaskContext.initSnapshotter(
                        snapshotter, connectorConfig, null, dataConnection));
        Assertions.assertSame(slotState, snapshotter.slotState);
    }

    @Test
    public void testInvalidatedSlotFailsBeforeStreaming() throws SQLException {
        PostgresConnection dataConnection = connectionReturningSlot("idle_timeout");
        InitRequiredSnapshotter snapshotter = new InitRequiredSnapshotter(true);

        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () ->
                                PostgresSourceFetchTaskContext.initSnapshotter(
                                        snapshotter, connectorConfig, null, dataConnection));
        Assertions.assertEquals(
                PostgresConnectorErrorCode.REPLICATION_SLOT_INVALIDATED,
                exception.getSeaTunnelErrorCode());
        Assertions.assertTrue(snapshotter.initialized);
        verify(dataConnection, never()).getReplicationSlotState(anyString(), anyString());
    }

    @Test
    public void testInvalidatedSlotDoesNotBlockSnapshotOnlyJob() throws SQLException {
        PostgresConnection dataConnection = connectionReturningSlot("wal_removed");
        InitRequiredSnapshotter snapshotter = new InitRequiredSnapshotter(false);

        Assertions.assertNull(
                PostgresSourceFetchTaskContext.initSnapshotter(
                        snapshotter, connectorConfig, null, dataConnection));
        Assertions.assertTrue(snapshotter.initialized);
        Assertions.assertNull(snapshotter.slotState);
        verify(dataConnection, never()).getReplicationSlotState(anyString(), anyString());
    }

    @Test
    public void testFailedCheckRollsBackAndReadsSlotState() throws SQLException {
        SlotState slotState = new SlotState(null, null, 0L, true);
        Connection connection = mock(Connection.class);
        when(connection.prepareStatement(anyString())).thenThrow(new SQLException("denied"));
        PostgresConnection dataConnection = mock(PostgresConnection.class);
        when(dataConnection.connection()).thenReturn(connection);
        when(dataConnection.serverInfo()).thenReturn(mock(ServerInfo.class));
        when(dataConnection.getReplicationSlotState(SLOT_NAME, PLUGIN_NAME)).thenReturn(slotState);
        InitRequiredSnapshotter snapshotter = new InitRequiredSnapshotter(true);

        Assertions.assertSame(
                slotState,
                PostgresSourceFetchTaskContext.initSnapshotter(
                        snapshotter, connectorConfig, null, dataConnection));
        verify(dataConnection).rollback();
        Assertions.assertSame(slotState, snapshotter.slotState);
    }

    private static PostgresConnection connectionReturningSlot(String invalidationReason)
            throws SQLException {
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);
        when(metaData.getColumnCount()).thenReturn(2);
        when(metaData.getColumnName(1)).thenReturn("slot_name");
        when(metaData.getColumnName(2)).thenReturn("invalidation_reason");
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getMetaData()).thenReturn(metaData);
        when(resultSet.getString("invalidation_reason")).thenReturn(invalidationReason);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(statement.executeQuery()).thenReturn(resultSet);
        Connection connection = mock(Connection.class);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        PostgresConnection dataConnection = mock(PostgresConnection.class);
        when(dataConnection.connection()).thenReturn(connection);
        when(dataConnection.serverInfo()).thenReturn(mock(ServerInfo.class));
        return dataConnection;
    }

    /** A custom snapshotter that, like many real ones, decides from the state given to init. */
    private static class InitRequiredSnapshotter implements Snapshotter {
        private final boolean stream;
        private boolean initialized;
        private SlotState slotState;

        private InitRequiredSnapshotter(boolean stream) {
            this.stream = stream;
        }

        @Override
        public void init(
                PostgresConnectorConfig config, OffsetState sourceInfo, SlotState slotState) {
            this.initialized = true;
            this.slotState = slotState;
        }

        @Override
        public boolean shouldSnapshot() {
            checkInitialized();
            return !stream;
        }

        @Override
        public boolean shouldStream() {
            checkInitialized();
            return stream;
        }

        @Override
        public Optional<String> buildSnapshotQuery(
                TableId tableId, List<String> snapshotSelectColumns) {
            return Optional.empty();
        }

        private void checkInitialized() {
            if (!initialized) {
                throw new IllegalStateException("init must be called first");
            }
        }
    }
}
