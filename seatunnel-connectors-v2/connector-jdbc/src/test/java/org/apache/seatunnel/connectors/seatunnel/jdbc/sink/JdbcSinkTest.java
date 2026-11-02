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

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSinkState;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * Verifies restored JDBC schemas retain configured fallback and reject divergent state.
 *
 * <p>Legacy state without a schema must keep the configured table definition, while incompatible
 * schemas from the same checkpoint must fail instead of silently choosing one.
 */
class JdbcSinkTest {

    @Test
    void shouldUseTheOnlyRestoredSchemaOrTheInitialSchema() {
        TableSchema initialSchema = tableSchema("id");
        TableSchema restoredSchema = tableSchema("id", "email");

        Assertions.assertEquals(
                initialSchema,
                JdbcSink.resolveRestoredTableSchema(
                        initialSchema, Collections.singletonList(new JdbcSinkState(null))));
        Assertions.assertEquals(
                restoredSchema,
                JdbcSink.resolveRestoredTableSchema(
                        initialSchema,
                        Arrays.asList(
                                new JdbcSinkState(null, restoredSchema),
                                new JdbcSinkState(null, restoredSchema))));
    }

    @Test
    void shouldRejectDivergentRestoredSchemas() {
        TableSchema initialSchema = tableSchema("id");

        IllegalStateException exception =
                Assertions.assertThrows(
                        IllegalStateException.class,
                        () ->
                                JdbcSink.resolveRestoredTableSchema(
                                        initialSchema,
                                        Arrays.asList(
                                                new JdbcSinkState(null, tableSchema("id", "email")),
                                                new JdbcSinkState(
                                                        null, tableSchema("id", "phone")))));

        Assertions.assertTrue(exception.getMessage().contains("divergent table schemas"));
    }

    private org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent addColumn(String name) {
        return org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent.add(
                org.apache.seatunnel.api.table.catalog.TableIdentifier.of(
                        "test", "db", null, "records"),
                PhysicalColumn.of(name, BasicType.STRING_TYPE, (Long) null, true, null, null));
    }

    @Test
    void ambiguousDdlSuccessIsNotRepeatedAfterRecovery() throws Exception {
        TableSchema before = tableSchema("id"), after = tableSchema("id", "name");
        java.util.concurrent.atomic.AtomicReference<TableSchema> physical =
                new java.util.concurrent.atomic.AtomicReference<>(before);
        java.util.concurrent.atomic.AtomicInteger ddlCalls =
                new java.util.concurrent.atomic.AtomicInteger();
        java.util.List<org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent> events =
                Collections.singletonList(addColumn("name"));
        Assertions.assertThrows(
                java.io.IOException.class,
                () ->
                        JdbcSchemaChangeReconciler.reconcile(
                                before,
                                after,
                                events,
                                physical::get,
                                event -> {
                                    ddlCalls.incrementAndGet();
                                    physical.set(after);
                                    throw new java.io.IOException(
                                            "Connection lost after DDL committed");
                                }));
        JdbcSchemaChangeReconciler.reconcile(
                before,
                after,
                events,
                physical::get,
                event -> {
                    throw new AssertionError("Duplicate physical DDL");
                });
        Assertions.assertEquals(1, ddlCalls.get());
        Assertions.assertEquals(after, physical.get());
    }

    @Test
    void partialDdlBlockResumesAtTheFirstUnappliedChange() throws Exception {
        TableSchema before = tableSchema("id"), after = tableSchema("id", "name", "email");
        java.util.List<org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent> events =
                Arrays.asList(addColumn("name"), addColumn("email"));
        java.util.concurrent.atomic.AtomicReference<TableSchema> physical =
                new java.util.concurrent.atomic.AtomicReference<>(before);
        java.util.concurrent.atomic.AtomicInteger ddlCalls =
                new java.util.concurrent.atomic.AtomicInteger();
        JdbcSchemaChangeReconciler.SchemaApplier apply =
                event -> {
                    physical.set(
                            new org.apache.seatunnel.api.table.schema.handler
                                            .TableSchemaChangeEventDispatcher()
                                    .reset(physical.get())
                                    .apply(event));
                    if (ddlCalls.incrementAndGet() == 1)
                        throw new java.io.IOException("Crash between DDL statements");
                };
        Assertions.assertThrows(
                java.io.IOException.class,
                () ->
                        JdbcSchemaChangeReconciler.reconcile(
                                before, after, events, physical::get, apply));
        JdbcSchemaChangeReconciler.reconcile(before, after, events, physical::get, apply);
        Assertions.assertEquals(2, ddlCalls.get());
        Assertions.assertEquals(after, physical.get());
    }

    @Test
    void divergentPhysicalSchemaCannotApplyTheCommittedBlock() {
        Assertions.assertThrows(
                java.io.IOException.class,
                () ->
                        JdbcSchemaChangeReconciler.reconcile(
                                tableSchema("id"),
                                tableSchema("id", "name"),
                                Collections.singletonList(addColumn("name")),
                                () -> tableSchema("foreign"),
                                event -> {
                                    throw new AssertionError("Divergent DDL");
                                }));
    }

    @Test
    void physicalDdlMustReachItsExpectedSchemaBeforeRebuilding() {
        Assertions.assertThrows(
                java.io.IOException.class,
                () ->
                        JdbcSchemaChangeReconciler.reconcile(
                                tableSchema("id"),
                                tableSchema("id", "name"),
                                Collections.singletonList(addColumn("name")),
                                () -> tableSchema("id"),
                                event -> {}));
    }

    @Test
    void localSchemaRestoreCannotReplayPhysicalDdl() {
        org.apache.seatunnel.api.table.catalog.CatalogTable restored =
                org.apache.seatunnel.api.table.catalog.CatalogTableUtil.getCatalogTable(
                        "test",
                        "db",
                        null,
                        "records",
                        new org.apache.seatunnel.api.table.type.SeaTunnelRowType(
                                new String[] {"id"},
                                new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                                    BasicType.STRING_TYPE
                                }));
        Assertions.assertThrows(
                java.io.IOException.class,
                () ->
                        JdbcSchemaChangeReconciler.reconcile(
                                tableSchema("id"),
                                tableSchema("id"),
                                Collections.singletonList(
                                        new org.apache.seatunnel.api.table.schema.event
                                                .RestoreTableSchemaEvent(restored)),
                                () -> {
                                    throw new AssertionError(
                                            "Local restore inspected physical DDL");
                                },
                                event -> {
                                    throw new AssertionError("Local restore applied physical DDL");
                                }));
    }

    @Test
    void siblingPhysicalColumnsDoNotRepeatOrBlockAnOwnedAdd() throws Exception {
        TableSchema before = tableSchema("id"), after = tableSchema("id", "name");
        java.util.concurrent.atomic.AtomicReference<TableSchema> physical =
                new java.util.concurrent.atomic.AtomicReference<>(tableSchema("id", "sibling"));
        java.util.concurrent.atomic.AtomicInteger calls =
                new java.util.concurrent.atomic.AtomicInteger();
        java.util.List<org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent> events =
                Collections.singletonList(addColumn("name"));
        JdbcSchemaChangeReconciler.reconcile(
                before,
                after,
                events,
                physical::get,
                event -> {
                    calls.incrementAndGet();
                    physical.set(tableSchema("id", "sibling", "name"));
                });
        JdbcSchemaChangeReconciler.reconcile(
                before,
                after,
                events,
                physical::get,
                event -> {
                    throw new AssertionError("Shared physical ADD repeated");
                });
        Assertions.assertEquals(1, calls.get());
    }

    @Test
    void compoundAlterResumesAfterAnIndividuallyCommittedChild() throws Exception {
        TableSchema before = tableSchema("id"), after = tableSchema("id", "name", "email");
        org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent block =
                new org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent(
                        org.apache.seatunnel.api.table.catalog.TableIdentifier.of(
                                "test", "db", null, "records"));
        block.addEvent(
                (org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent)
                        addColumn("name"));
        block.addEvent(
                (org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent)
                        addColumn("email"));
        java.util.concurrent.atomic.AtomicReference<TableSchema> physical =
                new java.util.concurrent.atomic.AtomicReference<>(before);
        java.util.concurrent.atomic.AtomicInteger calls =
                new java.util.concurrent.atomic.AtomicInteger();
        JdbcSchemaChangeReconciler.SchemaApplier apply =
                event -> {
                    physical.set(
                            new org.apache.seatunnel.api.table.schema.handler
                                            .TableSchemaChangeEventDispatcher()
                                    .reset(physical.get())
                                    .apply(event));
                    if (calls.incrementAndGet() == 1)
                        throw new java.io.IOException("Crash after first child committed");
                };
        Assertions.assertThrows(
                java.io.IOException.class,
                () ->
                        JdbcSchemaChangeReconciler.reconcile(
                                before,
                                after,
                                Collections.singletonList(block),
                                physical::get,
                                apply));
        JdbcSchemaChangeReconciler.reconcile(
                before, after, Collections.singletonList(block), physical::get, apply);
        Assertions.assertEquals(2, calls.get());
        Assertions.assertEquals(after, physical.get());
    }

    @Test
    void mysqlCharacterCapacityIsIndependentOfCharsetByteCapacity() throws Exception {
        TableSchema before = tableSchema("id");
        org.apache.seatunnel.api.table.catalog.Column source =
                PhysicalColumn.of("name", BasicType.STRING_TYPE, 40L, true, null, null)
                        .reSourceType("VARCHAR(10)");
        org.apache.seatunnel.api.table.catalog.Column receiver =
                PhysicalColumn.of("name", BasicType.STRING_TYPE, 10L, true, null, null)
                        .reSourceType("varchar(10)");
        TableSchema after =
                TableSchema.builder()
                        .columns(java.util.Arrays.asList(before.getColumns().get(0), source))
                        .build();
        TableSchema physical =
                TableSchema.builder()
                        .columns(java.util.Arrays.asList(before.getColumns().get(0), receiver))
                        .build();
        org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent event =
                org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent.add(
                        org.apache.seatunnel.api.table.catalog.TableIdentifier.of(
                                "test", "db", null, "records"),
                        source);
        JdbcSchemaChangeReconciler.reconcile(
                before,
                after,
                Collections.singletonList(event),
                () -> physical,
                ignored -> {
                    throw new AssertionError("VARCHAR(10) DDL repeated after restart");
                },
                "MySQL");
        org.apache.seatunnel.api.table.catalog.Column different =
                PhysicalColumn.of("name", BasicType.STRING_TYPE, 11L, true, null, null)
                        .reSourceType("varchar(11)");
        TableSchema divergent =
                TableSchema.builder()
                        .columns(java.util.Arrays.asList(before.getColumns().get(0), different))
                        .build();
        Assertions.assertThrows(
                java.io.IOException.class,
                () ->
                        JdbcSchemaChangeReconciler.reconcile(
                                before,
                                after,
                                Collections.singletonList(event),
                                () -> divergent,
                                ignored -> {
                                    throw new AssertionError("Divergent capacity accepted");
                                },
                                "MySQL"));
    }

    @Test
    void mysqlDefaultNormalizationPreservesTypesAndTemporalPrecision() throws Exception {
        assertAlreadyAppliedDefault(
                org.apache.seatunnel.api.table.type.LocalTimeType.LOCAL_DATE_TIME_TYPE,
                "current_timestamp()",
                "CURRENT_TIMESTAMP",
                true);
        assertAlreadyAppliedDefault(
                org.apache.seatunnel.api.table.type.LocalTimeType.OFFSET_DATE_TIME_TYPE,
                "current_timestamp()",
                "CURRENT_TIMESTAMP",
                true);
        assertAlreadyAppliedDefault(BasicType.BOOLEAN_TYPE, "true", "1", true);
        assertAlreadyAppliedDefault(BasicType.BOOLEAN_TYPE, "FALSE", "0", true);
        assertAlreadyAppliedDefault(
                org.apache.seatunnel.api.table.type.LocalTimeType.LOCAL_DATE_TIME_TYPE,
                "CURRENT_TIMESTAMP(6)",
                "CURRENT_TIMESTAMP(3)",
                false);
        assertAlreadyAppliedDefault(
                BasicType.STRING_TYPE, "CURRENT_TIMESTAMP", "current_timestamp()", false);
    }

    private void assertAlreadyAppliedDefault(
            org.apache.seatunnel.api.table.type.SeaTunnelDataType type,
            String expectedDefault,
            String actualDefault,
            boolean equal)
            throws Exception {
        org.apache.seatunnel.api.table.catalog.Column source =
                PhysicalColumn.of("c", type, 32L, true, expectedDefault, null);
        org.apache.seatunnel.api.table.catalog.Column actual =
                PhysicalColumn.of("c", type, 32L, true, actualDefault, null);
        assertAlreadyAppliedColumn(source, actual, equal);
    }

    @Test
    void mysqlEnumAndSetRecoveryCompareLiteralDomains() throws Exception {
        for (String family : Arrays.asList("ENUM", "SET")) {
            org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MySqlTypeConverter
                    converter =
                            org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql
                                    .MySqlTypeConverter.DEFAULT_INSTANCE;
            org.apache.seatunnel.api.table.catalog.Column source =
                    converter.convert(
                            org.apache.seatunnel.api.table.converter.BasicTypeDefine.builder()
                                    .name("c")
                                    .dataType(family)
                                    .columnType(family + "('cat','dog')")
                                    .length(family.equals("ENUM") ? 3L : 7L)
                                    .nullable(true)
                                    .build());
            for (String domain : Arrays.asList("('Cat','dog')", "('dog','cat')", "('red','tan')")) {
                org.apache.seatunnel.api.table.catalog.Column different =
                        converter.convert(
                                org.apache.seatunnel.api.table.converter.BasicTypeDefine.builder()
                                        .name("c")
                                        .dataType(family)
                                        .columnType(family + domain)
                                        .length(family.equals("ENUM") ? 3L : 7L)
                                        .nullable(true)
                                        .build());
                assertAlreadyAppliedColumn(source, different, false);
            }
            org.apache.seatunnel.api.table.catalog.Column same =
                    converter.convert(
                            org.apache.seatunnel.api.table.converter.BasicTypeDefine.builder()
                                    .name("c")
                                    .dataType(family)
                                    .columnType(
                                            family.toLowerCase(java.util.Locale.ROOT)
                                                    + " ( 'cat' , 'dog' )")
                                    .length(family.equals("ENUM") ? 3L : 7L)
                                    .nullable(true)
                                    .build());
            assertAlreadyAppliedColumn(source, same, true);
        }
    }

    private void assertAlreadyAppliedColumn(
            org.apache.seatunnel.api.table.catalog.Column source,
            org.apache.seatunnel.api.table.catalog.Column actual,
            boolean equal)
            throws Exception {
        TableSchema before = tableSchema("id");
        TableSchema after =
                TableSchema.builder()
                        .columns(Arrays.asList(before.getColumns().get(0), source))
                        .build();
        TableSchema physical =
                TableSchema.builder()
                        .columns(Arrays.asList(before.getColumns().get(0), actual))
                        .build();
        org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent event =
                org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent.add(
                        org.apache.seatunnel.api.table.catalog.TableIdentifier.of(
                                "test", "db", null, "records"),
                        source);
        org.junit.jupiter.api.function.Executable reconcile =
                () ->
                        JdbcSchemaChangeReconciler.reconcile(
                                before,
                                after,
                                Collections.singletonList(event),
                                () -> physical,
                                ignored -> {
                                    throw new AssertionError(
                                            "Already applied or divergent DDL must not run");
                                },
                                "MySQL");
        if (equal) Assertions.assertDoesNotThrow(reconcile);
        else Assertions.assertThrows(java.io.IOException.class, reconcile);
    }

    private static TableSchema tableSchema(String... columnNames) {
        TableSchema.Builder builder = TableSchema.builder();
        for (String columnName : columnNames) {
            builder.column(
                    PhysicalColumn.of(
                            columnName, BasicType.STRING_TYPE, (Long) null, true, null, null));
        }
        return builder.build();
    }
}
