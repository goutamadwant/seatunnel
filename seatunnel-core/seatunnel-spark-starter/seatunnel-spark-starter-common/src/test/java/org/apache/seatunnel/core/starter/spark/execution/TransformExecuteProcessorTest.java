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

package org.apache.seatunnel.core.starter.spark.execution;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelMapTransform;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.translation.spark.execution.DatasetTableInfo;

import org.apache.spark.TaskContext;
import org.apache.spark.util.TaskCompletionListener;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class TransformExecuteProcessorTest {

    @Test
    void schemaBoundaryUsesCompoundRoutingForUnqualifiedAdd() throws Exception {
        CatalogTable before = compoundRoutingTable("db", "id");
        CatalogTable after = compoundRoutingTable("db", "id", "score");
        TableIdentifier unqualified = TableIdentifier.of("test", null, null, "records");
        AlterTableColumnsEvent block =
                compoundRoutingBlock(
                        before,
                        after,
                        AlterTableAddColumnEvent.add(
                                unqualified, after.getTableSchema().getColumns().get(1)));
        String journal = SerializationUtils.objectToString(block);

        List<SchemaChangeEvent> mapped =
                SparkSchemaPlan.mapChanges(
                        compoundRoutingPlan(before), compoundRoutingPlan(after),
                        Collections.singletonList(block), Collections.singletonList(before));

        Assertions.assertEquals(1, mapped.size());
        AlterTableColumnsEvent result = (AlterTableColumnsEvent) mapped.get(0);
        Assertions.assertEquals(before.getTablePath(), result.tablePath());
        Assertions.assertEquals(1, result.getEvents().size());
        Assertions.assertEquals(unqualified.toTablePath(), result.getEvents().get(0).tablePath());
        Assertions.assertEquals("ALTER TABLE records", result.getStatement());
        Assertions.assertEquals("MySQL", result.getSourceDialectName());
        Assertions.assertEquals(journal, SerializationUtils.objectToString(block));
    }

    @Test
    void schemaBoundaryKeepsUnqualifiedCompoundRenameChainAndDefault() throws Exception {
        CatalogTable before = compoundRoutingTable("db", "id");
        CatalogTable temporary = compoundRoutingDefaultTable("tmp");
        CatalogTable middle = compoundRoutingDefaultTable("mid");
        CatalogTable after = compoundRoutingDefaultTable("final");
        TableIdentifier unqualified = TableIdentifier.of("test", null, null, "records");
        Column defaultColumn = PhysicalColumn.of("tmp", BasicType.INT_TYPE, 0, true, "42", null);
        List<SchemaChangeEvent> events =
                java.util.Arrays.asList(
                        compoundRoutingBlock(
                                before,
                                temporary,
                                AlterTableAddColumnEvent.add(unqualified, defaultColumn)),
                        compoundRoutingBlock(
                                temporary,
                                middle,
                                AlterTableChangeColumnEvent.change(
                                        unqualified,
                                        "tmp",
                                        middle.getTableSchema().getColumns().get(1))),
                        compoundRoutingBlock(
                                middle,
                                after,
                                AlterTableChangeColumnEvent.change(
                                        unqualified,
                                        "mid",
                                        after.getTableSchema().getColumns().get(1))));
        String journal = SerializationUtils.objectToString(new java.util.ArrayList<>(events));

        List<SchemaChangeEvent> mapped =
                SparkSchemaPlan.mapChanges(
                        compoundRoutingPlan(before),
                        compoundRoutingPlan(after),
                        events,
                        Collections.singletonList(before));

        Assertions.assertEquals(3, mapped.size());
        for (SchemaChangeEvent event : mapped) {
            Assertions.assertEquals(before.getTablePath(), event.tablePath());
            Assertions.assertEquals(1, ((AlterTableColumnsEvent) event).getEvents().size());
        }
        AlterTableAddColumnEvent add =
                (AlterTableAddColumnEvent)
                        ((AlterTableColumnsEvent) mapped.get(0)).getEvents().get(0);
        Assertions.assertEquals("tmp", add.getColumn().getName());
        Assertions.assertEquals("42", add.getColumn().getDefaultValue());
        Assertions.assertEquals(
                "tmp",
                ((AlterTableChangeColumnEvent)
                                ((AlterTableColumnsEvent) mapped.get(1)).getEvents().get(0))
                        .getOldColumn());
        Assertions.assertEquals(
                "mid",
                ((AlterTableChangeColumnEvent)
                                ((AlterTableColumnsEvent) mapped.get(2)).getEvents().get(0))
                        .getOldColumn());
        Assertions.assertEquals(
                journal, SerializationUtils.objectToString(new java.util.ArrayList<>(events)));
    }

    @Test
    void schemaBoundaryDoesNotBorrowSiblingRelevanceFromCompoundChildRouting() throws Exception {
        CatalogTable left = compoundRoutingTable("left_db", "id");
        CatalogTable leftAfter = compoundRoutingTable("left_db", "id", "score");
        CatalogTable right = compoundRoutingTable("right_db", "id", "unused");
        // The right block's child points to left_db, whose endpoint consumes score.
        // Its own parent only consumes unused, so score must not borrow relevance.
        AlterTableColumnsEvent consumed =
                compoundRoutingBlock(
                        left,
                        leftAfter,
                        AlterTableAddColumnEvent.add(
                                right.getTableId(),
                                leftAfter.getTableSchema().getColumns().get(1)));
        AlterTableColumnsEvent ignored =
                compoundRoutingBlock(
                        right,
                        right,
                        AlterTableAddColumnEvent.add(
                                left.getTableId(), leftAfter.getTableSchema().getColumns().get(1)),
                        new AlterTableDropColumnEvent(left.getTableId(), "score"));
        List<SchemaChangeEvent> events = java.util.Arrays.asList(consumed, ignored);
        String journal = SerializationUtils.objectToString(new java.util.ArrayList<>(events));
        List<SchemaChangeEvent> mapped =
                SparkSchemaPlan.mapChanges(
                        compoundRoutingPlan(left, right),
                        compoundRoutingPlan(leftAfter, right),
                        events,
                        java.util.Arrays.asList(left, right));
        Assertions.assertEquals(1, mapped.size());
        Assertions.assertEquals(left.getTablePath(), mapped.get(0).tablePath());
        Assertions.assertEquals(
                journal, SerializationUtils.objectToString(new java.util.ArrayList<>(events)));
    }

    private static CatalogTable compoundRoutingTable(String database, String... names) {
        SeaTunnelDataType<?>[] types = new SeaTunnelDataType<?>[names.length];
        java.util.Arrays.fill(types, BasicType.INT_TYPE);
        return CatalogTableUtil.getCatalogTable(
                "test", database, "schema", "records", new SeaTunnelRowType(names, types));
    }

    private static CatalogTable compoundRoutingDefaultTable(String name) {
        CatalogTable base = compoundRoutingTable("db", "id");
        return CatalogTable.of(
                base.getTableId(),
                TableSchema.builder()
                        .column(base.getTableSchema().getColumns().get(0))
                        .column(PhysicalColumn.of(name, BasicType.INT_TYPE, 0, true, "42", null))
                        .build(),
                base.getOptions(),
                base.getPartitionKeys(),
                base.getComment());
    }

    private static DatasetTableInfo compoundRoutingPlan(CatalogTable... tables) {
        return new DatasetTableInfo(
                null, java.util.Arrays.asList(tables), "records", 0, Collections.emptyList());
    }

    private static AlterTableColumnsEvent compoundRoutingBlock(
            CatalogTable before, CatalogTable after, AlterTableColumnEvent... children) {
        AlterTableColumnsEvent block =
                new AlterTableColumnsEvent(before.getTableId(), java.util.Arrays.asList(children));
        block.setChangeAfter(after);
        block.setStatement("ALTER TABLE records");
        block.setSourceDialectName("MySQL");
        return block;
    }

    @Test
    void typedPlanKeepsCatalogSnapshotWhenTransformRefreshesItsProducedList() {
        java.util.List<CatalogTable> mutable =
                new java.util.ArrayList<>(Collections.singletonList(boundaryTable(false)));
        org.apache.seatunnel.translation.spark.execution.DatasetTableInfo captured =
                new org.apache.seatunnel.translation.spark.execution.DatasetTableInfo(
                        null, mutable, "records");
        mutable.clear();
        mutable.add(boundaryTable(true));
        Assertions.assertEquals(
                1, captured.getCatalogTables().get(0).getTableSchema().getColumns().size());
    }

    @Test
    void schemaBoundaryRebuildMapsTableAndFieldRenameWithoutMutatingJournal() throws Exception {
        org.apache.seatunnel.translation.spark.execution.DatasetTableInfo before =
                renamedPlan(false);
        org.apache.seatunnel.translation.spark.execution.DatasetTableInfo after = renamedPlan(true);
        CatalogTable sourceBefore = boundaryTable(false);
        CatalogTable sourceAfter = boundaryTable(true);
        org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent event =
                org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent.add(
                        sourceBefore.getTableId(),
                        sourceAfter.getTableSchema().getColumns().get(1));
        event.setChangeAfter(sourceAfter);
        List<org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent> mapped =
                SparkSchemaPlan.mapChanges(
                        before,
                        after,
                        Collections.singletonList(event),
                        Collections.singletonList(sourceBefore));
        Assertions.assertEquals("db.schema.dst_records", mapped.get(0).tablePath().getFullName());
        Assertions.assertEquals(
                java.util.Arrays.asList("f_id", "f_name"),
                mapped.get(0).getChangeAfter().getTableSchema().getColumns().stream()
                        .map(org.apache.seatunnel.api.table.catalog.Column::getName)
                        .collect(java.util.stream.Collectors.toList()));
        Assertions.assertEquals("name", event.getColumn().getName());
        Assertions.assertEquals(sourceBefore.getTablePath(), event.tablePath());
    }

    @Test
    void schemaBoundaryFiltersOnlyUnconsumedChildrenOfCompoundDdl() throws Exception {
        CatalogTable sourceBefore = boundaryTable(false);
        CatalogTable sourceAfter =
                org.apache.seatunnel.api.table.catalog.CatalogTableUtil.getCatalogTable(
                        "test",
                        "db",
                        "schema",
                        "records",
                        new org.apache.seatunnel.api.table.type.SeaTunnelRowType(
                                new String[] {"id", "name"},
                                new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                                    org.apache.seatunnel.api.table.type.BasicType.LONG_TYPE,
                                    org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE
                                }));
        org.apache.seatunnel.api.configuration.ReadonlyConfig config =
                org.apache.seatunnel.api.configuration.ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                "include_fields", Collections.singletonList("id")));
        org.apache.seatunnel.api.transform.SeaTunnelTransform oldTransform =
                new org.apache.seatunnel.transform.filter.FilterFieldMultiCatalogTransform(
                        Collections.singletonList(sourceBefore), config);
        org.apache.seatunnel.api.transform.SeaTunnelTransform newTransform =
                new org.apache.seatunnel.transform.filter.FilterFieldMultiCatalogTransform(
                        Collections.singletonList(sourceAfter), config);
        org.apache.seatunnel.translation.spark.execution.DatasetTableInfo before =
                new org.apache.seatunnel.translation.spark.execution.DatasetTableInfo(
                        null,
                        oldTransform.getProducedCatalogTables(),
                        "filtered",
                        0,
                        Collections.singletonList(oldTransform));
        org.apache.seatunnel.translation.spark.execution.DatasetTableInfo after =
                new org.apache.seatunnel.translation.spark.execution.DatasetTableInfo(
                        null,
                        newTransform.getProducedCatalogTables(),
                        "filtered",
                        0,
                        Collections.singletonList(newTransform));
        org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent add =
                org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent.add(
                        sourceBefore.getTableId(),
                        sourceAfter.getTableSchema().getColumns().get(1));
        org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent modify =
                org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent.modify(
                        sourceBefore.getTableId(),
                        sourceAfter.getTableSchema().getColumns().get(0));
        org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent block =
                new org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent(
                        sourceBefore.getTableId(), java.util.Arrays.asList(add, modify));
        block.setChangeAfter(sourceAfter);
        List<org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent> mapped =
                SparkSchemaPlan.mapChanges(
                        before,
                        after,
                        Collections.singletonList(block),
                        Collections.singletonList(sourceBefore));
        List<org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent> children =
                ((org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent) mapped.get(0))
                        .getEvents();
        Assertions.assertEquals(1, children.size());
        Assertions.assertInstanceOf(
                org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent.class,
                children.get(0));
        Assertions.assertEquals(2, block.getEvents().size());
    }

    private CatalogTable boundaryTable(boolean after) {
        return org.apache.seatunnel.api.table.catalog.CatalogTableUtil.getCatalogTable(
                "test",
                "db",
                "schema",
                "records",
                after
                        ? new org.apache.seatunnel.api.table.type.SeaTunnelRowType(
                                new String[] {"id", "name"},
                                new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                                    org.apache.seatunnel.api.table.type.BasicType.INT_TYPE,
                                    org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE
                                })
                        : new org.apache.seatunnel.api.table.type.SeaTunnelRowType(
                                new String[] {"id"},
                                new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                                    org.apache.seatunnel.api.table.type.BasicType.INT_TYPE
                                }));
    }

    private org.apache.seatunnel.translation.spark.execution.DatasetTableInfo renamedPlan(
            boolean after) {
        org.apache.seatunnel.api.transform.SeaTunnelTransform table =
                new org.apache.seatunnel.transform.rename.TableRenameMultiCatalogTransform(
                        Collections.singletonList(boundaryTable(after)),
                        org.apache.seatunnel.api.configuration.ReadonlyConfig.fromMap(
                                Collections.singletonMap("prefix", "dst_")));
        org.apache.seatunnel.api.transform.SeaTunnelTransform fields =
                new org.apache.seatunnel.transform.rename.FieldRenameMultiCatalogTransform(
                        table.getProducedCatalogTables(),
                        org.apache.seatunnel.api.configuration.ReadonlyConfig.fromMap(
                                Collections.singletonMap("prefix", "f_")));
        return new org.apache.seatunnel.translation.spark.execution.DatasetTableInfo(
                null,
                fields.getProducedCatalogTables(),
                "renamed",
                0,
                java.util.Arrays.asList(table, fields));
    }

    @Test
    void opensOnceAndClosesOnceOnTaskCompletion() {
        LifecycleTracker tracker = new LifecycleTracker();
        TransformExecuteProcessor.TransformMapPartitionsFunction function =
                createFunction(new TrackingMapTransform(tracker));
        TaskContext taskContext = mock(TaskContext.class);
        ArgumentCaptor<TaskCompletionListener> listener = registerCompletionListener(taskContext);

        function.initialize(taskContext);
        function.initialize(taskContext);

        Assertions.assertEquals(1, tracker.openCount);
        verify(taskContext, times(1)).addTaskCompletionListener(listener.capture());

        listener.getValue().onTaskCompletion(taskContext);
        listener.getValue().onTaskCompletion(taskContext);
        Assertions.assertEquals(1, tracker.closeCount);
    }

    @Test
    void completionListenerSuppressesCloseFailure() {
        LifecycleTracker tracker = new LifecycleTracker();
        tracker.failOnClose = true;
        TransformExecuteProcessor.TransformMapPartitionsFunction function =
                createFunction(new TrackingMapTransform(tracker));
        TaskContext taskContext = mock(TaskContext.class);
        ArgumentCaptor<TaskCompletionListener> listener = registerCompletionListener(taskContext);

        function.initialize(taskContext);
        verify(taskContext).addTaskCompletionListener(listener.capture());

        Assertions.assertDoesNotThrow(() -> listener.getValue().onTaskCompletion(taskContext));
        Assertions.assertEquals(1, tracker.closeCount);
    }

    @Test
    void registersCleanupBeforeOpeningTransform() {
        LifecycleTracker tracker = new LifecycleTracker();
        tracker.failOnOpen = true;
        TransformExecuteProcessor.TransformMapPartitionsFunction function =
                createFunction(new TrackingMapTransform(tracker));
        TaskContext taskContext = mock(TaskContext.class);
        ArgumentCaptor<TaskCompletionListener> listener = registerCompletionListener(taskContext);

        Assertions.assertThrows(RuntimeException.class, () -> function.initialize(taskContext));
        verify(taskContext).addTaskCompletionListener(listener.capture());

        listener.getValue().onTaskCompletion(taskContext);
        Assertions.assertEquals(1, tracker.openCount);
        Assertions.assertEquals(1, tracker.closeCount);
    }

    private static TransformExecuteProcessor.TransformMapPartitionsFunction createFunction(
            SeaTunnelMapTransform<SeaTunnelRow> transform) {
        return new TransformExecuteProcessor.TransformMapPartitionsFunction(transform, null, null);
    }

    private static ArgumentCaptor<TaskCompletionListener> registerCompletionListener(
            TaskContext taskContext) {
        ArgumentCaptor<TaskCompletionListener> listener =
                ArgumentCaptor.forClass(TaskCompletionListener.class);
        doReturn(taskContext).when(taskContext).addTaskCompletionListener(listener.capture());
        return listener;
    }

    private org.apache.seatunnel.translation.spark.execution.DatasetTableInfo plan(
            SeaTunnelMapTransform<SeaTunnelRow>... transforms) {
        return new org.apache.seatunnel.translation.spark.execution.DatasetTableInfo(
                null,
                Collections.emptyList(),
                "branch",
                0,
                new java.util.ArrayList<>(java.util.Arrays.asList(transforms)));
    }

    @Test
    void selectingABranchClosesUnusedTransformsAndRetainsSharedOwners() {
        LifecycleTracker shared = new LifecycleTracker(),
                used = new LifecycleTracker(),
                unused = new LifecycleTracker();
        TrackingMapTransform common = new TrackingMapTransform(shared);
        org.apache.seatunnel.translation.spark.execution.DatasetTableInfo chosen =
                plan(common, new TrackingMapTransform(used));
        org.apache.seatunnel.translation.spark.execution.DatasetTableInfo other =
                plan(common, new TrackingMapTransform(unused));
        Assertions.assertSame(
                chosen,
                SparkSchemaPlan.selectPlan(
                        java.util.Arrays.asList(chosen, other), plans -> plans.get(0)));
        Assertions.assertEquals(1, unused.closeCount);
        Assertions.assertEquals(0, shared.closeCount);
        Assertions.assertEquals(0, used.closeCount);
        SparkSchemaPlan.closePlans(null, chosen);
        Assertions.assertEquals(1, shared.closeCount);
        Assertions.assertEquals(1, used.closeCount);
    }

    @Test
    void failedUnusedBranchCleanupAlsoReleasesTheSelectedBranch() {
        LifecycleTracker used = new LifecycleTracker(), unused = new LifecycleTracker();
        unused.failOnClose = true;
        Assertions.assertThrows(
                RuntimeException.class,
                () ->
                        SparkSchemaPlan.selectPlan(
                                java.util.Arrays.asList(
                                        plan(new TrackingMapTransform(used)),
                                        plan(new TrackingMapTransform(unused))),
                                plans -> plans.get(0)));
        Assertions.assertEquals(1, used.closeCount);
        Assertions.assertEquals(1, unused.closeCount);
    }

    @Test
    void schemaPlanCleanupKeepsTheOriginalFailureAndClosesAllOwnersOnce() {
        LifecycleTracker first = new LifecycleTracker(), second = new LifecycleTracker();
        first.failOnClose = true;
        second.failOnClose = true;
        TrackingMapTransform shared = new TrackingMapTransform(first);
        IllegalStateException expected = new IllegalStateException("mapping failed");
        SparkSchemaPlan.closePlans(
                expected, plan(shared), plan(shared, new TrackingMapTransform(second)));
        Assertions.assertEquals(1, first.closeCount);
        Assertions.assertEquals(1, second.closeCount);
        Assertions.assertEquals(2, expected.getSuppressed().length);
    }

    private static class LifecycleTracker {
        private int openCount;
        private int closeCount;
        private boolean failOnOpen;
        private boolean failOnClose;
    }

    private static class TrackingMapTransform implements SeaTunnelMapTransform<SeaTunnelRow> {
        private final LifecycleTracker tracker;

        private TrackingMapTransform(LifecycleTracker tracker) {
            this.tracker = tracker;
        }

        @Override
        public String getPluginName() {
            return "lifecycle-test";
        }

        @Override
        public void open() {
            tracker.openCount++;
            if (tracker.failOnOpen) {
                throw new RuntimeException("expected open failure");
            }
        }

        @Override
        public SeaTunnelRow map(SeaTunnelRow row) {
            return row;
        }

        @Override
        public CatalogTable getProducedCatalogTable() {
            return null;
        }

        @Override
        public List<CatalogTable> getProducedCatalogTables() {
            return Collections.emptyList();
        }

        @Override
        public void close() {
            tracker.closeCount++;
            if (tracker.failOnClose) {
                throw new RuntimeException("expected close failure");
            }
        }
    }
}
