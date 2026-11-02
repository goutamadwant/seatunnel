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
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.translation.spark.execution.DatasetTableInfo;
import org.apache.seatunnel.translation.spark.source.SparkSourceCatalog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/** Replays schema mapping on a driver-owned old plan before reconciling the fresh typed plan. */
final class SparkSchemaPlan {
    static List<SchemaChangeEvent> mapChanges(
            DatasetTableInfo oldPlan,
            DatasetTableInfo newPlan,
            List<SchemaChangeEvent> sourceChanges,
            List<CatalogTable> sourceBefore)
            throws IOException {
        List<SchemaChangeEvent> mapped = new ArrayList<>();
        List<SeaTunnelTransform> transforms = oldPlan.getSchemaTransforms();
        List<CatalogTable> currentSource = new ArrayList<>(sourceBefore);
        for (SchemaChangeEvent source : sourceChanges) {
            SchemaChangeEvent event =
                    SerializationUtils.deserialize(SerializationUtils.serialize(source));
            if (event.getChangeAfter() == null)
                throw new IOException("Schema boundary has no authoritative post-event catalog");
            boolean found = false;
            for (int i = 0; i < currentSource.size(); i++) {
                if (currentSource.get(i).getTablePath().equals(event.tablePath())) {
                    currentSource.set(i, event.getChangeAfter());
                    found = true;
                    break;
                }
            }
            if (!found) throw new IOException("Schema boundary changed an unknown source routing");
            List<CatalogTable> inputAfter = new ArrayList<>(currentSource);
            for (SeaTunnelTransform transform : transforms) {
                // Map names against the old plan, then adopt the authoritative upstream layout.
                // A fresh query owns row conversion, so no live task sees this driver-only refresh.
                if (event != null) event = transform.mapSchemaChangeEvent(event);
                transform.setInputCatalogTables(inputAfter);
                inputAfter = transform.getProducedCatalogTables();
                if (event != null) {
                    CatalogTable produced = null;
                    for (CatalogTable table : inputAfter) {
                        if (table.getTablePath().equals(event.tablePath())) {
                            produced = table;
                            break;
                        }
                    }
                    if (produced == null)
                        throw new IOException("Mapped schema event lost its output routing");
                    event.setChangeAfter(produced);
                }
            }
            if (event != null) mapped.add(event);
        }
        List<CatalogTable> recovered =
                transforms.isEmpty()
                        ? currentSource
                        : transforms.get(transforms.size() - 1).getProducedCatalogTables();
        if (!SparkSourceCatalog.sameSchemas(recovered, newPlan.getCatalogTables())) {
            throw new IOException("Recovered transform schema differs from the rebuilt Spark plan");
        }
        return filterUnconsumedColumns(
                mapped, oldPlan.getCatalogTables(), newPlan.getCatalogTables());
    }

    private static List<SchemaChangeEvent> filterUnconsumedColumns(
            List<SchemaChangeEvent> events, List<CatalogTable> before, List<CatalogTable> after) {
        java.util.Map<org.apache.seatunnel.api.table.catalog.TablePath, Set<String>> relevant =
                new java.util.HashMap<>();
        List<CatalogTable> tables = new ArrayList<>(before);
        tables.addAll(after);
        for (CatalogTable table : tables) {
            Set<String> names =
                    relevant.computeIfAbsent(
                            table.getTablePath(), ignored -> new java.util.HashSet<>());
            table.getTableSchema().getColumns().forEach(column -> names.add(column.getName()));
        }
        List<SchemaChangeEvent> flat = new ArrayList<>();
        java.util.Map<SchemaChangeEvent, TablePath> routing = new IdentityHashMap<>();
        for (SchemaChangeEvent event : events) {
            if (event
                    instanceof org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent) {
                for (SchemaChangeEvent child :
                        ((org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent) event)
                                .getEvents()) {
                    flat.add(child);
                    // Parsed children can have unqualified SQL names. The enclosing DDL
                    // block owns their routing through every transform and sink.
                    routing.put(child, event.tablePath());
                }
            } else {
                flat.add(event);
                routing.put(event, event.tablePath());
            }
        }
        // Keep complete rename chains that lead to an output column, including intermediate
        // names absent from both endpoint catalogs. Otherwise an ADD/default/RENAME sequence
        // could be collapsed into a different physical data transformation.
        boolean changed;
        do {
            changed = false;
            for (SchemaChangeEvent event : flat) {
                if (!(event
                        instanceof
                        org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent))
                    continue;
                Set<String> names = relevant.get(routing.get(event));
                Set<String> touched = changedColumns(event);
                if (names != null && touched.stream().anyMatch(names::contains))
                    changed |= names.addAll(touched);
            }
        } while (changed);
        List<SchemaChangeEvent> filtered = new ArrayList<>();
        for (SchemaChangeEvent event : events) {
            if (event
                    instanceof org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent) {
                org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent block =
                        (org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent) event;
                java.util.List<org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent>
                        children = new ArrayList<>();
                for (org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent child :
                        block.getEvents()) {
                    if (consumedColumnChange(child, relevant, block.tablePath()))
                        children.add(child);
                }
                if (!children.isEmpty()) {
                    org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent retained =
                            new org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent(
                                    block.tableIdentifier(), children);
                    retained.setChangeAfter(block.getChangeAfter());
                    retained.setSourceDialectName(block.getSourceDialectName());
                    retained.setStatement(block.getStatement());
                    retained.setJobId(block.getJobId());
                    filtered.add(retained);
                }
            } else if (consumedColumnChange(event, relevant, event.tablePath()))
                filtered.add(event);
        }
        return filtered;
    }

    private static boolean consumedColumnChange(
            SchemaChangeEvent event,
            java.util.Map<org.apache.seatunnel.api.table.catalog.TablePath, Set<String>> relevant,
            TablePath routing) {
        Set<String> columns = changedColumns(event);
        if (columns == null) return true;
        Set<String> consumed = relevant.get(routing);
        return consumed != null && columns.stream().anyMatch(consumed::contains);
    }

    private static Set<String> changedColumns(SchemaChangeEvent event) {
        Set<String> names = new java.util.HashSet<>();
        if (event instanceof org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent)
            names.add(
                    ((org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent) event)
                            .getColumn()
                            .getName());
        else if (event
                instanceof org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent)
            names.add(
                    ((org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent)
                                    event)
                            .getColumn()
                            .getName());
        else if (event
                instanceof org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent)
            names.add(
                    ((org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent) event)
                            .getColumn());
        else if (event
                instanceof
                org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent) {
            org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent change =
                    (org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent) event;
            names.add(change.getOldColumn());
            names.add(change.getColumn().getName());
        } else return null;
        return names;
    }

    static DatasetTableInfo selectPlan(
            List<DatasetTableInfo> plans,
            Function<List<DatasetTableInfo>, DatasetTableInfo> select) {
        DatasetTableInfo chosen;
        try {
            chosen = select.apply(plans);
        } catch (RuntimeException | Error error) {
            closePlans(error, plans.toArray(new DatasetTableInfo[0]));
            throw error;
        }
        Set<SeaTunnelTransform> retained = Collections.newSetFromMap(new IdentityHashMap<>());
        retained.addAll(chosen.getSchemaTransforms());
        List<SeaTunnelTransform> unused = new ArrayList<>();
        for (DatasetTableInfo plan : plans) {
            for (SeaTunnelTransform transform : plan.getSchemaTransforms()) {
                if (!retained.contains(transform)) unused.add(transform);
            }
        }
        try {
            closePlans(null, new DatasetTableInfo(null, Collections.emptyList(), "", -1, unused));
        } catch (RuntimeException | Error error) {
            closePlans(error, chosen);
            throw error;
        }
        return chosen;
    }

    static void closePlans(Throwable primary, DatasetTableInfo... plans) {
        Set<SeaTunnelTransform> closed = Collections.newSetFromMap(new IdentityHashMap<>());
        Throwable cleanup = null;
        for (DatasetTableInfo plan : plans) {
            if (plan == null) continue;
            List<SeaTunnelTransform> transforms = plan.getSchemaTransforms();
            for (int i = transforms.size() - 1; i >= 0; i--) {
                SeaTunnelTransform transform = transforms.get(i);
                if (!closed.add(transform)) continue;
                try {
                    transform.close();
                } catch (RuntimeException | Error error) {
                    if (primary != null) {
                        if (primary != error) primary.addSuppressed(error);
                    } else if (cleanup == null) cleanup = error;
                    else if (cleanup != error) cleanup.addSuppressed(error);
                }
            }
        }
        if (cleanup instanceof Error) throw (Error) cleanup;
        if (cleanup != null) throw (RuntimeException) cleanup;
    }

    private SparkSchemaPlan() {}
}
