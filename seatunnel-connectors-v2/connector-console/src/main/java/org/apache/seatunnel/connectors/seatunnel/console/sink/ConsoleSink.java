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

package org.apache.seatunnel.connectors.seatunnel.console.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionRecovery;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.SchemaChangeType;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class ConsoleSink extends AbstractSimpleSink<SeaTunnelRow, Void>
        implements SupportMultiTableSink, SupportSchemaEvolutionRecovery {
    private final SeaTunnelRowType seaTunnelRowType;
    private final boolean isPrintData;
    private final int delayMs;
    private final CatalogTable catalogTable;

    public ConsoleSink(CatalogTable catalogTable, ReadonlyConfig options) {
        this.catalogTable = catalogTable;
        this.isPrintData = options.get(ConsoleSinkOptions.LOG_PRINT_DATA);
        this.delayMs = options.get(ConsoleSinkOptions.LOG_PRINT_DELAY);
        this.seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
    }

    @Override
    public ConsoleSinkWriter createWriter(SinkWriter.Context context) {
        return new ConsoleSinkWriter(seaTunnelRowType, context, isPrintData, delayMs);
    }

    @Override
    public String getPluginName() {
        return "Console";
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }

    @Override
    public void reconcileSchemaChanges(
            TableSchema before, TableSchema after, List<SchemaChangeEvent> events) {
        // Console output has no persistent receiver schema to alter.
    }

    @Override
    public List<SchemaChangeType> supports() {
        return Arrays.asList(
                SchemaChangeType.ADD_COLUMN,
                SchemaChangeType.DROP_COLUMN,
                SchemaChangeType.RENAME_COLUMN,
                SchemaChangeType.UPDATE_COLUMN);
    }
}
