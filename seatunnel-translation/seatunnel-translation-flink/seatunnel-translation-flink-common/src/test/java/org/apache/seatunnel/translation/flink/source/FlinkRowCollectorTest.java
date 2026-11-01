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

package org.apache.seatunnel.translation.flink.source;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.common.metrics.Meter;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.ReaderOutput;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class FlinkRowCollectorTest {
    @Test
    void restoreDoesNotEmitDdlOrChangeTheFirstRestoredRecord() {
        MetricsContext metrics = mock(MetricsContext.class);
        Counter count = mock(Counter.class);
        Meter meter = mock(Meter.class);
        when(metrics.counter(anyString())).thenReturn(count);
        when(metrics.meter(anyString())).thenReturn(meter);
        ReaderOutput<SeaTunnelRow> output = mock(ReaderOutput.class);
        FlinkRowCollector collector = new FlinkRowCollector(ConfigFactory.empty(), metrics);
        collector.withReaderOutput(output);
        CatalogTable table = table("products", true);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "restored", 42L});
        row.setTableId(table.getTablePath().toString());
        row.setRowKind(RowKind.UPDATE_AFTER);

        collector.resetEmptyThisPollNext();
        collector.restoreSchema(Collections.singletonList(table));
        collector.restoreSchema(Collections.singletonList(table));

        assertTrue(collector.isEmptyThisPollNext());
        verifyNoInteractions(output, count, meter);
        collector.collect(row);

        ArgumentCaptor<SeaTunnelRow> first = ArgumentCaptor.forClass(SeaTunnelRow.class);
        verify(output).collect(first.capture());
        assertSame(row, first.getValue());
        assertEquals(RowKind.UPDATE_AFTER, first.getValue().getRowKind());
        assertFalse(collector.isEmptyThisPollNext());
        verify(meter).markEvent();
        assertSame(collector, collector.getCheckpointLock());
    }

    @Test
    void multiTableRestoreDoesNotBindRowsToOneSchema() {
        MetricsContext metrics = mock(MetricsContext.class);
        when(metrics.counter(anyString())).thenReturn(mock(Counter.class));
        when(metrics.meter(anyString())).thenReturn(mock(Meter.class));
        ReaderOutput<SeaTunnelRow> output = mock(ReaderOutput.class);
        FlinkRowCollector collector = new FlinkRowCollector(ConfigFactory.empty(), metrics);
        collector.withReaderOutput(output);
        CatalogTable first = table("products", true);
        CatalogTable second = table("other", false);
        collector.restoreSchema(Arrays.asList(first, second));
        verifyNoInteractions(output);

        SeaTunnelRow firstRow = new SeaTunnelRow(new Object[] {1, "first", 1234567890123L});
        firstRow.setTableId(first.getTablePath().toString());
        SeaTunnelRow secondRow = new SeaTunnelRow(new Object[] {2, "second"});
        secondRow.setTableId(second.getTablePath().toString());
        collector.collect(firstRow);
        collector.collect(secondRow);

        ArgumentCaptor<SeaTunnelRow> rows = ArgumentCaptor.forClass(SeaTunnelRow.class);
        verify(output, times(2)).collect(rows.capture());
        assertSame(firstRow, rows.getAllValues().get(0));
        assertSame(secondRow, rows.getAllValues().get(1));
    }

    @Test
    void missingCheckpointSchemaPreservesLiveRows() {
        MetricsContext metrics = mock(MetricsContext.class);
        when(metrics.counter(anyString())).thenReturn(mock(Counter.class));
        when(metrics.meter(anyString())).thenReturn(mock(Meter.class));
        ReaderOutput<SeaTunnelRow> output = mock(ReaderOutput.class);
        FlinkRowCollector collector = new FlinkRowCollector(ConfigFactory.empty(), metrics);
        collector.withReaderOutput(output);
        collector.restoreSchema(Collections.emptyList());
        SeaTunnelRow live = new SeaTunnelRow(new Object[] {3, "live", 88L, "added"});
        collector.collect(live);
        verify(output).collect(live);
        assertEquals(
                TypeInformation.of(SeaTunnelRow.class),
                new FlinkSource<>(mock(SeaTunnelSource.class), ConfigFactory.empty())
                        .getProducedType());
    }

    private CatalogTable table(String name, boolean evolved) {
        SeaTunnelRowType type =
                evolved
                        ? new SeaTunnelRowType(
                                new String[] {"id", "name", "score"},
                                new SeaTunnelDataType<?>[] {
                                    BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.LONG_TYPE
                                })
                        : new SeaTunnelRowType(
                                new String[] {"id", "name"},
                                new SeaTunnelDataType<?>[] {
                                    BasicType.INT_TYPE, BasicType.STRING_TYPE
                                });
        return CatalogTableUtil.getCatalogTable("mysql", "shop", null, name, type);
    }
}
