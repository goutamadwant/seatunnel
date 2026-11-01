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

package org.apache.seatunnel.translation.spark.sink.writer;

import org.apache.seatunnel.api.sink.DefaultSinkWriterContext;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

class SparkDataSourceWriterTest {
    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void streamingWriterUsesDistinctCheckpointIdsForSuccessiveEpochs() throws Exception {
        SeaTunnelSink sink = Mockito.mock(SeaTunnelSink.class);
        SinkWriter writer = Mockito.mock(SinkWriter.class);
        Mockito.when(sink.createWriter(ArgumentMatchers.any())).thenReturn(writer);
        Mockito.when(sink.createCommitter()).thenReturn(Optional.empty());
        Mockito.when(writer.prepareCommit(ArgumentMatchers.anyLong())).thenReturn(Optional.empty());
        CatalogTable table =
                CatalogTableUtil.getCatalogTable(
                        "test",
                        "db",
                        "schema",
                        "rows",
                        new SeaTunnelRowType(
                                new String[] {"id"}, new SeaTunnelDataType[] {BasicType.INT_TYPE}));
        SparkDataWriterFactory factory =
                new SparkDataWriterFactory(sink, new CatalogTable[] {table}, "job", 1);
        factory.createDataWriter(0, 0, 0).commit();
        factory.createDataWriter(0, 1, 1).commit();
        Mockito.verify(writer).prepareCommit(1);
        Mockito.verify(writer).prepareCommit(2);
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void workerCommitRetryFailsInsteadOfReportingSparkSuccess() throws Exception {
        SinkWriter writer = Mockito.mock(SinkWriter.class);
        SinkCommitter committer = Mockito.mock(SinkCommitter.class);
        Mockito.when(writer.prepareCommit(1)).thenReturn(Optional.of("pending"));
        Mockito.when(committer.commit(Collections.singletonList("pending")))
                .thenReturn(Collections.singletonList("pending"));
        SparkDataWriter dataWriter =
                new SparkDataWriter(
                        writer, committer, null, 0, new DefaultSinkWriterContext("job", 0, 1));
        Assertions.assertThrows(IOException.class, dataWriter::commit);
        dataWriter.abort();
        Mockito.verify(committer).abort(Collections.singletonList("pending"));
        Mockito.verify(writer).close();
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void abortFailureStillClosesWriterAndPreservesCleanupFailure() throws Exception {
        SinkWriter writer = Mockito.mock(SinkWriter.class);
        Mockito.doThrow(new IllegalStateException("abort failure")).when(writer).abortPrepare();
        Mockito.doThrow(new IOException("close failure")).when(writer).close();
        org.apache.seatunnel.translation.spark.sink.writer.SparkDataWriter dataWriter =
                new org.apache.seatunnel.translation.spark.sink.writer.SparkDataWriter(
                        writer, null, null, 0, new DefaultSinkWriterContext("job", 0, 1));
        IOException error = Assertions.assertThrows(IOException.class, dataWriter::abort);
        Assertions.assertEquals("abort failure", error.getCause().getMessage());
        Assertions.assertEquals(1, error.getSuppressed().length);
        Mockito.verify(writer).close();
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void driverCommitRetryFailsInsteadOfAcknowledgingSourceProgress() throws Exception {
        SeaTunnelSink sink = Mockito.mock(SeaTunnelSink.class);
        SinkAggregatedCommitter committer = Mockito.mock(SinkAggregatedCommitter.class);
        Mockito.when(sink.createAggregatedCommitter()).thenReturn(Optional.of(committer));
        Mockito.when(committer.combine(Collections.singletonList("pending")))
                .thenReturn("aggregate");
        Mockito.when(committer.commit(Collections.singletonList("aggregate")))
                .thenReturn(Collections.singletonList("aggregate"));
        SparkDataSourceWriter write =
                new SparkDataSourceWriter(sink, new CatalogTable[0], "job", 1);
        SparkWriterCommitMessage message = Mockito.mock(SparkWriterCommitMessage.class);
        Mockito.when(message.getMessage()).thenReturn("pending");
        WriterCommitMessage[] messages = {message};
        Assertions.assertThrows(RuntimeException.class, () -> write.commit(messages));
        write.abort(messages);
        Mockito.verify(committer).abort(Collections.singletonList("aggregate"));
    }
}
