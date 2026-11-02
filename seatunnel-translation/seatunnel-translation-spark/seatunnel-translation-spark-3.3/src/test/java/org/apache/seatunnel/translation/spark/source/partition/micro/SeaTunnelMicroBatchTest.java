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

package org.apache.seatunnel.translation.spark.source.partition.micro;

import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.translation.spark.execution.MultiTableManager;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

class SeaTunnelMicroBatchTest {
    @Test
    void firstBatchHasANonemptyOffsetRange() {
        SeaTunnelMicroBatch stream = stream();
        assertNotEquals(stream.initialOffset().json(), stream.latestOffset().json());
    }

    @Test
    void retryAndRestartUseTheRequestedOffsetNotAFixedCheckpoint() {
        SparkSession session = mock(SparkSession.class);
        SparkContext context = mock(SparkContext.class);
        when(session.sparkContext()).thenReturn(context);
        when(context.hadoopConfiguration()).thenReturn(new Configuration());
        try (MockedStatic<SparkSession> sessions = mockStatic(SparkSession.class)) {
            sessions.when(SparkSession::getActiveSession).thenReturn(scala.Option.apply(session));
            SeaTunnelMicroBatch stream = stream();
            for (int i = 0; i < 2; i++) {
                SeaTunnelMicroBatchInputPartition partition =
                        (SeaTunnelMicroBatchInputPartition)
                                stream.planInputPartitions(
                                                SeaTunnelOffset.of(6), SeaTunnelOffset.of(7))[0];
                assertEquals(7, partition.getCheckpointId());
            }
            assertThrows(
                    IllegalArgumentException.class,
                    () -> stream.planInputPartitions(SeaTunnelOffset.of(3), SeaTunnelOffset.of(7)));
        }
    }

    @Test
    void cleanDriverRestartAdvertisesAnOffsetAfterTheRecoveredOffset() {
        SeaTunnelMicroBatch stream = stream();
        stream.deserializeOffset(SeaTunnelOffset.of(12).json());
        assertEquals(SeaTunnelOffset.of(13).json(), stream.latestOffset().json());
    }

    @Test
    void engineSuppliesTheRestoredStartBeforeDeserializingAnyOffset() {
        SeaTunnelMicroBatch stream = stream();
        assertEquals(
                SeaTunnelOffset.of(13).json(),
                stream.latestOffset(
                                SeaTunnelOffset.of(12),
                                org.apache.spark.sql.connector.read.streaming.ReadLimit
                                        .allAvailable())
                        .json());
    }

    private SeaTunnelMicroBatch stream() {
        return new SeaTunnelMicroBatch(
                mock(SeaTunnelSource.class),
                1,
                "test",
                "/tmp/unused-recovery-test",
                new CaseInsensitiveStringMap(Collections.emptyMap()),
                mock(MultiTableManager.class));
    }
}
