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

import org.apache.seatunnel.api.sink.SeaTunnelSink;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SparkCheckpointOptionsTest {
    @Test
    void rejectsIncompatibleSpark2RuntimeBeforePlanningSinkWork() {
        org.junit.jupiter.api.Assertions.assertDoesNotThrow(
                () -> SparkCheckpointOptions.validateRuntime("2.4.6", "1.8"));
        org.junit.jupiter.api.Assertions.assertDoesNotThrow(
                () -> SparkCheckpointOptions.validateRuntime("3.3.0", "11"));
        for (String javaVersion : new String[] {"11", "17", "21"})
            org.junit.jupiter.api.Assertions.assertThrows(
                    UnsupportedOperationException.class,
                    () -> SparkCheckpointOptions.validateRuntime("2.4.6", javaVersion));
    }

    @Test
    void rejectsRequiredWriterStateBeforeStartingAQuery() {
        SeaTunnelSink<?, ?, ?, ?> sink = mock(SeaTunnelSink.class);
        when(sink.requiresWriterState()).thenReturn(true);
        UnsupportedOperationException failure =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> SparkCheckpointOptions.validateRecoverySink(sink));
        assertTrue(failure.getMessage().contains("sink writer state"));
    }

    @Test
    void permitsStatelessWriterRecovery() {
        SeaTunnelSink<?, ?, ?, ?> sink = mock(SeaTunnelSink.class);
        when(sink.requiresWriterState()).thenReturn(false);
        assertDoesNotThrow(() -> SparkCheckpointOptions.validateRecoverySink(sink));
    }
}
