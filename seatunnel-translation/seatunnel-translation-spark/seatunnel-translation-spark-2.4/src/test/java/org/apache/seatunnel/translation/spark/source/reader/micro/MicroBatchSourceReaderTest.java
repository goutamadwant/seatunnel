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

package org.apache.seatunnel.translation.spark.source.reader.micro;

import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.translation.spark.source.SparkMicroBatchReader;
import org.apache.seatunnel.translation.spark.source.state.MicroBatchState;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MicroBatchSourceReaderTest {
    @Test
    void restoredOffsetsCompareCheckpointValues() {
        MicroBatchSourceReader reader =
                new MicroBatchSourceReader(
                        null,
                        1,
                        "job",
                        0,
                        100,
                        "file:///tmp/unused",
                        "file:///",
                        "",
                        Collections.emptyMap(),
                        null);
        MicroBatchState start =
                (MicroBatchState)
                        reader.deserializeOffset(
                                SerializationUtils.objectToString(new MicroBatchState(2)));
        MicroBatchState end =
                (MicroBatchState)
                        reader.deserializeOffset(
                                SerializationUtils.objectToString(new MicroBatchState(3)));
        reader.setOffsetRange(Optional.of(start), Optional.of(end));
        assertEquals(3, reader.checkpointId.intValue());
        reader.setOffsetRange(
                Optional.of(new MicroBatchState(1000)), Optional.of(new MicroBatchState(1001)));
        assertEquals(1001, reader.checkpointId.intValue());
    }

    @Test
    void commitCancellationIsRecognizedBySpark2() {
        try {
            assertTrue(
                    org.apache.spark.sql.execution.streaming.StreamExecution
                            .isInterruptionException(
                                    SparkMicroBatchReader.checkpointFailure(
                                            new InterruptedException("stop"))));
        } finally {
            Thread.interrupted();
        }
    }
}
