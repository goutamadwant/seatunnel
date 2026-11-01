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

package org.apache.seatunnel.translation.spark.source;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Base64;

class SparkCheckpointOffsetTest {
    private static final String LEGACY_ONE =
            "rO0ABXNyAENvcmcuYXBhY2hlLnNlYXR1bm5lbC50cmFuc2xhdGlvbi5zcGFyay5zb3VyY2Uuc3RhdGUuTWljcm9CYXRjaFN0YXRldYAwecJuxNACAAFMAAxjaGVja3BvaW50SWR0ABNMamF2YS9sYW5nL0ludGVnZXI7eHBzcgARamF2YS5sYW5nLkludGVnZXIS4qCk94GHOAIAAUkABXZhbHVleHIAEGphdmEubGFuZy5OdW1iZXKGrJUdC5TgiwIAAHhwAAAAAQ==";

    @Test
    void jsonRoundTripsWithoutEngineSpecificSerializedClasses() throws Exception {
        for (long id : new long[] {0, 1, Integer.MAX_VALUE, Long.MAX_VALUE})
            Assertions.assertEquals(
                    id, SparkCheckpointOffset.checkpointId(SparkCheckpointOffset.json(id)));
    }

    @Test
    void oldSpark2OffsetRemainsReadableWithoutJavaDeserialization() throws Exception {
        Assertions.assertEquals(1, SparkCheckpointOffset.checkpointId(LEGACY_ONE));
    }

    @Test
    void malformedAndUnknownSerializedOffsetsFailClosed() {
        for (String input :
                new String[] {
                    "{}",
                    "{\"checkpointId\":1,\"checkpointId\":2}",
                    "{\"checkpointId\":1} {}",
                    "{\"checkpointId\":9223372036854775808}",
                    "{\"checkpointId\":-1}",
                    "{\"checkpointId\":1.5}",
                    "{\"checkpointId\":\"1\"}",
                    "not-base64",
                    LEGACY_ONE.substring(1)
                })
            Assertions.assertThrows(
                    IOException.class, () -> SparkCheckpointOffset.checkpointId(input));
        byte[] wrongClass = Base64.getDecoder().decode(LEGACY_ONE);
        wrongClass[10] ^= 1;
        Assertions.assertThrows(
                IOException.class,
                () ->
                        SparkCheckpointOffset.checkpointId(
                                Base64.getEncoder().encodeToString(wrongClass)));
        byte[] trailing =
                java.util.Arrays.copyOf(
                        Base64.getDecoder().decode(LEGACY_ONE),
                        Base64.getDecoder().decode(LEGACY_ONE).length + 1);
        Assertions.assertThrows(
                IOException.class,
                () ->
                        SparkCheckpointOffset.checkpointId(
                                Base64.getEncoder().encodeToString(trailing)));
    }
}
