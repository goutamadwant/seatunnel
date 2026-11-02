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

import org.apache.seatunnel.common.utils.JsonUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Stable JSON offsets with a strict, non-deserializing decoder for the old Spark 2 format. */
public final class SparkCheckpointOffset {
    // The original MicroBatchState stream has one Integer field and a fixed class descriptor.
    // Match its complete prefix; never deserialize arbitrary objects from an offset string.
    private static final byte[] LEGACY_ZERO =
            Base64.getDecoder()
                    .decode(
                            "rO0ABXNyAENvcmcuYXBhY2hlLnNlYXR1bm5lbC50cmFuc2xhdGlvbi5zcGFyay5zb3VyY2Uuc3RhdGUuTWljcm9CYXRjaFN0YXRldYAwecJuxNACAAFMAAxjaGVja3BvaW50SWR0ABNMamF2YS9sYW5nL0ludGVnZXI7eHBzcgARamF2YS5sYW5nLkludGVnZXIS4qCk94GHOAIAAUkABXZhbHVleHIAEGphdmEubGFuZy5OdW1iZXKGrJUdC5TgiwIAAHhwAAAAAA==");

    private static final Pattern JSON_OFFSET =
            Pattern.compile("\\s*\\{\\s*\"checkpointId\"\\s*:\\s*(0|[1-9][0-9]*)\\s*}\\s*");

    public static String json(long checkpointId) {
        if (checkpointId < 0) throw new IllegalArgumentException("Negative Spark checkpoint id");
        return JsonUtils.toJsonString(Collections.singletonMap("checkpointId", checkpointId));
    }

    public static long checkpointId(String encoded) throws IOException {
        if (encoded == null || encoded.length() > 4096)
            throw new IOException("Invalid SeaTunnel source offset");
        long id;
        if (encoded.trim().startsWith("{")) {
            Matcher value = JSON_OFFSET.matcher(encoded);
            if (!value.matches())
                throw new IOException("Invalid SeaTunnel source checkpoint offset");
            try {
                id = Long.parseLong(value.group(1));
            } catch (NumberFormatException error) {
                throw new IOException("Invalid SeaTunnel source checkpoint offset", error);
            }
        } else {
            byte[] bytes;
            try {
                bytes = Base64.getDecoder().decode(encoded);
            } catch (IllegalArgumentException error) {
                throw new IOException("Invalid legacy Spark source offset", error);
            }
            if (bytes.length != LEGACY_ZERO.length
                    || !Arrays.equals(
                            Arrays.copyOf(bytes, bytes.length - 4),
                            Arrays.copyOf(LEGACY_ZERO, LEGACY_ZERO.length - 4)))
                throw new IOException("Unsupported legacy Spark source offset descriptor");
            id = ByteBuffer.wrap(bytes, bytes.length - 4, 4).getInt();
        }
        if (id < 0) throw new IOException("Invalid SeaTunnel source checkpoint id");
        return id;
    }

    private SparkCheckpointOffset() {}
}
