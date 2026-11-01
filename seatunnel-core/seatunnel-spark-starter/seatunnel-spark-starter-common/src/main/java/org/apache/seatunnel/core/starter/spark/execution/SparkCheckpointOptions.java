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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/** Optional durable Structured Streaming boundary for Spark jobs. */
public final class SparkCheckpointOptions {
    public static final Option<String> CHECKPOINT_LOCATION =
            Options.key("spark.checkpoint.location")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Durable shared filesystem directory for Spark Structured Streaming recovery. "
                                    + "Use a dedicated directory per unchanged job. Without it, the existing Spark batch execution is retained.");

    static void validateRecoverySink(org.apache.seatunnel.api.sink.SeaTunnelSink<?, ?, ?, ?> sink) {
        if (sink.requiresWriterState()) {
            throw new UnsupportedOperationException(
                    "Spark checkpoint recovery does not yet restore sink writer state. "
                            + "A stateful or XA sink requires a durable Spark sink recovery protocol.");
        }
    }

    static void validateRuntime(String sparkVersion, String javaVersion) {
        if (sparkVersion.startsWith("2.")
                && !("1.8".equals(javaVersion) || "8".equals(javaVersion))) {
            throw new UnsupportedOperationException(
                    "Spark 2.4 checkpoint recovery requires Java 8 runtime and Java 8 artifacts. Build with the spark2-java8 profile; the default Java 11 distribution cannot run Spark 2.4 recovery.");
        }
    }

    private SparkCheckpointOptions() {}
}
