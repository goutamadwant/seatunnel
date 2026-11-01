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

import org.apache.seatunnel.core.starter.exception.TaskExecuteException;

import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.List;

/** Releases queries owned by one execution while retaining its original failure. */
final class SparkQueryLifecycle {
    private SparkQueryLifecycle() {}

    static void stop(List<StreamingQuery> queries, Throwable executionFailure) {
        RuntimeException cleanupFailure = null;
        for (StreamingQuery query : queries) {
            try {
                if (query.isActive()) {
                    query.stop();
                }
                if (query.exception().isDefined()) {
                    throw query.exception().get();
                }
            } catch (Exception failure) {
                if (executionFailure != null) {
                    if (executionFailure != failure) {
                        executionFailure.addSuppressed(failure);
                    }
                } else if (cleanupFailure == null) {
                    cleanupFailure = new TaskExecuteException("Cannot stop Spark query", failure);
                } else {
                    cleanupFailure.addSuppressed(failure);
                }
            }
        }
        if (cleanupFailure != null) {
            throw cleanupFailure;
        }
    }
}
