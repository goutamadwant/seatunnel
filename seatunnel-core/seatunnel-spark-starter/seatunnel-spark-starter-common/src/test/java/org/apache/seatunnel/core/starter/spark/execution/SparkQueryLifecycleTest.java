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
import org.apache.spark.sql.streaming.StreamingQueryException;

import org.junit.jupiter.api.Test;

import scala.Option;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SparkQueryLifecycleTest {
    @Test
    void sameQueryFailureCannotMaskTheOriginalWithSelfSuppression() {
        StreamingQuery query = mock(StreamingQuery.class);
        StreamingQueryException failure =
                new StreamingQueryException(
                        "debug", "query failed", new RuntimeException("root cause"), "0", "1");
        when(query.exception()).thenReturn(Option.apply(failure));
        assertDoesNotThrow(
                () -> SparkQueryLifecycle.stop(Collections.singletonList(query), failure));
        assertEquals(0, failure.getSuppressed().length);
    }

    @Test
    void normalStopStillSurfacesAnotherQueryFailure() {
        StreamingQuery normal = mock(StreamingQuery.class);
        when(normal.exception()).thenReturn(Option.empty());
        StreamingQuery failed = mock(StreamingQuery.class);
        StreamingQueryException failure =
                new StreamingQueryException(
                        "debug", "query failed", new RuntimeException("root cause"), "0", "1");
        when(failed.exception()).thenReturn(Option.apply(failure));
        TaskExecuteException reported =
                assertThrows(
                        TaskExecuteException.class,
                        () -> SparkQueryLifecycle.stop(Arrays.asList(normal, failed), null));
        assertSame(failure, reported.getCause());
    }

    @Test
    void failedStopDoesNotPreventSiblingShutdown() throws Exception {
        StreamingQuery first = mock(StreamingQuery.class);
        StreamingQuery second = mock(StreamingQuery.class);
        when(first.isActive()).thenReturn(true);
        when(second.isActive()).thenReturn(true);
        when(second.exception()).thenReturn(Option.empty());
        RuntimeException cleanup = new RuntimeException("stop failure");
        doThrow(cleanup).when(first).stop();
        RuntimeException original = new RuntimeException("original");
        SparkQueryLifecycle.stop(Arrays.asList(first, second), original);
        assertSame(cleanup, original.getSuppressed()[0]);
        verify(second).stop();
    }
}
