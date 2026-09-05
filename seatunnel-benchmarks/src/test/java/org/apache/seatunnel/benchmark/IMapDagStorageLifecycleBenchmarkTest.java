/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.benchmark;

import org.apache.seatunnel.benchmark.storage.SeaTunnelStorageEnvironmentContext;
import org.apache.seatunnel.benchmark.storage.imap.IMapDagStorageBenchmarkWorkload;

import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IMapDagStorageLifecycleBenchmarkTest {
    @Test
    void failedFixtureSetupClosesEnvironment() throws Exception {
        List<String> events = new ArrayList<>();
        TestState state =
                new TestState(events, false) {
                    @Override
                    protected IMapDagStorageBenchmarkWorkload createWorkload() {
                        return new RecordingWorkload(events, false) {
                            @Override
                            public void setUp(SeaTunnelStorageEnvironmentContext environment) {
                                throw new IllegalStateException("seed failure");
                            }
                        };
                    }
                };
        assertThrows(IllegalStateException.class, () -> state.start(false));
        assertEquals(Arrays.asList("start", "shutdown"), events);
        state.close();
        assertEquals(2, events.size());
    }

    @Test
    void keepsVerificationFailureWhenCleanupAlsoFails() throws Exception {
        TestState state =
                new TestState(new ArrayList<>(), true) {
                    @Override
                    protected IMapDagStorageBenchmarkWorkload createWorkload() {
                        return new RecordingWorkload(new ArrayList<>(), true) {
                            @Override
                            public void cleanStoreIteration() {
                                throw new IllegalStateException("cleanup failure");
                            }
                        };
                    }
                };
        new IMapDagStorageLifecycleBenchmark().finishedJobDagStartupEndToEnd(state);
        IllegalStateException failure = assertThrows(IllegalStateException.class, state::close);
        assertEquals("wrong DAG", failure.getMessage());
        assertEquals("cleanup failure", failure.getSuppressed()[0].getMessage());
    }

    @Test
    void startupOwnsCreationAndShutdownIsOutsideMethod() throws Exception {
        List<String> events = new ArrayList<>();
        TestState state = new TestState(events, false);
        assertEquals(
                100, new IMapDagStorageLifecycleBenchmark().finishedJobDagStartupEndToEnd(state));
        assertEquals(Arrays.asList("start", "seed", "prepare", "evict", "store"), events);
        state.close();
        assertEquals(
                Arrays.asList(
                        "start",
                        "seed",
                        "prepare",
                        "evict",
                        "store",
                        "verify",
                        "cleanupInvocation",
                        "cleanupBatch",
                        "shutdown"),
                events);
    }

    @Test
    void verificationFailureStillCleansAndCloses() throws Exception {
        List<String> events = new ArrayList<>();
        TestState state = new TestState(events, true);
        new IMapDagStorageLifecycleBenchmark().finishedJobDagStartupEndToEnd(state);
        assertThrows(IllegalStateException.class, state::close);
        assertTrue(events.contains("cleanupBatch"));
        assertEquals("shutdown", events.get(events.size() - 1));
    }

    @Test
    void preventsReusingStartupEnvironment() throws Exception {
        TestState state = new TestState(new ArrayList<>(), false);
        try {
            state.start(false);
            assertThrows(IllegalStateException.class, () -> state.start(false));
        } finally {
            state.close();
        }
    }

    @Test
    void afterReadinessMethodOnlyStores() {
        List<String> events = new ArrayList<>();
        IMapDagStorageLifecycleBenchmark.ReadyState state =
                new IMapDagStorageLifecycleBenchmark.ReadyState();
        state.workload = new RecordingWorkload(events, false);
        assertEquals(
                100,
                new IMapDagStorageLifecycleBenchmark().finishedJobDagStoreAfterReadiness(state));
        assertEquals(Arrays.asList("store"), events);
    }

    @Test
    void startupAndStoreHaveDistinctNormalization() throws Exception {
        java.lang.reflect.Method startup =
                IMapDagStorageLifecycleBenchmark.class.getMethod(
                        "finishedJobDagStartupEndToEnd",
                        IMapDagStorageLifecycleBenchmark.StartupState.class);
        assertEquals(0, startup.getAnnotation(Warmup.class).iterations());
        assertEquals(1, startup.getAnnotation(Measurement.class).iterations());
        assertNull(startup.getAnnotation(OperationsPerInvocation.class));
        java.lang.reflect.Method ready =
                IMapDagStorageLifecycleBenchmark.class.getMethod(
                        "finishedJobDagStoreAfterReadiness",
                        IMapDagStorageLifecycleBenchmark.ReadyState.class);
        assertEquals(100, ready.getAnnotation(OperationsPerInvocation.class).value());
    }

    static class TestState extends IMapDagStorageLifecycleBenchmark.StartupState {
        private final List<String> events;
        private final boolean fail;

        TestState(List<String> events, boolean fail) {
            this.events = events;
            this.fail = fail;
        }

        @Override
        protected SeaTunnelStorageEnvironmentContext createEnvironment() {
            return new SeaTunnelStorageEnvironmentContext() {
                @Override
                public void setUp() {
                    events.add("start");
                }

                @Override
                public void tearDown() {
                    events.add("shutdown");
                }
            };
        }

        @Override
        protected IMapDagStorageBenchmarkWorkload createWorkload() {
            return new RecordingWorkload(events, fail);
        }
    }

    static class RecordingWorkload extends IMapDagStorageBenchmarkWorkload {
        final List<String> events;
        final boolean fail;

        RecordingWorkload(List<String> events, boolean fail) {
            this.events = events;
            this.fail = fail;
        }

        @Override
        public void setUp(SeaTunnelStorageEnvironmentContext environment) {
            events.add("seed");
        }

        @Override
        public void prepareStoreIteration() {
            events.add("prepare");
        }

        @Override
        public void prepareInvocation() {
            events.add("evict");
        }

        @Override
        public long storeFinishedJobDagBatch() {
            events.add("store");
            return 100;
        }

        @Override
        public void verifyAllStoredJobDags() {
            events.add("verify");
            if (fail) throw new IllegalStateException("wrong DAG");
        }

        @Override
        public void cleanInvocation() {
            events.add("cleanupInvocation");
        }

        @Override
        public void cleanStoreIteration() {
            events.add("cleanupBatch");
        }
    }
}
