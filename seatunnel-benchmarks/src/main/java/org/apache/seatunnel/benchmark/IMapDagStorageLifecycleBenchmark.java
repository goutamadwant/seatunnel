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
import org.apache.seatunnel.benchmark.storage.imap.StorageBenchmarkReadiness;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;

import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.benchmark.storage.imap.IMapDagStorageBenchmarkWorkload.STORE_OPERATIONS_PER_INVOCATION;

/** Distinguishes startup-inclusive latency from writes after an operational readiness boundary. */
@BenchmarkMode(Mode.SingleShotTime)
@Threads(1)
@Fork(
        value = 3,
        jvmArgsAppend = {
            "-Xms4g",
            "-Xmx4g",
            "-XX:+UseG1GC",
            "-XX:+AlwaysPreTouch",
            "-XX:+DisableExplicitGC",
            "-XX:ActiveProcessorCount=4",
            "-Djava.net.preferIPv4Stack=true"
        })
public class IMapDagStorageLifecycleBenchmark {

    /**
     * Includes engine/client startup, fixture seeding and 100 writes, but not JVM launch or
     * shutdown.
     */
    @Benchmark
    @Warmup(iterations = 0)
    @Measurement(iterations = 1, batchSize = 1)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public long finishedJobDagStartupEndToEnd(StartupState state) throws Exception {
        state.start(false);
        state.prepare();
        return state.store();
    }

    /**
     * Measures 100 writes after coordinator observation, storage read-back and fixed JMH warmup.
     */
    @Benchmark
    @Warmup(iterations = 3)
    @Measurement(iterations = 5, batchSize = 1)
    @OperationsPerInvocation(STORE_OPERATIONS_PER_INVOCATION)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public long finishedJobDagStoreAfterReadiness(ReadyState state) {
        return state.store();
    }

    /**
     * Owns the environment explicitly so JMH cannot move startup out of the startup measurement.
     */
    @State(Scope.Thread)
    public abstract static class StorageState {
        @Param({"1", "10", "100"})
        public int pipelineCount;

        @Param({"0", "100"})
        public int storedDagCount;

        protected SeaTunnelStorageEnvironmentContext environment;
        protected IMapDagStorageBenchmarkWorkload workload;
        private boolean prepared;

        protected SeaTunnelStorageEnvironmentContext createEnvironment() {
            return new SeaTunnelStorageEnvironmentContext();
        }

        protected IMapDagStorageBenchmarkWorkload createWorkload() {
            return new IMapDagStorageBenchmarkWorkload();
        }

        void start(boolean afterReadiness) throws Exception {
            if (environment != null) {
                throw new IllegalStateException("A storage trial can only start once");
            }
            environment = createEnvironment();
            boolean started = false;
            try {
                environment.setUp();
                started = true;
                if (afterReadiness) {
                    StorageBenchmarkReadiness.awaitCoordinator(environment);
                }
                workload = createWorkload();
                workload.pipelineCount = pipelineCount;
                workload.storedDagCount = storedDagCount;
                workload.setUp(environment);
                if (afterReadiness) {
                    // Exercise the production MapStore reload before warmup, not a settling delay.
                    workload.loadFinishedJobDag();
                    workload.verifyFinishedJobDagLoaded();
                    StorageBenchmarkReadiness.awaitCoordinator(environment);
                }
            } catch (Exception failure) {
                // Environment setup owns cleanup of its own failures; later failures are ours.
                if (started) {
                    try {
                        environment.tearDown();
                    } catch (Exception cleanupFailure) {
                        failure.addSuppressed(cleanupFailure);
                    }
                }
                environment = null;
                throw failure;
            }
        }

        void prepare() {
            prepared = true;
            workload.prepareStoreIteration();
            workload.prepareInvocation();
        }

        long store() {
            return workload.storeFinishedJobDagBatch();
        }

        void clean() {
            if (!prepared) {
                return;
            }
            prepared = false;
            RuntimeException failure = null;
            for (Runnable action :
                    new Runnable[] {
                        workload::verifyAllStoredJobDags,
                        workload::cleanInvocation,
                        workload::cleanStoreIteration
                    }) {
                try {
                    action.run();
                } catch (RuntimeException error) {
                    if (failure == null) {
                        failure = error;
                    } else {
                        failure.addSuppressed(error);
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        }

        /**
         * Keeps shutdown outside the score and still closes the engine after verification failure.
         */
        @TearDown(Level.Trial)
        public void close() throws Exception {
            Exception failure = null;
            try {
                clean();
            } catch (RuntimeException error) {
                failure = error;
            } finally {
                if (environment != null) {
                    try {
                        environment.tearDown();
                    } catch (Exception error) {
                        if (failure == null) {
                            failure = error;
                        } else {
                            failure.addSuppressed(error);
                        }
                    } finally {
                        environment = null;
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    @State(Scope.Thread)
    public static class StartupState extends StorageState {
        @Setup(Level.Trial)
        public void validate(BenchmarkParams params) {
            validateSingleShot(params, true);
        }
    }

    @State(Scope.Thread)
    public static class ReadyState extends StorageState {
        @Setup(Level.Trial)
        public void initialize(BenchmarkParams params) throws Exception {
            validateSingleShot(params, false);
            start(true);
        }

        @Setup(Level.Iteration)
        public void prepareIteration() {
            if (!environment.getServer().isCoordinatorActive()) {
                throw new IllegalStateException(
                        "Coordinator lost readiness before the store batch");
            }
            prepare();
        }

        @TearDown(Level.Iteration)
        public void cleanIteration() {
            try {
                if (!environment.getServer().isCoordinatorActive()) {
                    throw new IllegalStateException(
                            "Coordinator lost readiness after the store batch");
                }
            } finally {
                clean();
            }
        }
    }

    static void validateSingleShot(BenchmarkParams params, boolean startup) {
        if (params.getOpsPerInvocation() != (startup ? 1 : STORE_OPERATIONS_PER_INVOCATION)) {
            throw new IllegalArgumentException(
                    "Storage lifecycle operation normalization must not be overridden");
        }
        if (params.getMode() != Mode.SingleShotTime
                || params.getThreads() != 1
                || params.getForks() < 1
                || params.getWarmupForks() != 0
                || params.getMeasurement().getBatchSize() != 1
                || params.getWarmup().getBatchSize() != 1) {
            throw new IllegalArgumentException(
                    "Storage lifecycle measurements require forked, single-thread single-shot execution");
        }
        if (startup
                && (params.getWarmup().getCount() != 0
                        || params.getMeasurement().getCount() != 1)) {
            throw new IllegalArgumentException(
                    "Startup measurement requires zero warmup and one measurement per fresh JVM");
        }
        if (!startup
                && (params.getWarmup().getCount() < 1 || params.getMeasurement().getCount() < 1)) {
            throw new IllegalArgumentException(
                    "After-readiness measurement requires fixed positive warmup and measurement counts");
        }
    }
}
