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

package org.apache.seatunnel.benchmark.storage.imap;

import org.apache.seatunnel.benchmark.storage.SeaTunnelStorageEnvironmentContext;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/** Operational readiness, not completion of background startup or JVM warmup. */
public final class StorageBenchmarkReadiness {
    private StorageBenchmarkReadiness() {}

    public static void awaitCoordinator(SeaTunnelStorageEnvironmentContext environment)
            throws IOException {
        await(
                environment.getServer()::isCoordinatorActive,
                () ->
                        environment
                                .getServer()
                                .getNodeEngine()
                                .getHazelcastInstance()
                                .getLifecycleService()
                                .isRunning(),
                30_000L);
    }

    static void await(BooleanSupplier active, BooleanSupplier running, long timeoutMillis)
            throws IOException {
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("Readiness timeout must be positive");
        }
        long start = System.nanoTime();
        long timeout = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (true) {
            if (Thread.currentThread().isInterrupted()) {
                throw new IOException("Interrupted while waiting for storage benchmark readiness");
            }
            if (!running.getAsBoolean()) {
                throw new IOException("Benchmark member stopped before readiness");
            }
            if (active.getAsBoolean()) {
                return;
            }
            long remaining = timeout - (System.nanoTime() - start);
            if (remaining <= 0) {
                throw new IOException("Benchmark coordinator readiness timed out");
            }
            try {
                TimeUnit.NANOSECONDS.sleep(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(50)));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(
                        "Interrupted while waiting for storage benchmark readiness", e);
            }
        }
    }
}
