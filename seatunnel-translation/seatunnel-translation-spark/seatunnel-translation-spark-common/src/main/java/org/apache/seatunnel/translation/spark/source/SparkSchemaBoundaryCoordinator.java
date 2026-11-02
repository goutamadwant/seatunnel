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

import org.apache.seatunnel.api.table.catalog.CatalogTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/** Owns typed query replacement after a durably completed source schema boundary. */
public final class SparkSchemaBoundaryCoordinator {
    /** A rebuild recreates source, transforms and sink together with the supplied catalogs. */
    public interface QueryFactory {
        StreamingQuery start(List<CatalogTable> catalogs) throws Exception;

        default StreamingQuery start(List<CatalogTable> catalogs, long appliedBoundary)
                throws Exception {
            return start(catalogs);
        }

        /**
         * Reconciles external schema state after old work is committed and its query stopped. This
         * operation must tolerate repetition after a driver failure, including failure after
         * physical DDL but before a new Spark epoch commits. A local restore must not replay DDL.
         */
        void reconcile(SparkCommittedSourceState boundary) throws Exception;

        default boolean restoreCheckpointSchema() {
            return true;
        }
    }

    private final AtomicBoolean stopped = new AtomicBoolean();

    public void requestStop() {
        stopped.set(true);
    }

    /** Runs one query with one SeaTunnel source; the caller owns independent sink query plans. */
    public void run(
            Configuration configuration,
            String queryCheckpoint,
            String user,
            List<CatalogTable> initialCatalogs,
            QueryFactory factory)
            throws Exception {
        if (stopped.get()) return;
        Optional<SparkCommittedSourceState> restored =
                SparkCommittedSourceState.load(configuration, queryCheckpoint, 0, 1, user);
        if (stopped.get()) return;
        List<CatalogTable> analyzed = new ArrayList<>(initialCatalogs);
        long appliedBoundary = -1;
        if (restored.isPresent()) {
            SparkSourceCheckpoint checkpoint = restored.get().getCheckpoint();
            if (!factory.restoreCheckpointSchema()
                    && checkpoint.getPhase() != SparkSourceCheckpoint.Phase.DATA)
                throw new IllegalStateException(
                        "Schema propagation disabled with a pending Spark schema boundary");
            if (checkpoint.requiresReanalysis()) {
                factory.reconcile(restored.get());
                appliedBoundary = restored.get().getSourceCheckpointId();
            }
            if (factory.restoreCheckpointSchema() && !checkpoint.getTables().isEmpty())
                analyzed = new ArrayList<>(checkpoint.getTables());
        }
        StreamingQuery query = null;
        Throwable failure = null;
        try {
            if (stopped.get()) return;
            query = factory.start(analyzed, appliedBoundary);
            while (!stopped.get()) {
                if (query.awaitTermination(100)) return;
                Optional<SparkCommittedSourceState> committed =
                        SparkCommittedSourceState.load(configuration, queryCheckpoint, 0, 1, user);
                if (stopped.get()) break;
                if (!committed.isPresent()) continue;
                SparkSourceCheckpoint checkpoint = committed.get().getCheckpoint();
                if (!checkpoint.requiresReanalysis()
                        || committed.get().getSourceCheckpointId() <= appliedBoundary) continue;
                if (!factory.restoreCheckpointSchema())
                    throw new IllegalStateException(
                            "A disabled source emitted a Spark schema boundary");
                // The commit marker proves the old sink epoch completed. Stop every old task
                // before physical DDL or constructing a plan with the new column layout.
                query.stop();
                query = null;
                if (stopped.get()) break;
                factory.reconcile(committed.get());
                analyzed = new ArrayList<>(checkpoint.getTables());
                appliedBoundary = committed.get().getSourceCheckpointId();
                if (!stopped.get()) query = factory.start(analyzed, appliedBoundary);
            }
        } catch (Exception | Error error) {
            failure = error;
            throw error;
        } finally {
            if (query != null) {
                try {
                    query.stop();
                } catch (Exception | Error closeFailure) {
                    if (failure != null) {
                        if (failure != closeFailure) failure.addSuppressed(closeFailure);
                    } else throw closeFailure;
                }
            }
        }
    }
}
