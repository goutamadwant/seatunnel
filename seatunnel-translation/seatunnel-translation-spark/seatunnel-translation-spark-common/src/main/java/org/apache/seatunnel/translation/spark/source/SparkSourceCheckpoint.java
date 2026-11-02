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
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.translation.spark.execution.MultiTableManager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Source state and schema boundary published atomically with one interval's typed rows. */
public final class SparkSourceCheckpoint implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final int VERSION = 2;
    private static final int MAX_STATE_BYTES = 256 * 1024 * 1024;

    public enum Phase {
        DATA,
        BEFORE_SCHEMA_CHANGE,
        AFTER_SCHEMA_CHANGE
    }

    private final int parallelism;
    private final String rowSchema;
    private final Map<Integer, List<byte[]>> state;
    private final List<CatalogTable> beforeTables;
    private final List<CatalogTable> tables;
    private final List<SchemaChangeEvent> events;
    private final Phase phase;

    SparkSourceCheckpoint(
            int parallelism,
            String rowSchema,
            Map<Integer, List<byte[]>> state,
            List<CatalogTable> beforeTables,
            List<CatalogTable> tables,
            List<SchemaChangeEvent> events,
            Phase phase) {
        this.parallelism = parallelism;
        this.rowSchema = rowSchema;
        this.state = new HashMap<>(state);
        this.beforeTables = Collections.unmodifiableList(new ArrayList<>(beforeTables));
        this.tables = Collections.unmodifiableList(new ArrayList<>(tables));
        this.events = Collections.unmodifiableList(new ArrayList<>(events));
        this.phase = phase;
    }

    public Phase getPhase() {
        return phase;
    }

    public List<CatalogTable> getBeforeTables() {
        return beforeTables;
    }

    public List<CatalogTable> getTables() {
        return tables;
    }

    public List<SchemaChangeEvent> getEvents() {
        return events;
    }

    public boolean requiresReanalysis() {
        return phase == Phase.AFTER_SCHEMA_CHANGE && !events.isEmpty();
    }

    Map<Integer, List<byte[]>> getState() {
        return new HashMap<>(state);
    }

    void validateAcknowledgement(int expectedParallelism, MultiTableManager manager)
            throws IOException {
        try {
            validate(expectedParallelism, manager, true);
        } catch (IOException oldPlanMismatch) {
            if (phase != Phase.AFTER_SCHEMA_CHANGE) throw oldPlanMismatch;
            // A committed boundary can be acknowledged by either the finishing old query or
            // the rebuilt query on restart. Neither permits an unrelated catalog or layout.
            validate(expectedParallelism, manager, false);
        }
    }

    void validateParallelism(int expected) throws IOException {
        if (parallelism != expected)
            throw new IOException("Incompatible Spark source recovery parallelism");
    }

    void validateLiveSchemaPredecessor(int expectedParallelism, MultiTableManager manager)
            throws IOException {
        validateParallelism(expectedParallelism);
        if (phase != Phase.DATA) {
            throw new IOException(
                    "Schema propagation disabled with a pending Spark schema boundary");
        }
        if (tables.isEmpty()) {
            // Legacy journals cannot establish table identity for a layout migration.
            validate(expectedParallelism, manager.getTableSchema().json(), false);
            return;
        }
        java.util.Set<String> expected = new java.util.HashSet<>();
        for (CatalogTable table : tables) expected.add(table.getTablePath().toString());
        java.util.Set<String> actual = new java.util.HashSet<>();
        for (CatalogTable table : manager.getCatalogTables())
            actual.add(table.getTablePath().toString());
        if (!tables.isEmpty() && (expected.size() != tables.size() || !expected.equals(actual))) {
            throw new IOException("Incompatible Spark source routing for live schema recovery");
        }
    }

    void validate(int expectedParallelism, MultiTableManager manager, boolean replayRows)
            throws IOException {
        validate(expectedParallelism, manager.getTableSchema().json(), replayRows);
        List<CatalogTable> expected =
                replayRows && phase == Phase.AFTER_SCHEMA_CHANGE ? beforeTables : tables;
        if (!expected.isEmpty()
                && !SparkSourceCatalog.sameSchemas(expected, manager.getCatalogTables())) {
            throw new IOException(
                    "Incompatible Spark source table schemas or routing; query reanalysis is required");
        }
    }

    void validate(int expectedParallelism, String analyzedSchema, boolean replayRows)
            throws IOException {
        String expectedSchema = rowSchema;
        if (!replayRows && phase == Phase.AFTER_SCHEMA_CHANGE && !tables.isEmpty()) {
            expectedSchema =
                    new MultiTableManager(tables.toArray(new CatalogTable[0]))
                            .getTableSchema()
                            .json();
        }
        if (parallelism != expectedParallelism || !expectedSchema.equals(analyzedSchema)) {
            throw new IOException("Incompatible Spark source recovery schema or parallelism");
        }
    }

    void write(DataOutputStream output) throws IOException {
        output.writeInt(VERSION);
        output.writeInt(parallelism);
        output.writeUTF(rowSchema);
        byte[] bytes = SerializationUtils.serialize(this);
        if (bytes.length > MAX_STATE_BYTES)
            throw new IOException("Spark recovery state exceeds size limit");
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    /** Reads a published interval without advancing, acknowledging or reclaiming source state. */
    public static Optional<SparkSourceCheckpoint> load(
            Configuration configuration,
            String sourceCheckpoint,
            int partition,
            long checkpoint,
            String user)
            throws Exception {
        if (checkpoint < 1) return Optional.empty();
        Path path =
                new Path(
                        new Path(
                                new Path(sourceCheckpoint, "seatunnel-batches"),
                                Integer.toString(partition)),
                        checkpoint + "/state");
        UserGroupInformation identity =
                user == null || user.isEmpty()
                        ? UserGroupInformation.getCurrentUser()
                        : UserGroupInformation.getBestUGI(
                                configuration.get("hadoop.security.kerberos.ticket.cache.path"),
                                user);
        URI uri =
                path.toUri().getScheme() == null
                        ? FileSystem.getDefaultUri(configuration)
                        : path.toUri();
        return identity.doAs(
                (PrivilegedExceptionAction<Optional<SparkSourceCheckpoint>>)
                        () -> {
                            try (FileSystem fs = FileSystem.newInstance(uri, configuration)) {
                                if (!fs.exists(path)) return Optional.empty();
                                try (DataInputStream input = new DataInputStream(fs.open(path))) {
                                    return Optional.of(read(input));
                                }
                            }
                        });
    }

    /** Holds the old typed query at an evolved catalog until the driver rebuilds its plan. */
    public static boolean shouldPause(
            Configuration configuration,
            String sourceCheckpoint,
            long checkpoint,
            String user,
            MultiTableManager analyzed)
            throws Exception {
        Optional<SparkSourceCheckpoint> published =
                load(configuration, sourceCheckpoint, 0, checkpoint, user);
        return published.isPresent()
                && published.get().requiresReanalysis()
                && !SparkSourceCatalog.sameSchemas(
                        published.get().tables, analyzed.getCatalogTables());
    }

    /** Holds every unapplied DDL boundary, including destructive changes with equal layouts. */
    public static boolean shouldPause(
            Configuration configuration,
            String sourceCheckpoint,
            long checkpoint,
            String user,
            MultiTableManager analyzed,
            long appliedBoundary)
            throws Exception {
        Optional<SparkSourceCheckpoint> published =
                load(configuration, sourceCheckpoint, 0, checkpoint, user);
        return published.isPresent()
                && published.get().requiresReanalysis()
                && (checkpoint > appliedBoundary
                        || !SparkSourceCatalog.sameSchemas(
                                published.get().tables, analyzed.getCatalogTables()));
    }

    /** Reads the fixed-schema v1 format as well as schema-aware v2 metadata. */
    @SuppressWarnings("unchecked")
    public static SparkSourceCheckpoint read(DataInputStream input) throws IOException {
        int version = input.readInt();
        if (version != 1 && version != VERSION)
            throw new IOException("Unsupported Spark source journal version " + version);
        int parallelism = input.readInt();
        String schema = input.readUTF();
        int size = input.readInt();
        if (parallelism < 1 || size < 0 || size > MAX_STATE_BYTES)
            throw new IOException("Invalid Spark recovery state size or parallelism");
        byte[] bytes = new byte[size];
        input.readFully(bytes);
        try {
            Object decoded = SerializationUtils.deserialize(bytes);
            if (version == 1) {
                return new SparkSourceCheckpoint(
                        parallelism,
                        schema,
                        (Map<Integer, List<byte[]>>) decoded,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Phase.DATA);
            }
            SparkSourceCheckpoint result = (SparkSourceCheckpoint) decoded;
            if (result.parallelism != parallelism
                    || !result.rowSchema.equals(schema)
                    || result.phase == null
                    || result.tables == null
                    || result.events == null
                    || result.beforeTables == null) {
                throw new IOException("Inconsistent Spark recovery metadata");
            }
            return result;
        } catch (RuntimeException error) {
            throw new IOException("Invalid Spark recovery state", error);
        }
    }
}
