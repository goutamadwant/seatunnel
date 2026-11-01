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

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SupportCoordinate;
import org.apache.seatunnel.api.source.SupportSchemaChangeWatermarks;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.schema.event.RestoreTableSchemaEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.Handover;
import org.apache.seatunnel.translation.source.BaseSourceFunction;
import org.apache.seatunnel.translation.source.CoordinatedSource;
import org.apache.seatunnel.translation.source.ParallelSource;
import org.apache.seatunnel.translation.spark.execution.MultiTableManager;
import org.apache.seatunnel.translation.spark.serialization.InternalRowCollector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Materializes one source interval before Spark can consume it. A published interval contains both
 * its rows and its end state; task retries replay that interval without reopening the source.
 * Checkpoint completion is notified only from Spark's commit callback, after sink commit.
 */
public final class SparkMicroBatchReader implements Closeable {
    private final SeaTunnelSource<SeaTunnelRow, ?, ?> source;
    private final int parallelism;
    private final int subtask;
    private final int checkpoint;
    private final int interval;
    private final String jobId;
    private final String attemptOwner;
    private final Path root;
    private final Configuration configuration;
    private final String user;
    private final MultiTableManager tables;
    private final Map<String, String> options;
    private final Object checkpointLock = new Object();
    private final Handover<InternalRow> handover = new Handover<>();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private volatile boolean sealed;
    private volatile boolean cancelled;
    private FileSystem fs;
    private DataInputStream input;
    private InternalRow current;
    private boolean exhausted;
    private volatile BaseSourceFunction<SeaTunnelRow> internal;
    private ExecutorService executor;
    private Path temporary;
    private boolean initialized;
    private volatile boolean materializing;
    private org.apache.hadoop.security.UserGroupInformation identity;

    public SparkMicroBatchReader(
            SeaTunnelSource<SeaTunnelRow, ?, ?> source,
            int parallelism,
            String jobId,
            int subtask,
            int checkpoint,
            int interval,
            String location,
            String hdfsRoot,
            String user,
            Map<String, String> options,
            MultiTableManager tables) {
        this(
                source,
                parallelism,
                jobId,
                subtask,
                checkpoint,
                interval,
                location,
                hdfsRoot,
                user,
                options,
                tables,
                new Configuration());
    }

    public SparkMicroBatchReader(
            SeaTunnelSource<SeaTunnelRow, ?, ?> source,
            int parallelism,
            String jobId,
            int subtask,
            int checkpoint,
            int interval,
            String location,
            String hdfsRoot,
            String user,
            Map<String, String> options,
            MultiTableManager tables,
            Configuration hadoopConfiguration) {
        this(
                source,
                parallelism,
                jobId,
                subtask,
                checkpoint,
                interval,
                location,
                hdfsRoot,
                user,
                options,
                tables,
                hadoopConfiguration,
                jobId);
    }

    public SparkMicroBatchReader(
            SeaTunnelSource<SeaTunnelRow, ?, ?> source,
            int parallelism,
            String jobId,
            int subtask,
            int checkpoint,
            int interval,
            String location,
            String hdfsRoot,
            String user,
            Map<String, String> options,
            MultiTableManager tables,
            Configuration hadoopConfiguration,
            String driverOwner) {
        if (checkpoint < 1 || interval < 1 || parallelism < 1) {
            throw new IllegalArgumentException(
                    "Checkpoint, interval and parallelism must be positive");
        }
        this.source = source;
        this.parallelism = parallelism;
        this.jobId = jobId;
        this.attemptOwner =
                UUID.nameUUIDFromBytes(
                                driverOwner.getBytes(java.nio.charset.StandardCharsets.UTF_8))
                        .toString();
        this.subtask = subtask;
        this.checkpoint = checkpoint;
        this.interval = interval;
        this.root = new Path(new Path(location, "seatunnel-batches"), Integer.toString(subtask));
        this.configuration = new Configuration(hadoopConfiguration);
        configuration.set("fs.defaultFS", hdfsRoot);
        this.user = user;
        this.options = options;
        this.tables = tables;
    }

    public boolean next() throws Exception {
        if (cancelled) {
            throw new IOException("Spark source interval cancelled");
        }
        if (!initialized) {
            try {
                materializing = true;
                initialize();
                checkFailure();
                initialized = true;
            } catch (Exception error) {
                materializing = false;
                try {
                    close();
                } catch (Exception cleanup) {
                    error.addSuppressed(cleanup);
                }
                throw error;
            } finally {
                materializing = false;
            }
        }
        if (exhausted) {
            return false;
        }
        int length = input.readInt();
        if (length == -1) {
            exhausted = true;
            current = null;
            return false;
        }
        if (length < 0 || length > 256 * 1024 * 1024) {
            throw new IOException("Invalid Spark recovery row length: " + length);
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        UnsafeRow row = new UnsafeRow(tables.getTableSchema().fields().length);
        row.pointTo(bytes, bytes.length);
        current = row;
        return true;
    }

    public InternalRow get() {
        if (current == null) {
            throw new IllegalStateException("No current Spark recovery row");
        }
        return current;
    }

    private void openFileSystem() throws Exception {
        URI uri =
                root.toUri().getScheme() == null
                        ? FileSystem.getDefaultUri(configuration)
                        : root.toUri();
        identity =
                user == null || user.isEmpty()
                        ? org.apache.hadoop.security.UserGroupInformation.getCurrentUser()
                        : org.apache.hadoop.security.UserGroupInformation.getBestUGI(
                                configuration.get("hadoop.security.kerberos.ticket.cache.path"),
                                user);
        fs =
                identity.doAs(
                        (java.security.PrivilegedExceptionAction<FileSystem>)
                                () -> FileSystem.newInstance(uri, configuration));
    }

    private void initialize() throws Exception {
        openFileSystem();
        if (!(fs instanceof DistributedFileSystem)
                && !(fs instanceof org.apache.hadoop.fs.LocalFileSystem)
                && !(fs instanceof org.apache.hadoop.fs.RawLocalFileSystem)) {
            throw new IOException(
                    "Spark source recovery requires a local or HDFS checkpoint filesystem");
        }
        reclaimPreviousApplicationAttempts();
        Path completed = batch(checkpoint);
        if (!fs.exists(completed)) {
            materialize(completed);
        }
        readState(completed);
        input = new DataInputStream(fs.open(new Path(completed, "rows")));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void materialize(Path completed) throws Exception {
        SparkSourceCheckpoint predecessor =
                checkpoint == 1 ? null : readCheckpoint(batch(checkpoint - 1));
        boolean restoreCheckpointSchema = SparkSourceCatalog.restoreCheckpointSchema(source);
        if (predecessor != null) {
            if (restoreCheckpointSchema) predecessor.validate(parallelism, tables, false);
            else predecessor.validateLiveSchemaPredecessor(parallelism, tables);
        }
        Map<Integer, List<byte[]>> restored = predecessor == null ? null : predecessor.getState();
        List<CatalogTable> beforeTables =
                !restoreCheckpointSchema || predecessor == null || predecessor.getTables().isEmpty()
                        ? tables.getCatalogTables()
                        : predecessor.getTables();
        AtomicReference<List<CatalogTable>> catalogTables =
                new AtomicReference<>(new ArrayList<>(beforeTables));
        List<SchemaChangeEvent> events = new ArrayList<>();
        AtomicReference<Map<Integer, List<byte[]>>> boundaryState = new AtomicReference<>();
        boolean schemaOnly =
                predecessor != null
                        && predecessor.getPhase()
                                == SparkSourceCheckpoint.Phase.BEFORE_SCHEMA_CHANGE;
        AtomicReference<SparkSourceCheckpoint.Phase> phase =
                new AtomicReference<>(
                        schemaOnly
                                ? SparkSourceCheckpoint.Phase.BEFORE_SCHEMA_CHANGE
                                : SparkSourceCheckpoint.Phase.DATA);
        temporary = new Path(root, "." + checkpoint + "-" + attemptOwner + "-" + UUID.randomUUID());
        fs.mkdirs(temporary);
        try (DataOutputStream owner =
                new DataOutputStream(fs.create(new Path(temporary, "owner"), false))) {
            owner.writeUTF(temporary.getName());
        }
        executor = Executors.newFixedThreadPool(2);
        UnsafeProjection projection = UnsafeProjection.create(tables.getTableSchema());
        final DataOutputStream output =
                new DataOutputStream(fs.create(new Path(temporary, "rows"), false));
        Throwable primaryFailure = null;
        try {
            AtomicReference<InternalRowCollector> delegate =
                    new AtomicReference<>(
                            tables.getInternalRowCollector(handover, checkpointLock, options));
            Collector<SeaTunnelRow> collector =
                    new Collector<SeaTunnelRow>() {
                        @Override
                        public void collect(SeaTunnelRow row) {
                            checkpointAction(
                                    () -> {
                                        if (schemaOnly) {
                                            CatalogTable rowTable =
                                                    beforeTables.stream()
                                                            .filter(
                                                                    table ->
                                                                            table.getTablePath()
                                                                                            .toString()
                                                                                            .equals(
                                                                                                    row
                                                                                                            .getTableId())
                                                                                    || (beforeTables
                                                                                                            .size()
                                                                                                    == 1
                                                                                            && row.getTableId()
                                                                                                    .isEmpty()))
                                                            .findFirst()
                                                            .orElseThrow(
                                                                    () ->
                                                                            new IllegalStateException(
                                                                                    "Unknown table at Spark schema boundary"));
                                            if (row.getArity()
                                                            != rowTable.getSeaTunnelRowType()
                                                                    .getTotalFields()
                                                    || events.stream()
                                                            .anyMatch(
                                                                    event ->
                                                                            event.tablePath()
                                                                                    .equals(
                                                                                            rowTable
                                                                                                    .getTablePath()))) {
                                                throw new IllegalStateException(
                                                        "A new-schema row preceded its Spark after-watermark boundary");
                                            }
                                        }
                                        delegate.get().collect(row);
                                    });
                        }

                        @Override
                        public void collect(SchemaChangeEvent event) {
                            checkpointAction(
                                    () -> {
                                        if (event instanceof RestoreTableSchemaEvent) {
                                            List<CatalogTable> refreshed =
                                                    replaceTable(
                                                            catalogTables.get(),
                                                            event.getChangeAfter());
                                            restoreSchema(refreshed);
                                            return;
                                        }
                                        if (!schemaOnly || event.getChangeAfter() == null) {
                                            throw new IllegalStateException(
                                                    "Spark schema events require a before-watermark boundary and an evolved catalog");
                                        }
                                        catalogTables.set(
                                                replaceTable(
                                                        catalogTables.get(),
                                                        event.getChangeAfter()));
                                        events.add(event);
                                    });
                        }

                        @Override
                        public void restoreSchema(List<CatalogTable> restoredTables) {
                            checkpointAction(
                                    () -> {
                                        if (restoredTables.isEmpty()) return;
                                        MultiTableManager restoredManager =
                                                new MultiTableManager(
                                                        restoredTables.toArray(
                                                                new CatalogTable[0]));
                                        if (!restoredManager
                                                        .getTableSchema()
                                                        .equals(tables.getTableSchema())
                                                || !SparkSourceCatalog.sameSchemas(
                                                        restoredTables,
                                                        tables.getCatalogTables())) {
                                            throw new IllegalStateException(
                                                    "Restored collector schema requires Spark query reanalysis before rows are admitted");
                                        }
                                        catalogTables.set(new ArrayList<>(restoredTables));
                                        delegate.set(
                                                restoredManager.getInternalRowCollector(
                                                        handover, checkpointLock, options));
                                    });
                        }

                        @Override
                        public void markSchemaChangeBeforeCheckpoint() {
                            checkpointAction(
                                    () -> {
                                        if (!(source instanceof SupportSchemaChangeWatermarks)) {
                                            throw new IllegalStateException(
                                                    "Spark typed schema transitions require a snapshot-safe source watermark protocol");
                                        }
                                        if (!schemaOnly) {
                                            phase.set(
                                                    SparkSourceCheckpoint.Phase
                                                            .BEFORE_SCHEMA_CHANGE);
                                            sealSchemaBoundary();
                                        }
                                    });
                        }

                        @Override
                        public void markSchemaChangeAfterCheckpoint() {
                            checkpointAction(
                                    () -> {
                                        if (!schemaOnly)
                                            throw new IllegalStateException(
                                                    "Spark schema after-watermark has no before boundary");
                                        phase.set(SparkSourceCheckpoint.Phase.AFTER_SCHEMA_CHANGE);
                                        sealSchemaBoundary();
                                    });
                        }

                        private void checkpointAction(Runnable action) {
                            synchronized (checkpointLock) {
                                if (sealed || cancelled) throw new BatchSealed();
                                try {
                                    action.run();
                                } catch (BatchSealed complete) {
                                    throw complete;
                                } catch (RuntimeException | Error error) {
                                    failure.compareAndSet(null, error);
                                    throw error;
                                }
                            }
                        }

                        private void sealSchemaBoundary() {
                            // Capture bytes before releasing the poll's checkpoint lock. Another
                            // reader may advance its offset before discovering that collection is
                            // sealed; that unaccepted offset must never enter this checkpoint.
                            sealed = true;
                            try {
                                boundaryState.set(internal.snapshotState(checkpoint));
                            } catch (Exception error) {
                                throw new IllegalStateException(
                                        "Cannot snapshot Spark schema boundary", error);
                            }
                            throw new BatchSealed();
                        }

                        @Override
                        public Object getCheckpointLock() {
                            return checkpointLock;
                        }

                        @Override
                        public boolean isEmptyThisPollNext() {
                            return delegate.get().isEmptyThisPollNext();
                        }

                        @Override
                        public void resetEmptyThisPollNext() {
                            delegate.get().resetEmptyThisPollNext();
                        }
                    };
            internal =
                    source instanceof SupportCoordinate
                            ? new CheckpointCoordinatedSource(
                                    source, restored, parallelism, jobId, false, checkpointLock)
                            : new CheckpointParallelSource(
                                    source, restored, parallelism, jobId, subtask, false);
            if (cancelled) {
                ((CancellableSource) internal).requestCancellation();
                checkFailure();
            }
            internal.open();
            checkFailure();
            if (restored != null) {
                ((CancellableSource) internal).completeRestoredEnumerator(checkpoint - 1);
            }
            Future<?> drain =
                    executor.submit(
                            () -> {
                                try {
                                    while (!sealed || !handover.isEmpty()) {
                                        java.util.Optional<InternalRow> row = handover.pollNext();
                                        if (row.isPresent()) {
                                            byte[] bytes = projection.apply(row.get()).getBytes();
                                            output.writeInt(bytes.length);
                                            output.write(bytes);
                                        } else {
                                            Thread.sleep(1);
                                        }
                                    }
                                } catch (Throwable error) {
                                    failure.compareAndSet(null, error);
                                    while (!sealed) {
                                        try {
                                            handover.pollNext();
                                            Thread.sleep(1);
                                        } catch (Exception interrupted) {
                                            if (sealed) {
                                                break;
                                            }
                                        }
                                    }
                                }
                            });
            final BaseSourceFunction<SeaTunnelRow> runningSource = internal;
            Future<?> run =
                    executor.submit(
                            () -> {
                                try {
                                    runningSource.run(collector);
                                } catch (Throwable error) {
                                    Throwable cause = error;
                                    while (cause.getCause() != null) {
                                        cause = cause.getCause();
                                    }
                                    if (!(cause instanceof BatchSealed)) {
                                        failure.compareAndSet(null, error);
                                    }
                                }
                            });
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(interval);
            while (!run.isDone()
                    && !sealed
                    && failure.get() == null
                    && !cancelled
                    && System.nanoTime() < deadline) {
                Thread.sleep(5);
            }
            Map<Integer, List<byte[]>> state;
            synchronized (checkpointLock) {
                sealed = true;
                checkFailure();
                ((CancellableSource) internal).verifyFailure();
                state =
                        boundaryState.get() == null
                                ? internal.snapshotState(checkpoint)
                                : boundaryState.get();
                if (schemaOnly
                        && !events.isEmpty()
                        && phase.get() != SparkSourceCheckpoint.Phase.AFTER_SCHEMA_CHANGE) {
                    throw new IOException(
                            "Incomplete Spark schema-change block cannot be published");
                }
            }
            BaseSourceFunction<SeaTunnelRow> completedSource = internal;
            internal = null;
            completedSource.close();
            run.get(30, TimeUnit.SECONDS);
            ((CancellableSource) completedSource).verifyFailure();
            drain.get(30, TimeUnit.SECONDS);
            checkFailure();
            output.writeInt(-1);
            output.flush();
            try (DataOutputStream metadata =
                    new DataOutputStream(fs.create(new Path(temporary, "state"), false))) {
                new SparkSourceCheckpoint(
                                parallelism,
                                tables.getTableSchema().json(),
                                state,
                                beforeTables,
                                catalogTables.get(),
                                events,
                                phase.get())
                        .write(metadata);
            }
        } catch (Throwable error) {
            primaryFailure = error;
            throw error;
        } finally {
            IOException cleanupFailure = null;
            try {
                stopSource();
            } catch (IOException cleanup) {
                cleanupFailure = cleanup;
            }
            cleanupFailure = release(output, cleanupFailure);
            if (cleanupFailure != null) {
                if (primaryFailure != null) {
                    primaryFailure.addSuppressed(cleanupFailure);
                } else {
                    throw cleanupFailure;
                }
            }
        }
        checkFailure();
        // A previous-driver attempt may have been retired while its source was closing.
        // Never publish a directory recreated by a late metadata write after reclamation.
        try (DataInputStream owner = new DataInputStream(fs.open(new Path(temporary, "owner")))) {
            if (!temporary.getName().equals(owner.readUTF())) {
                throw new IOException("Spark source attempt ownership changed before publication");
            }
        }
        // Publish a complete directory; a losing attempt must replay the existing winner.
        try {
            identity.doAs(
                    (java.security.PrivilegedExceptionAction<Void>)
                            () -> {
                                if (fs instanceof DistributedFileSystem) {
                                    ((DistributedFileSystem) fs)
                                            .rename(temporary, completed, Options.Rename.NONE);
                                } else {
                                    publishLocal(localPath(temporary), localPath(completed));
                                }
                                return null;
                            });
            temporary = null;
        } catch (IOException publicationFailure) {
            if (!fs.exists(completed)) {
                throw publicationFailure;
            }
            try {
                readState(completed);
            } catch (IOException invalidWinner) {
                invalidWinner.addSuppressed(publicationFailure);
                throw invalidWinner;
            }
            if (fs.exists(temporary) && !fs.delete(temporary, true)) {
                throw new IOException(
                        "Cannot remove losing Spark source attempt " + temporary,
                        publicationFailure);
            }
            temporary = null;
        }
    }

    /**
     * A Spark checkpoint directory belongs to one active application. On a driver restart, reclaim
     * only staging from previous driver/query identities; retries/speculation in the current
     * application retain their own directories. Completed intervals are never touched.
     */
    private void reclaimPreviousApplicationAttempts() throws IOException {
        if (!fs.exists(root)) {
            return;
        }
        java.util.regex.Pattern pattern =
                java.util.regex.Pattern.compile(
                        "\\.[0-9]+-([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})-"
                                + "[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}");
        for (org.apache.hadoop.fs.FileStatus status : fs.listStatus(root)) {
            if (!status.isDirectory()) {
                continue;
            }
            Path retired;
            if (status.getPath().getName().startsWith(".retired-")) {
                retired = status.getPath();
            } else {
                java.util.regex.Matcher matcher = pattern.matcher(status.getPath().getName());
                if (!matcher.matches() || attemptOwner.equals(matcher.group(1))) {
                    continue;
                }
                retired = new Path(root, ".retired-" + UUID.randomUUID());
                try {
                    if (fs instanceof DistributedFileSystem) {
                        ((DistributedFileSystem) fs)
                                .rename(status.getPath(), retired, Options.Rename.NONE);
                    } else {
                        publishLocal(localPath(status.getPath()), localPath(retired));
                    }
                } catch (java.io.FileNotFoundException | java.nio.file.NoSuchFileException absent) {
                    continue;
                }
            }
            if (!fs.delete(retired, true) && fs.exists(retired)) {
                throw new IOException("Cannot reclaim retired Spark source staging " + retired);
            }
        }
    }

    private java.nio.file.Path localPath(Path path) {
        org.apache.hadoop.fs.RawLocalFileSystem local =
                fs instanceof org.apache.hadoop.fs.LocalFileSystem
                        ? (org.apache.hadoop.fs.RawLocalFileSystem)
                                ((org.apache.hadoop.fs.LocalFileSystem) fs).getRawFileSystem()
                        : (org.apache.hadoop.fs.RawLocalFileSystem) fs;
        return local.pathToFile(path).toPath();
    }

    static void publishLocal(java.nio.file.Path attempt, java.nio.file.Path completed)
            throws IOException {
        // A published directory is nonempty. Atomic directory rename cannot replace that winner,
        // and avoids Hadoop's rename failure fallback which copies into an existing directory.
        java.nio.file.Files.move(attempt, completed, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
    }

    private static List<CatalogTable> replaceTable(
            List<CatalogTable> catalogs, CatalogTable changed) {
        List<CatalogTable> result = new ArrayList<>(catalogs);
        for (int i = 0; i < result.size(); i++) {
            if (result.get(i).getTablePath().equals(changed.getTablePath())) {
                result.set(i, changed);
                return result;
            }
        }
        throw new IllegalStateException(
                "Schema event refers to an unregistered Spark source table "
                        + changed.getTablePath());
    }

    private SparkSourceCheckpoint readCheckpoint(Path batch) throws IOException {
        try (DataInputStream metadata = new DataInputStream(fs.open(new Path(batch, "state")))) {
            return SparkSourceCheckpoint.read(metadata);
        }
    }

    private Map<Integer, List<byte[]>> readState(Path batch) throws IOException {
        SparkSourceCheckpoint metadata = readCheckpoint(batch);
        try {
            metadata.validate(parallelism, tables, true);
        } catch (IOException incompatible) {
            if (SparkSourceCatalog.restoreCheckpointSchema(source)) throw incompatible;
            metadata.validateLiveSchemaPredecessor(parallelism, tables);
            // An empty published interval contains no typed row to reinterpret. Preserve its
            // exact state and offset; nonempty old-layout intervals must still fail closed.
            Path rows = new Path(batch, "rows");
            if (fs.getFileStatus(rows).getLen() != Integer.BYTES) throw incompatible;
            try (DataInputStream input = new DataInputStream(fs.open(rows))) {
                if (input.readInt() != -1 || input.read() != -1) throw incompatible;
            }
        }
        return metadata.getState();
    }

    /**
     * Notifies completion only for an interval Spark has committed to its sink. Source checkpoint
     * callbacks may be asynchronous; their return does not prove external offset durability.
     */
    public static void acknowledge(
            SparkContext context,
            SeaTunnelSource<SeaTunnelRow, ?, ?> source,
            int parallelism,
            String jobId,
            long checkpoint,
            String location,
            String hdfsRoot,
            String user,
            Map<String, String> options,
            MultiTableManager tables)
            throws Exception {
        if (checkpoint == 0) {
            return;
        }
        int id = Math.toIntExact(checkpoint);
        List<Integer> partitions = new ArrayList<>();
        for (int i = 0; i < (source instanceof SupportCoordinate ? 1 : parallelism); i++) {
            partitions.add(i);
        }
        org.apache.spark.util.SerializableConfiguration hadoopConfiguration =
                new org.apache.spark.util.SerializableConfiguration(
                        new Configuration(context.hadoopConfiguration()));
        // JavaSparkContext wraps the existing session; ownership stays with the caller.
        new JavaSparkContext(context)
                .parallelize(partitions, partitions.size())
                .foreach(
                        partition -> {
                            try (SparkMicroBatchReader reader =
                                    new SparkMicroBatchReader(
                                            source,
                                            parallelism,
                                            jobId,
                                            partition,
                                            id,
                                            1,
                                            location,
                                            hdfsRoot,
                                            user,
                                            options,
                                            tables,
                                            hadoopConfiguration.value())) {
                                reader.acknowledge();
                            }
                        });
        commit(location, checkpoint, hdfsRoot, user, hadoopConfiguration.value());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    void acknowledge() throws Exception {
        openFileSystem();
        Path completed = batch(checkpoint);
        SparkSourceCheckpoint metadata = readCheckpoint(completed);
        if (SparkSourceCatalog.restoreCheckpointSchema(source))
            metadata.validateAcknowledgement(parallelism, tables);
        else metadata.validateLiveSchemaPredecessor(parallelism, tables);
        Map<Integer, List<byte[]>> state = metadata.getState();
        internal =
                source instanceof SupportCoordinate
                        ? new CheckpointCoordinatedSource(
                                source, state, parallelism, jobId, true, new Object())
                        : new CheckpointParallelSource(
                                source, state, parallelism, jobId, subtask, true);
        internal.open();
        ((CancellableSource) internal).acknowledgeReaders(checkpoint);
        stopSource();
    }

    /** Preserves cancellation in the unchecked exception contract of Spark commit callbacks. */
    public static RuntimeException checkpointFailure(Exception error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof InterruptedException
                    || (cause instanceof java.io.InterruptedIOException
                            && !(cause instanceof java.net.SocketTimeoutException))
                    || cause instanceof java.nio.channels.ClosedByInterruptException) {
                Thread.currentThread().interrupt();
                java.io.InterruptedIOException interrupted =
                        new java.io.InterruptedIOException(
                                "Spark checkpoint completion interrupted");
                interrupted.initCause(error);
                return new java.io.UncheckedIOException(interrupted);
            }
        }
        return new IllegalStateException("Cannot complete Spark source checkpoint", error);
    }

    /** Retains the committed interval and its predecessor; older intervals cannot be retried. */
    public static void commit(String location, long checkpoint, String hdfsRoot, String user)
            throws Exception {
        commit(location, checkpoint, hdfsRoot, user, new Configuration());
    }

    public static void commit(
            String location,
            long checkpoint,
            String hdfsRoot,
            String user,
            Configuration hadoopConfiguration)
            throws Exception {
        Configuration conf = new Configuration(hadoopConfiguration);
        conf.set("fs.defaultFS", hdfsRoot);
        Path root = new Path(location, "seatunnel-batches");
        URI uri = root.toUri().getScheme() == null ? FileSystem.getDefaultUri(conf) : root.toUri();
        org.apache.hadoop.security.UserGroupInformation identity =
                user == null || user.isEmpty()
                        ? org.apache.hadoop.security.UserGroupInformation.getCurrentUser()
                        : org.apache.hadoop.security.UserGroupInformation.getBestUGI(
                                conf.get("hadoop.security.kerberos.ticket.cache.path"), user);
        try (FileSystem fs =
                identity.doAs(
                        (java.security.PrivilegedExceptionAction<FileSystem>)
                                () -> FileSystem.newInstance(uri, conf))) {
            if (!fs.exists(root)) {
                return;
            }
            for (org.apache.hadoop.fs.FileStatus partition : fs.listStatus(root)) {
                if (!partition.isDirectory()) {
                    continue;
                }
                for (org.apache.hadoop.fs.FileStatus batch : fs.listStatus(partition.getPath())) {
                    String id = batch.getPath().getName();
                    String intervalId =
                            id.startsWith(".") && id.contains("-")
                                    ? id.substring(1, id.indexOf('-'))
                                    : id;
                    if (intervalId.matches("[0-9]+")
                            && Long.parseLong(intervalId) < checkpoint - 1) {
                        if (!fs.delete(batch.getPath(), true)) {
                            throw new IOException(
                                    "Cannot delete committed Spark interval " + batch.getPath());
                        }
                    }
                }
            }
        }
    }

    private Path batch(int id) {
        return new Path(root, Integer.toString(id));
    }

    private void checkFailure() throws IOException {
        if (cancelled) {
            throw new IOException("Spark source interval cancelled");
        }
        if (failure.get() != null) {
            throw new IOException("Spark source interval failed", failure.get());
        }
    }

    private void stopSource() throws IOException {
        sealed = true;
        IOException error = null;
        BaseSourceFunction<SeaTunnelRow> closing = internal;
        internal = null;
        try {
            if (closing != null) {
                closing.close();
            }
        } catch (Throwable e) {
            error = new IOException("Cannot close Spark source", e);
        } finally {
            error = terminate(executor, error);
        }
        if (error != null) {
            throw error;
        }
    }

    @Override
    public void close() throws IOException {
        cancelled = true;
        if (materializing) {
            BaseSourceFunction<SeaTunnelRow> active = internal;
            if (active != null) {
                ((CancellableSource) active).requestCancellation();
            }
            if (executor != null) {
                executor.shutdownNow();
            }
            return;
        }
        IOException error = null;
        try {
            stopSource();
        } catch (IOException e) {
            error = e;
        }
        try {
            if (input != null) {
                input.close();
            }
        } catch (IOException e) {
            if (error == null) {
                error = e;
            } else {
                error.addSuppressed(e);
            }
        }
        try {
            if (fs != null && temporary != null) {
                fs.delete(temporary, true);
            }
        } catch (IOException e) {
            if (error == null) {
                error = e;
            } else {
                error.addSuppressed(e);
            }
        } finally {
            try {
                if (fs != null) {
                    fs.close();
                }
            } catch (IOException e) {
                if (error == null) {
                    error = e;
                } else {
                    error.addSuppressed(e);
                }
            }
        }
        if (error != null) {
            throw error;
        }
    }

    private static IOException release(AutoCloseable resource, IOException failure) {
        try {
            if (resource != null) {
                resource.close();
            }
        } catch (Throwable error) {
            if (failure == null) {
                return new IOException("Cannot release Spark source resource", error);
            }
            failure.addSuppressed(error);
        }
        return failure;
    }

    private static IOException terminate(ExecutorService workers, IOException failure) {
        if (workers == null) {
            return failure;
        }
        workers.shutdownNow();
        try {
            if (!workers.awaitTermination(30, TimeUnit.SECONDS)) {
                throw new IOException("Spark source reader workers did not terminate");
            }
        } catch (Exception error) {
            if (error instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            if (failure == null) {
                return new IOException(error);
            }
            failure.addSuppressed(error);
        }
        return failure;
    }

    private interface CancellableSource {
        void requestCancellation() throws IOException;

        void verifyFailure() throws IOException;

        void completeRestoredEnumerator(long checkpoint) throws Exception;

        void acknowledgeReaders(long checkpoint) throws Exception;
    }

    private static final class CheckpointCoordinatedSource<
                    S extends org.apache.seatunnel.api.source.SourceSplit, T extends Serializable>
            extends CoordinatedSource<SeaTunnelRow, S, T> implements CancellableSource {
        private final boolean acknowledgeOnly;

        CheckpointCoordinatedSource(
                SeaTunnelSource<SeaTunnelRow, S, T> source,
                Map<Integer, List<byte[]>> restored,
                int parallelism,
                String jobId,
                boolean acknowledgeOnly,
                Object checkpointLock) {
            super(source, restored, parallelism, jobId);
            this.acknowledgeOnly = acknowledgeOnly;
            if (source instanceof SupportSchemaChangeWatermarks) {
                splitEnumerator = new SparkSchemaEnumerator<>(splitEnumerator, checkpointLock);
            }
        }

        @Override
        public void run(Collector<SeaTunnelRow> collector) throws Exception {
            if (splitEnumerator instanceof SparkSchemaEnumerator) {
                // Some CDC readers request their first split only from pollNext. Queue every
                // reader before the finite assignment pass so one reader cannot emit DDL while
                // a sibling's restored offset is still waiting for its first request.
                for (Integer readerId : readerMap.keySet()) {
                    splitEnumerator.handleSplitRequest(readerId);
                }
                splitEnumerator.run();
            }
            super.run(collector);
        }

        @Override
        public void requestCancellation() throws IOException {
            IOException failure = null;
            for (org.apache.seatunnel.api.source.SourceReader<SeaTunnelRow, S> reader :
                    readerMap.values()) {
                try {
                    reader.cancel();
                } catch (Throwable error) {
                    failure =
                            release(
                                    () -> {
                                        throw new IOException(error);
                                    },
                                    failure);
                }
            }
            if (failure != null) {
                throw failure;
            }
        }

        @Override
        public void verifyFailure() throws IOException {
            Throwable failure = readerFailure.get();
            if (failure != null) {
                Throwable cause = failure;
                while (cause.getCause() != null) {
                    cause = cause.getCause();
                }
                if (!(cause instanceof BatchSealed)) {
                    throw new IOException("Coordinated Spark source reader failed", failure);
                }
            }
        }

        @Override
        public void completeRestoredEnumerator(long checkpoint) throws Exception {
            splitEnumerator.snapshotState(checkpoint);
            splitEnumerator.notifyCheckpointComplete(checkpoint);
        }

        @Override
        public void acknowledgeReaders(long checkpoint) throws Exception {
            for (org.apache.seatunnel.api.source.SourceReader<SeaTunnelRow, S> reader :
                    readerMap.values()) {
                reader.snapshotState(checkpoint);
                reader.notifyCheckpointComplete(checkpoint);
            }
        }

        @Override
        public void open() throws Exception {
            if (!acknowledgeOnly) {
                super.open();
                return;
            }
            executorService = new java.util.concurrent.ScheduledThreadPoolExecutor(parallelism);
            splitEnumerator.open();
            coordinatedEnumeratorContext
                    .getEventListener()
                    .onEvent(new org.apache.seatunnel.api.source.event.EnumeratorOpenEvent());
            for (Map.Entry<Integer, org.apache.seatunnel.api.source.SourceReader<SeaTunnelRow, S>>
                    entry : readerMap.entrySet()) {
                int id = entry.getKey();
                entry.getValue().open();
                readerContextMap
                        .get(id)
                        .getEventListener()
                        .onEvent(new org.apache.seatunnel.api.source.event.ReaderOpenEvent());
                // Parallelism is fixed by the journal. Restore each reader directly, before the
                // completed interval is acknowledged and before any new source record is polled.
                List<S> splits = restoredSplitStateMap.get(id);
                if (splits != null && !splits.isEmpty()) {
                    entry.getValue().addSplits(splits);
                }
                splitEnumerator.registerReader(id);
            }
        }

        @Override
        public void close() throws IOException {
            closing = true;
            running = false;
            readerRunningMap.values().forEach(flag -> flag.set(false));
            IOException failure = null;
            for (org.apache.seatunnel.api.source.SourceReader<SeaTunnelRow, S> reader :
                    readerMap.values()) {
                try {
                    reader.cancel();
                } catch (Throwable error) {
                    failure =
                            release(
                                    () -> {
                                        throw new IOException(error);
                                    },
                                    failure);
                }
            }
            for (Map.Entry<Integer, org.apache.seatunnel.api.source.SourceReader<SeaTunnelRow, S>>
                    entry : readerMap.entrySet()) {
                failure = release(entry.getValue(), failure);
                final int id = entry.getKey();
                failure =
                        release(
                                () ->
                                        readerContextMap
                                                .get(id)
                                                .getEventListener()
                                                .onEvent(
                                                        new org.apache.seatunnel.api.source.event
                                                                .ReaderCloseEvent()),
                                failure);
            }
            failure = release(splitEnumerator, failure);
            failure =
                    release(
                            () ->
                                    coordinatedEnumeratorContext
                                            .getEventListener()
                                            .onEvent(
                                                    new org.apache.seatunnel.api.source.event
                                                            .EnumeratorCloseEvent()),
                            failure);
            failure = terminate(executorService, failure);
            if (failure != null) {
                throw failure;
            }
        }
    }

    private static final class CheckpointParallelSource<
                    S extends org.apache.seatunnel.api.source.SourceSplit, T extends Serializable>
            extends ParallelSource<SeaTunnelRow, S, T> implements CancellableSource {
        private final boolean acknowledgeOnly;

        CheckpointParallelSource(
                SeaTunnelSource<SeaTunnelRow, S, T> source,
                Map<Integer, List<byte[]>> restored,
                int parallelism,
                String jobId,
                int subtask,
                boolean acknowledgeOnly) {
            super(source, restored, parallelism, jobId, subtask);
            this.acknowledgeOnly = acknowledgeOnly;
        }

        @Override
        public void requestCancellation() throws IOException {
            try {
                reader.cancel();
            } catch (Throwable error) {
                throw new IOException(error);
            }
        }

        @Override
        public void verifyFailure() {}

        @Override
        public void completeRestoredEnumerator(long checkpoint) throws Exception {
            splitEnumerator.snapshotState(checkpoint);
            splitEnumerator.notifyCheckpointComplete(checkpoint);
        }

        @Override
        public void acknowledgeReaders(long checkpoint) throws Exception {
            reader.snapshotState(checkpoint);
            reader.notifyCheckpointComplete(checkpoint);
        }

        @Override
        public void open() throws Exception {
            if (!acknowledgeOnly) {
                super.open();
                return;
            }
            executorService = new java.util.concurrent.ScheduledThreadPoolExecutor(1);
            splitEnumerator.open();
            reader.open();
            readerContext
                    .getEventListener()
                    .onEvent(new org.apache.seatunnel.api.source.event.ReaderOpenEvent());
            if (!restoredSplitState.isEmpty()) {
                reader.addSplits(restoredSplitState);
            }
            parallelEnumeratorContext.register();
            parallelEnumeratorContext
                    .getEventListener()
                    .onEvent(new org.apache.seatunnel.api.source.event.EnumeratorOpenEvent());
            splitEnumerator.registerReader(subtaskId);
        }

        @Override
        public void close() throws IOException {
            handleNoMoreElement();
            IOException failure = null;
            try {
                reader.cancel();
            } catch (Throwable error) {
                failure = new IOException(error);
            }
            failure = release(splitEnumerator, failure);
            failure = release(reader, failure);
            failure =
                    release(
                            () ->
                                    readerContext
                                            .getEventListener()
                                            .onEvent(
                                                    new org.apache.seatunnel.api.source.event
                                                            .ReaderCloseEvent()),
                            failure);
            failure =
                    release(
                            () ->
                                    parallelEnumeratorContext
                                            .getEventListener()
                                            .onEvent(
                                                    new org.apache.seatunnel.api.source.event
                                                            .EnumeratorCloseEvent()),
                            failure);
            failure = terminate(executorService, failure);
            if (failure != null) {
                throw failure;
            }
        }
    }

    private static final class BatchSealed extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
}
