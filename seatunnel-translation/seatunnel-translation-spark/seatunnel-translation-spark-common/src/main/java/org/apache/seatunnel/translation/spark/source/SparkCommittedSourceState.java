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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Binds a source catalog to Spark's durable completed epoch, rather than its pending offset. */
public final class SparkCommittedSourceState {
    private static final long MAX_LOG_BYTES = 1024 * 1024;
    private final long epoch;
    private final long sourceCheckpointId;
    private final SparkSourceCheckpoint checkpoint;

    private SparkCommittedSourceState(
            long epoch, long sourceCheckpointId, SparkSourceCheckpoint checkpoint) {
        this.epoch = epoch;
        this.sourceCheckpointId = sourceCheckpointId;
        this.checkpoint = checkpoint;
    }

    public long getEpoch() {
        return epoch;
    }

    public long getSourceCheckpointId() {
        return sourceCheckpointId;
    }

    public SparkSourceCheckpoint getCheckpoint() {
        return checkpoint;
    }

    /**
     * Reads the exact source offset of the latest completed Spark epoch. The source ordinal is
     * Spark's ordinal inside this query, not the logical source index in the SeaTunnel job.
     * Checkpoint directories are dedicated to an unchanged query topology.
     */
    public static Optional<SparkCommittedSourceState> load(
            Configuration configuration,
            String queryCheckpoint,
            int sourceOrdinal,
            int sourceCount,
            String user)
            throws Exception {
        if (sourceOrdinal < 0 || sourceCount < 1 || sourceOrdinal >= sourceCount) {
            throw new IllegalArgumentException("Invalid Spark query source ordinal or count");
        }
        Path root = new Path(queryCheckpoint);
        URI uri =
                root.toUri().getScheme() == null
                        ? FileSystem.getDefaultUri(configuration)
                        : root.toUri();
        UserGroupInformation identity =
                user == null || user.isEmpty()
                        ? UserGroupInformation.getCurrentUser()
                        : UserGroupInformation.getBestUGI(
                                configuration.get("hadoop.security.kerberos.ticket.cache.path"),
                                user);
        return identity.doAs(
                (PrivilegedExceptionAction<Optional<SparkCommittedSourceState>>)
                        () -> {
                            try (FileSystem fs = FileSystem.newInstance(uri, configuration)) {
                                Path commits = new Path(root, "commits");
                                if (!fs.exists(commits)) return Optional.empty();
                                long epoch = -1;
                                for (FileStatus file : fs.listStatus(commits)) {
                                    String name = file.getPath().getName();
                                    if (!file.isFile() || !name.matches("0|[1-9][0-9]*")) continue;
                                    try {
                                        epoch = Math.max(epoch, Long.parseLong(name));
                                    } catch (NumberFormatException malformed) {
                                        throw new IOException(
                                                "Invalid Spark commit epoch", malformed);
                                    }
                                }
                                if (epoch < 0) return Optional.empty();
                                List<String> commit =
                                        readLog(fs, new Path(commits, Long.toString(epoch)));
                                if (commit.size() != 2 || !commit.get(0).equals("v1")) {
                                    throw new IOException("Unsupported Spark commit log format");
                                }
                                // Validate the immutable marker before trusting the matching offset
                                // file.
                                parseObject(commit.get(1));
                                List<String> offsets =
                                        readLog(fs, new Path(root, "offsets/" + epoch));
                                if (offsets.size() != sourceCount + 2
                                        || !offsets.get(0).equals("v1")) {
                                    throw new IOException(
                                            "Incompatible Spark source count or offset log format");
                                }
                                parseObject(offsets.get(1));
                                String offset = offsets.get(sourceOrdinal + 2);
                                if (offset.equals("-")) return Optional.empty();
                                long checkpointId = SparkCheckpointOffset.checkpointId(offset);
                                if (checkpointId < 1)
                                    throw new IOException("Invalid SeaTunnel source checkpoint id");
                                SparkSourceCheckpoint checkpoint =
                                        SparkSourceCheckpoint.load(
                                                        configuration,
                                                        new Path(root, "sources/" + sourceOrdinal)
                                                                .toString(),
                                                        0,
                                                        checkpointId,
                                                        user)
                                                .orElseThrow(
                                                        () ->
                                                                new IOException(
                                                                        "Spark committed without its source journal"));
                                return Optional.of(
                                        new SparkCommittedSourceState(
                                                epoch, checkpointId, checkpoint));
                            }
                        });
    }

    private static List<String> readLog(FileSystem fs, Path path) throws IOException {
        if (fs.getFileStatus(path).getLen() > MAX_LOG_BYTES) {
            throw new IOException("Spark checkpoint log exceeds size limit");
        }
        List<String> lines = new ArrayList<>();
        try (BufferedReader input =
                new BufferedReader(new InputStreamReader(fs.open(path), StandardCharsets.UTF_8))) {
            String line;
            while ((line = input.readLine()) != null) lines.add(line);
        }
        return lines;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> parseObject(String json) throws IOException {
        try {
            Map<String, Object> result = JsonUtils.parseObject(json, Map.class);
            if (result == null) throw new IOException("Missing Spark checkpoint log object");
            return result;
        } catch (RuntimeException malformed) {
            throw new IOException("Invalid Spark checkpoint log object", malformed);
        }
    }
}
