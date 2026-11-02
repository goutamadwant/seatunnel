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

package org.apache.seatunnel.translation.spark.source.reader.micro;

import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SupportCoordinate;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.spark.execution.MultiTableManager;
import org.apache.seatunnel.translation.spark.source.SparkMicroBatchReader;
import org.apache.seatunnel.translation.spark.source.SparkSourceCheckpoint;
import org.apache.seatunnel.translation.spark.source.partition.micro.MicroBatchPartition;
import org.apache.seatunnel.translation.spark.source.state.MicroBatchState;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MicroBatchSourceReader implements MicroBatchReader {

    protected final SeaTunnelSource<SeaTunnelRow, ?, ?> source;
    protected final Integer parallelism;
    protected final String jobId;

    protected final Integer checkpointInterval;
    protected final String checkpointPath;
    protected final String hdfsRoot;
    protected final String hdfsUser;
    protected Integer checkpointId;
    protected MicroBatchState startOffset;
    protected MicroBatchState endOffset;
    private Map<String, String> envOptions;
    private final MultiTableManager multiTableManager;
    private final String driverOwner = java.util.UUID.randomUUID().toString();

    public MicroBatchSourceReader(
            SeaTunnelSource<SeaTunnelRow, ?, ?> source,
            Integer parallelism,
            String jobId,
            Integer checkpointId,
            Integer checkpointInterval,
            String checkpointPath,
            String hdfsRoot,
            String hdfsUser,
            Map<String, String> envOptions,
            MultiTableManager multiTableManager) {
        this.source = source;
        this.parallelism = parallelism;
        this.jobId = jobId;
        this.checkpointId = checkpointId;
        this.checkpointInterval = checkpointInterval;
        this.checkpointPath = checkpointPath;
        this.hdfsRoot = hdfsRoot;
        this.hdfsUser = hdfsUser;
        this.envOptions = envOptions;
        this.multiTableManager = multiTableManager;
    }

    @Override
    public void setOffsetRange(Optional<Offset> start, Optional<Offset> end) {
        startOffset = (MicroBatchState) start.orElse(new MicroBatchState(0));
        this.checkpointId = Math.addExact(startOffset.getCheckpointId(), 1);
        boolean pause = false;
        if (!end.isPresent()
                && source
                        instanceof org.apache.seatunnel.api.source.SupportSchemaChangeWatermarks) {
            try {
                org.apache.hadoop.conf.Configuration configuration =
                        new org.apache.hadoop.conf.Configuration(
                                org.apache.spark.sql.SparkSession.getActiveSession()
                                        .get()
                                        .sparkContext()
                                        .hadoopConfiguration());
                configuration.set("fs.defaultFS", hdfsRoot);
                pause =
                        SparkSourceCheckpoint.shouldPause(
                                configuration,
                                checkpointPath,
                                startOffset.getCheckpointId(),
                                hdfsUser,
                                multiTableManager,
                                Long.parseLong(
                                        envOptions.getOrDefault(
                                                org.apache.seatunnel.translation.spark.source
                                                        .SparkSourceCatalog.APPLIED_BOUNDARY,
                                                "-1")));
            } catch (Exception error) {
                throw SparkMicroBatchReader.checkpointFailure(error);
            }
        }
        endOffset =
                (MicroBatchState)
                        end.orElse(
                                new MicroBatchState(
                                        pause ? startOffset.getCheckpointId() : checkpointId));
        if (startOffset.getCheckpointId() < 0
                || (endOffset.getCheckpointId().intValue() != checkpointId.intValue() && !pause)) {
            throw new IllegalArgumentException("Expected one positive Spark checkpoint interval");
        }
    }

    @Override
    public Offset getStartOffset() {
        return startOffset;
    }

    @Override
    public Offset getEndOffset() {
        return endOffset;
    }

    @Override
    public Offset deserializeOffset(String microBatchState) {
        try {
            return new MicroBatchState(
                    Math.toIntExact(
                            org.apache.seatunnel.translation.spark.source.SparkCheckpointOffset
                                    .checkpointId(microBatchState)));
        } catch (java.io.IOException | ArithmeticException error) {
            throw SparkMicroBatchReader.checkpointFailure(error);
        }
    }

    @Override
    public void commit(Offset end) {
        try {
            SparkMicroBatchReader.acknowledge(
                    org.apache.spark.sql.SparkSession.getActiveSession().get().sparkContext(),
                    source,
                    parallelism,
                    jobId,
                    ((MicroBatchState) end).getCheckpointId(),
                    checkpointPath,
                    hdfsRoot,
                    hdfsUser,
                    envOptions,
                    multiTableManager);
        } catch (Exception e) {
            throw SparkMicroBatchReader.checkpointFailure(e);
        }
    }

    @Override
    public void stop() {
        // nothing
    }

    @Override
    public StructType readSchema() {
        return multiTableManager.getTableSchema();
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {
        List<InputPartition<InternalRow>> virtualPartitions;
        if (source instanceof SupportCoordinate) {
            virtualPartitions = new ArrayList<>(1);
            virtualPartitions.add(
                    new MicroBatchPartition(
                            source,
                            parallelism,
                            jobId,
                            0,
                            checkpointId,
                            checkpointInterval,
                            checkpointPath,
                            hdfsRoot,
                            hdfsUser,
                            envOptions,
                            multiTableManager,
                            org.apache.spark.sql.SparkSession.getActiveSession()
                                    .get()
                                    .sparkContext()
                                    .hadoopConfiguration(),
                            driverOwner));
        } else {
            virtualPartitions = new ArrayList<>(parallelism);
            for (int subtaskId = 0; subtaskId < parallelism; subtaskId++) {
                virtualPartitions.add(
                        new MicroBatchPartition(
                                source,
                                parallelism,
                                jobId,
                                subtaskId,
                                checkpointId,
                                checkpointInterval,
                                checkpointPath,
                                hdfsRoot,
                                hdfsUser,
                                envOptions,
                                multiTableManager,
                                org.apache.spark.sql.SparkSession.getActiveSession()
                                        .get()
                                        .sparkContext()
                                        .hadoopConfiguration(),
                                driverOwner));
            }
        }
        return virtualPartitions;
    }
}
