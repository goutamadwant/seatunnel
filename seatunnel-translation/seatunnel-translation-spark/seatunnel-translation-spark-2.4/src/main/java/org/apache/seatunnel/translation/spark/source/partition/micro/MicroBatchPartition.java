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

package org.apache.seatunnel.translation.spark.source.partition.micro;

import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.spark.execution.MultiTableManager;
import org.apache.seatunnel.translation.spark.source.SparkMicroBatchReader;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;

import java.util.Map;

public class MicroBatchPartition implements InputPartition<InternalRow> {
    protected final SeaTunnelSource<SeaTunnelRow, ?, ?> source;
    protected final Integer parallelism;
    protected final String jobId;
    protected final Integer subtaskId;
    protected final Integer checkpointId;
    protected final Integer checkpointInterval;
    protected final String checkpointPath;
    protected final String hdfsRoot;
    protected final String hdfsUser;
    private Map<String, String> envOptions;

    protected final MultiTableManager multiTableManager;
    private final org.apache.spark.util.SerializableConfiguration hadoopConfiguration;
    private final String driverOwner;

    public MicroBatchPartition(
            SeaTunnelSource<SeaTunnelRow, ?, ?> source,
            Integer parallelism,
            String jobId,
            Integer subtaskId,
            Integer checkpointId,
            Integer checkpointInterval,
            String checkpointPath,
            String hdfsRoot,
            String hdfsUser,
            Map<String, String> envOptions,
            MultiTableManager multiTableManager) {
        this(
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
                new org.apache.hadoop.conf.Configuration());
    }

    public MicroBatchPartition(
            SeaTunnelSource<SeaTunnelRow, ?, ?> source,
            Integer parallelism,
            String jobId,
            Integer subtaskId,
            Integer checkpointId,
            Integer checkpointInterval,
            String checkpointPath,
            String hdfsRoot,
            String hdfsUser,
            Map<String, String> envOptions,
            MultiTableManager multiTableManager,
            org.apache.hadoop.conf.Configuration configuration) {
        this(
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
                configuration,
                jobId);
    }

    public MicroBatchPartition(
            SeaTunnelSource<SeaTunnelRow, ?, ?> source,
            Integer parallelism,
            String jobId,
            Integer subtaskId,
            Integer checkpointId,
            Integer checkpointInterval,
            String checkpointPath,
            String hdfsRoot,
            String hdfsUser,
            Map<String, String> envOptions,
            MultiTableManager multiTableManager,
            org.apache.hadoop.conf.Configuration configuration,
            String driverOwner) {
        this.hadoopConfiguration =
                new org.apache.spark.util.SerializableConfiguration(
                        new org.apache.hadoop.conf.Configuration(configuration));
        this.driverOwner = driverOwner;
        this.source = source;
        this.parallelism = parallelism;
        this.jobId = jobId;
        this.subtaskId = subtaskId;
        this.checkpointId = checkpointId;
        this.checkpointInterval = checkpointInterval;
        this.checkpointPath = checkpointPath;
        this.hdfsRoot = hdfsRoot;
        this.hdfsUser = hdfsUser;
        this.envOptions = envOptions;
        this.multiTableManager = multiTableManager;
    }

    @Override
    public InputPartitionReader<InternalRow> createPartitionReader() {
        SparkMicroBatchReader reader =
                new SparkMicroBatchReader(
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
                        hadoopConfiguration.value(),
                        driverOwner);
        return new InputPartitionReader<InternalRow>() {
            @Override
            public boolean next() throws java.io.IOException {
                try {
                    return reader.next();
                } catch (Exception e) {
                    throw new java.io.IOException(e);
                }
            }

            @Override
            public InternalRow get() {
                return reader.get();
            }

            @Override
            public void close() throws java.io.IOException {
                reader.close();
            }
        };
    }
}
