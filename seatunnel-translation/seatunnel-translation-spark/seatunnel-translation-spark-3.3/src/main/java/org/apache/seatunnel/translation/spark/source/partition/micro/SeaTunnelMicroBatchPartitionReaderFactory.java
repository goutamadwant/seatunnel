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
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class SeaTunnelMicroBatchPartitionReaderFactory implements PartitionReaderFactory {

    private final SeaTunnelSource<SeaTunnelRow, ?, ?> source;

    private final int parallelism;
    private final String jobId;

    private final String checkpointLocation;

    private final java.util.Map<String, String> envOptions;

    private final MultiTableManager multiTableManager;
    private final org.apache.spark.util.SerializableConfiguration hadoopConfiguration;
    private final String driverOwner;

    public SeaTunnelMicroBatchPartitionReaderFactory(
            SeaTunnelSource<SeaTunnelRow, ?, ?> source,
            int parallelism,
            String jobId,
            String checkpointLocation,
            CaseInsensitiveStringMap caseInsensitiveStringMap,
            MultiTableManager multiTableManager) {
        this(
                source,
                parallelism,
                jobId,
                checkpointLocation,
                caseInsensitiveStringMap,
                multiTableManager,
                new org.apache.hadoop.conf.Configuration());
    }

    public SeaTunnelMicroBatchPartitionReaderFactory(
            SeaTunnelSource<SeaTunnelRow, ?, ?> source,
            int parallelism,
            String jobId,
            String checkpointLocation,
            CaseInsensitiveStringMap caseInsensitiveStringMap,
            MultiTableManager multiTableManager,
            org.apache.hadoop.conf.Configuration configuration) {
        this(
                source,
                parallelism,
                jobId,
                checkpointLocation,
                caseInsensitiveStringMap,
                multiTableManager,
                configuration,
                jobId);
    }

    public SeaTunnelMicroBatchPartitionReaderFactory(
            SeaTunnelSource<SeaTunnelRow, ?, ?> source,
            int parallelism,
            String jobId,
            String checkpointLocation,
            CaseInsensitiveStringMap caseInsensitiveStringMap,
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
        this.checkpointLocation = checkpointLocation;
        this.envOptions = new java.util.HashMap<>(caseInsensitiveStringMap.asCaseSensitiveMap());
        this.multiTableManager = multiTableManager;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        SeaTunnelMicroBatchInputPartition seaTunnelPartition =
                (SeaTunnelMicroBatchInputPartition) partition;
        SparkMicroBatchReader reader =
                new SparkMicroBatchReader(
                        source,
                        parallelism,
                        jobId,
                        seaTunnelPartition.getSubtaskId(),
                        seaTunnelPartition.getCheckpointId(),
                        seaTunnelPartition.getCheckpointInterval(),
                        seaTunnelPartition.getCheckpointPath(),
                        seaTunnelPartition.getHdfsRoot(),
                        seaTunnelPartition.getHdfsUser(),
                        envOptions,
                        multiTableManager,
                        hadoopConfiguration.value(),
                        driverOwner);
        return new PartitionReader<InternalRow>() {
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
