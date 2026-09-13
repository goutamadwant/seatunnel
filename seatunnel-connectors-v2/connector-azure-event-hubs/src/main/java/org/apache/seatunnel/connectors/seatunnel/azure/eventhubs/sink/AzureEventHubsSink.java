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
package org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.sink;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.exception.AzureEventHubsConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.exception.AzureEventHubsConnectorException;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import com.azure.messaging.eventhubs.EventHubProducerClient;

import java.util.Optional;

/**
 * Insert-only, at-least-once sink. Acknowledged events may be replayed after recovery; Event Hubs
 * sends are not transactions tied to SeaTunnel checkpoints.
 */
public class AzureEventHubsSink extends AbstractSimpleSink<SeaTunnelRow, Void> {

    private static final long serialVersionUID = 1L;

    private final AzureEventHubsSinkConfig config;
    private final CatalogTable catalogTable;
    private final EventHubsProducerFactory producerFactory;

    public AzureEventHubsSink(AzureEventHubsSinkConfig config, CatalogTable catalogTable) {
        this(config, catalogTable, new DefaultProducerFactory());
    }

    AzureEventHubsSink(
            AzureEventHubsSinkConfig config,
            CatalogTable catalogTable,
            EventHubsProducerFactory producerFactory) {
        this.config = config;
        this.catalogTable = catalogTable;
        this.producerFactory = producerFactory;
    }

    @Override
    public String getPluginName() {
        return AzureEventHubsSinkOptions.CONNECTOR_IDENTITY;
    }

    /** Creates one long-lived producer on the worker, including when restoring a failed task. */
    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context) {
        return new AzureEventHubsSinkWriter(
                catalogTable.getSeaTunnelRowType(), config, producerFactory);
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.of(catalogTable);
    }

    /** Requires checkpoints to flush low-volume streams because the writer has no timer thread. */
    @Override
    public void setJobContext(JobContext jobContext) {
        if (JobMode.STREAMING.equals(jobContext.getJobMode()) && !jobContext.isEnableCheckpoint()) {
            throw new AzureEventHubsConnectorException(
                    AzureEventHubsConnectorErrorCode.CONFIGURATION_FAILED,
                    "Azure Event Hubs sink requires checkpointing for streaming jobs to flush partial batches");
        }
    }

    /** Avoids serialized lambda method descriptors containing SDK types relocated by shading. */
    private static final class DefaultProducerFactory implements EventHubsProducerFactory {

        private static final long serialVersionUID = 1L;

        @Override
        public EventHubProducerClient create(AzureEventHubsSinkConfig config) {
            return AzureEventHubsSinkWriter.createProducer(config);
        }
    }
}
