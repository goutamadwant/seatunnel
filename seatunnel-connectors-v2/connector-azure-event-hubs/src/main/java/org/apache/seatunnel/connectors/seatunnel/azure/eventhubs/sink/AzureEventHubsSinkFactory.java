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

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsSinkOptions;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class AzureEventHubsSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return AzureEventHubsSinkOptions.CONNECTOR_IDENTITY;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        AzureEventHubsSinkOptions.CONNECTION_STRING,
                        AzureEventHubsSinkOptions.EVENT_HUB_NAME)
                .optional(
                        AzureEventHubsSinkOptions.FORMAT,
                        AzureEventHubsSinkOptions.FIELD_DELIMITER,
                        AzureEventHubsSinkOptions.BATCH_SIZE,
                        AzureEventHubsSinkOptions.PARTITION_KEY,
                        AzureEventHubsSinkOptions.PARTITION_ID)
                .build();
    }

    /** Validates options before dispatch; clients are only constructed by worker-side writers. */
    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        AzureEventHubsSinkConfig config = AzureEventHubsSinkConfig.from(context.getOptions());
        CatalogTable catalogTable = context.getCatalogTable();
        return () -> new AzureEventHubsSink(config, catalogTable);
    }
}
