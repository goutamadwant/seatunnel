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
package org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsConfigValidator.rejectEntityPath;
import static org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsConfigValidator.requireNonBlank;

/** Immutable, validated configuration shared by a sink's writers. */
@Getter
@Builder(access = AccessLevel.PRIVATE)
public class AzureEventHubsSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String connectionString;
    private final String eventHubName;
    private final AzureEventHubsMessageFormat format;
    private final String fieldDelimiter;
    private final int batchSize;
    private final String partitionKey;
    private final String partitionId;

    public static AzureEventHubsSinkConfig from(ReadonlyConfig config) {
        AzureEventHubsSinkConfig sinkConfig =
                AzureEventHubsSinkConfig.builder()
                        .connectionString(config.get(AzureEventHubsSinkOptions.CONNECTION_STRING))
                        .eventHubName(config.get(AzureEventHubsSinkOptions.EVENT_HUB_NAME))
                        .format(config.get(AzureEventHubsSinkOptions.FORMAT))
                        .fieldDelimiter(config.get(AzureEventHubsSinkOptions.FIELD_DELIMITER))
                        .batchSize(config.get(AzureEventHubsSinkOptions.BATCH_SIZE))
                        .partitionKey(config.get(AzureEventHubsSinkOptions.PARTITION_KEY))
                        .partitionId(config.get(AzureEventHubsSinkOptions.PARTITION_ID))
                        .build();
        sinkConfig.validate();
        return sinkConfig;
    }

    private void validate() {
        requireNonBlank(connectionString, AzureEventHubsSinkOptions.CONNECTION_STRING.key());
        requireNonBlank(eventHubName, AzureEventHubsSinkOptions.EVENT_HUB_NAME.key());
        rejectEntityPath(connectionString);
        if (format == AzureEventHubsMessageFormat.TEXT && fieldDelimiter.isEmpty()) {
            throw new IllegalArgumentException("Option 'field_delimiter' cannot be empty");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Option 'batch_size' must be greater than zero");
        }
        if (partitionKey != null) {
            requireNonBlank(partitionKey, AzureEventHubsSinkOptions.PARTITION_KEY.key());
            if (partitionKey.length() > 128) {
                throw new IllegalArgumentException(
                        "Option 'partition_key' must not exceed 128 UTF-16 code units");
            }
        }
        if (partitionId != null) {
            requireNonBlank(partitionId, AzureEventHubsSinkOptions.PARTITION_ID.key());
        }
        if (partitionKey != null && partitionId != null) {
            throw new IllegalArgumentException(
                    "Options 'partition_key' and 'partition_id' are mutually exclusive");
        }
    }
}
