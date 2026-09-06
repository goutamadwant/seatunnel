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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;

public class AzureEventHubsSinkOptions extends ConnectorCommonOptions {

    public static final String CONNECTOR_IDENTITY = AzureEventHubsSourceOptions.CONNECTOR_IDENTITY;

    public static final Option<String> CONNECTION_STRING =
            AzureEventHubsSourceOptions.CONNECTION_STRING;
    public static final Option<String> EVENT_HUB_NAME = AzureEventHubsSourceOptions.EVENT_HUB_NAME;
    public static final Option<AzureEventHubsMessageFormat> FORMAT =
            AzureEventHubsSourceOptions.FORMAT;
    public static final Option<String> FIELD_DELIMITER =
            AzureEventHubsSourceOptions.FIELD_DELIMITER;

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "Maximum events buffered before a synchronous send. The SDK byte limit may flush earlier. Checkpoints and final commit flush partial batches.");

    public static final Option<String> PARTITION_KEY =
            Options.key("partition_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional static partition key for all events, at most 128 UTF-16 code units. Cannot be used with partition_id.");

    public static final Option<String> PARTITION_ID =
            Options.key("partition_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional fixed destination partition. Cannot be used with partition_key.");

    private AzureEventHubsSinkOptions() {}
}
