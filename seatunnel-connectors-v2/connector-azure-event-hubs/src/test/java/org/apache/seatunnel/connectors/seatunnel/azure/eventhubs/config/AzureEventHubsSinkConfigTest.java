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
import org.apache.seatunnel.common.utils.SerializationUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class AzureEventHubsSinkConfigTest {

    @Test
    void defaultsUseJsonBoundedCountAndSdkRouting() {
        AzureEventHubsSinkConfig config = config(validOptions());

        Assertions.assertEquals("events", config.getEventHubName());
        Assertions.assertEquals(AzureEventHubsMessageFormat.JSON, config.getFormat());
        Assertions.assertEquals(",", config.getFieldDelimiter());
        Assertions.assertEquals(100, config.getBatchSize());
        Assertions.assertNull(config.getPartitionKey());
        Assertions.assertNull(config.getPartitionId());
        Assertions.assertSame(
                AzureEventHubsSourceOptions.CONNECTION_STRING,
                AzureEventHubsSinkOptions.CONNECTION_STRING);
        Assertions.assertSame(
                AzureEventHubsSourceOptions.EVENT_HUB_NAME,
                AzureEventHubsSinkOptions.EVENT_HUB_NAME);
        Assertions.assertSame(AzureEventHubsSourceOptions.FORMAT, AzureEventHubsSinkOptions.FORMAT);
        Assertions.assertSame(
                AzureEventHubsSourceOptions.FIELD_DELIMITER,
                AzureEventHubsSinkOptions.FIELD_DELIMITER);
    }

    @Test
    void configurationSurvivesSerialization() {
        Map<String, Object> options = validOptions();
        options.put("format", "text");
        options.put("field_delimiter", "|");
        options.put("batch_size", 2);
        options.put("partition_key", "seatunnel");
        AzureEventHubsSinkConfig config =
                SerializationUtils.deserialize(SerializationUtils.serialize(config(options)));

        Assertions.assertEquals(options.get("connection_string"), config.getConnectionString());
        Assertions.assertEquals(AzureEventHubsMessageFormat.TEXT, config.getFormat());
        Assertions.assertEquals("|", config.getFieldDelimiter());
        Assertions.assertEquals(2, config.getBatchSize());
        Assertions.assertEquals("seatunnel", config.getPartitionKey());
    }

    @Test
    void requiredValuesCannotBeMissingOrBlank() {
        for (String key : new String[] {"connection_string", "event_hub_name"}) {
            Map<String, Object> missing = validOptions();
            missing.remove(key);
            Assertions.assertThrows(RuntimeException.class, () -> config(missing));
            for (String value : new String[] {"", "  "}) {
                assertInvalid(key, value);
            }
        }
    }

    @Test
    void connectionValidationMatchesSourceWithoutLeakingCredentials() {
        for (String entityPath : new String[] {"EntityPath", "EnTiTyPaTh", " EntityPath "}) {
            Map<String, Object> options = validOptions();
            options.put(
                    "connection_string",
                    options.get("connection_string") + entityPath + "=events;");
            IllegalArgumentException error =
                    Assertions.assertThrows(IllegalArgumentException.class, () -> config(options));
            Assertions.assertTrue(error.getMessage().contains("must not include EntityPath"));
            Assertions.assertFalse(error.getMessage().contains("private-key"));
        }
        Map<String, Object> options = validOptions();
        options.put(
                "connection_string",
                "Endpoint=sb://example/;SharedAccessKeyName=EntityPathUser;SharedAccessKey=private-key;");
        Assertions.assertDoesNotThrow(() -> config(options));
    }

    @Test
    void rejectsNonPositiveBatchCounts() {
        assertInvalid("batch_size", 0);
        assertInvalid("batch_size", -1);
    }

    @Test
    void partitionSettingsAreOptionalButNotBlank() {
        for (String key : new String[] {"partition_key", "partition_id"}) {
            assertInvalid(key, "");
            assertInvalid(key, "  ");
        }
    }

    @Test
    void partitionKeyLengthMatchesSdkJavaStringBoundaryRatherThanUtf8Bytes() {
        for (String character : new String[] {"a", "\u6f22"}) {
            String key = String.join("", Collections.nCopies(128, character));
            Map<String, Object> options = validOptions();
            options.put("partition_key", key);
            Assertions.assertEquals(key, config(options).getPartitionKey());
            assertInvalid("partition_key", key + character);
        }
    }

    @Test
    void supplementaryCharactersCountAsTwoUtf16CodeUnits() {
        String key = String.join("", Collections.nCopies(64, "\uD83D\uDE00"));
        Assertions.assertEquals(128, key.length());
        Map<String, Object> options = validOptions();
        options.put("partition_key", key);
        Assertions.assertEquals(key, config(options).getPartitionKey());
        assertInvalid("partition_key", key + "a");
    }

    @Test
    void cannotCombineStaticPartitionKeyWithPartitionId() {
        Map<String, Object> options = validOptions();
        options.put("partition_key", "seatunnel");
        options.put("partition_id", "0");

        IllegalArgumentException error =
                Assertions.assertThrows(IllegalArgumentException.class, () -> config(options));
        Assertions.assertTrue(error.getMessage().contains("mutually exclusive"));
    }

    @Test
    void preservesStaticRoutingValuesWithoutTrimming() {
        Map<String, Object> options = validOptions();
        options.put("partition_key", " key ");
        Assertions.assertEquals(" key ", config(options).getPartitionKey());
        options.remove("partition_key");
        options.put("partition_id", "0");
        Assertions.assertEquals("0", config(options).getPartitionId());
    }

    @Test
    void validatesFormatAndTextDelimiter() {
        Assertions.assertThrows(
                RuntimeException.class,
                () -> {
                    Map<String, Object> options = validOptions();
                    options.put("format", "avro");
                    config(options);
                });
        Map<String, Object> options = validOptions();
        options.put("format", "text");
        options.put("field_delimiter", "");
        IllegalArgumentException error =
                Assertions.assertThrows(IllegalArgumentException.class, () -> config(options));
        Assertions.assertTrue(error.getMessage().contains("field_delimiter"));
        options.put("field_delimiter", " ");
        Assertions.assertEquals(" ", config(options).getFieldDelimiter());
        options.put("format", "json");
        options.put("field_delimiter", "");
        Assertions.assertDoesNotThrow(() -> config(options));
    }

    @Test
    void sinkDoesNotApplySourceOnlyDefaultsOrValidation() {
        Map<String, Object> options = validOptions();
        options.put("consumer_group", "");
        options.put("max_batch_size", -1);
        options.put("prefetch_count", 0);
        options.put("poll_timeout_ms", -1L);

        Assertions.assertEquals(100, config(options).getBatchSize());
    }

    private void assertInvalid(String key, Object value) {
        Map<String, Object> options = validOptions();
        options.put(key, value);
        IllegalArgumentException error =
                Assertions.assertThrows(IllegalArgumentException.class, () -> config(options));
        Assertions.assertTrue(error.getMessage().contains(key));
    }

    private AzureEventHubsSinkConfig config(Map<String, Object> options) {
        return AzureEventHubsSinkConfig.from(ReadonlyConfig.fromMap(options));
    }

    private Map<String, Object> validOptions() {
        Map<String, Object> options = new HashMap<>();
        options.put(
                "connection_string",
                "Endpoint=sb://example/;SharedAccessKeyName=name;SharedAccessKey=private-key;");
        options.put("event_hub_name", "events");
        return options;
    }
}
