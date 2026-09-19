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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsSinkConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Runs after packaging to exercise relocated SDK method descriptors, without Azure access. */
class TestAzureEventHubsSinkSerializationIT {

    @Test
    void shadedSinkRoundTripsBetweenWorkerClassLoaders() throws Exception {
        Path target =
                Paths.get(
                                AzureEventHubsSink.class
                                        .getProtectionDomain()
                                        .getCodeSource()
                                        .getLocation()
                                        .toURI())
                        .getParent();
        List<Path> artifacts;
        try (Stream<Path> files = Files.list(target)) {
            artifacts =
                    files.filter(
                                    path -> {
                                        String name = path.getFileName().toString();
                                        return name.startsWith("connector-azure-event-hubs-")
                                                && name.endsWith(".jar")
                                                && !name.endsWith("-sources.jar")
                                                && !name.endsWith("-javadoc.jar")
                                                && !name.endsWith("-tests.jar");
                                    })
                            .collect(Collectors.toList());
        }
        Assertions.assertEquals(1, artifacts.size(), "Expected the packaged connector JAR");
        URL artifact = artifacts.get(0).toUri().toURL();
        try (ConnectorClassLoader writerLoader = new ConnectorClassLoader(artifact);
                ConnectorClassLoader readerLoader = new ConnectorClassLoader(artifact)) {
            Class<?> sinkType = writerLoader.loadClass(AzureEventHubsSink.class.getName());
            Class<?> configType = writerLoader.loadClass(AzureEventHubsSinkConfig.class.getName());
            Assertions.assertSame(writerLoader, sinkType.getClassLoader());
            Assertions.assertEquals(
                    artifact, sinkType.getProtectionDomain().getCodeSource().getLocation());
            Class<?> producerType =
                    writerLoader
                            .loadClass(EventHubsProducerFactory.class.getName())
                            .getMethod("create", configType)
                            .getReturnType();
            Assertions.assertEquals(
                    "org.apache.seatunnel.shade.azure.eventhubs.com.azure.messaging.eventhubs.EventHubProducerClient",
                    producerType.getName());
            Assertions.assertSame(writerLoader, producerType.getClassLoader());
            Assertions.assertEquals(
                    artifact, producerType.getProtectionDomain().getCodeSource().getLocation());
            Map<String, Object> options = new HashMap<>();
            options.put(
                    "connection_string",
                    "Endpoint=sb://example/;SharedAccessKeyName=name;SharedAccessKey=key;");
            options.put("event_hub_name", "events");
            Object config =
                    configType
                            .getMethod("from", ReadonlyConfig.class)
                            .invoke(null, ReadonlyConfig.fromMap(options));
            SeaTunnelSink<?, ?, ?, ?> sink =
                    (SeaTunnelSink<?, ?, ?, ?>)
                            sinkType.getConstructor(configType, CatalogTable.class)
                                    .newInstance(config, CatalogTableUtil.buildSimpleTextTable());

            SeaTunnelSink<?, ?, ?, ?> restored =
                    SerializationUtils.deserialize(
                            SerializationUtils.serialize(sink), readerLoader);

            Assertions.assertSame(readerLoader, restored.getClass().getClassLoader());
            Assertions.assertEquals("AzureEventHubs", restored.getPluginName());
            Assertions.assertEquals(
                    sink.getWriteCatalogTable().get().getSeaTunnelRowType(),
                    restored.getWriteCatalogTable().get().getSeaTunnelRowType());
        }
    }

    /**
     * Shares engine APIs but never falls back to unshaded connector classes from target/classes.
     */
    private static final class ConnectorClassLoader extends URLClassLoader {

        private ConnectorClassLoader(URL artifact) {
            super(
                    new URL[] {artifact},
                    TestAzureEventHubsSinkSerializationIT.class.getClassLoader());
        }

        @Override
        protected synchronized Class<?> loadClass(String name, boolean resolve)
                throws ClassNotFoundException {
            if (!name.startsWith("org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.")
                    && !name.startsWith("org.apache.seatunnel.shade.azure.eventhubs.")) {
                return super.loadClass(name, resolve);
            }
            Class<?> type = findLoadedClass(name);
            if (type == null) {
                type = findClass(name);
            }
            if (resolve) {
                resolveClass(type);
            }
            return type;
        }
    }
}
