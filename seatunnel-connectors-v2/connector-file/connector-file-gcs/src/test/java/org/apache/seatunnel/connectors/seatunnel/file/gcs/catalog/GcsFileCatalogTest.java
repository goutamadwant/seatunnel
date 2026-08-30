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

package org.apache.seatunnel.connectors.seatunnel.file.gcs.catalog;

import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertTrue;

class GcsFileCatalogTest {

    @TempDir private java.nio.file.Path tempDir;

    @Test
    void shouldCheckForExistingDataRecursively() throws Exception {
        java.nio.file.Path sinkPath = tempDir.resolve("warehouse/orders");
        java.nio.file.Path partitionPath =
                Files.createDirectories(sinkPath.resolve("day=2026-08-30"));
        Files.write(partitionPath.resolve("part-0.parquet"), new byte[] {1});
        HadoopFileSystemProxy fileSystemProxy =
                new HadoopFileSystemProxy(new HadoopConf("file:///"));
        GcsFileCatalog catalog =
                new GcsFileCatalog(fileSystemProxy, sinkPath.toString(), "GcsFile");

        try {
            assertTrue(catalog.isExistsData(null));
        } finally {
            fileSystemProxy.close();
        }
    }
}
