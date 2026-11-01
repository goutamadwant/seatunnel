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

package org.apache.seatunnel.translation.spark.source;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;

import org.junit.jupiter.api.Assertions;

import java.io.DataInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Independently binds the commit file, its source offset and the exact durable interval schema. */
public final class SparkRecoveryCheckpointAssertions {
    public static long assertData(Path root, boolean evolved, boolean offline) throws Exception {
        long epoch;
        try (Stream<Path> commits = Files.list(root.resolve("commits"))) {
            epoch =
                    commits.map(path -> path.getFileName().toString())
                            .filter(name -> name.matches("[0-9]+"))
                            .mapToLong(Long::parseLong)
                            .max()
                            .orElseThrow(() -> new AssertionError("No completed Spark epoch"));
        }
        List<String> marker = Files.readAllLines(root.resolve("commits/" + epoch));
        Assertions.assertEquals(2, marker.size());
        Assertions.assertEquals("v1", marker.get(0));
        Assertions.assertNotNull(JsonUtils.parseObject(marker.get(1), Map.class));
        List<String> offsets = Files.readAllLines(root.resolve("offsets/" + epoch));
        Assertions.assertEquals(3, offsets.size());
        Assertions.assertEquals("v1", offsets.get(0));
        long checkpoint = SparkCheckpointOffset.checkpointId(offsets.get(2));
        SparkSourceCheckpoint state;
        try (DataInputStream input =
                new DataInputStream(
                        Files.newInputStream(
                                root.resolve(
                                        "sources/0/seatunnel-batches/0/"
                                                + checkpoint
                                                + "/state")))) {
            state = SparkSourceCheckpoint.read(input);
        }
        Assertions.assertEquals(SparkSourceCheckpoint.Phase.DATA, state.getPhase());
        state.validateParallelism(2);
        Assertions.assertEquals(2, state.getTables().size());
        Assertions.assertEquals(
                new HashSet<>(
                        Arrays.asList("mysql_cdc.spark_restore_a", "mysql_cdc.spark_restore_b")),
                state.getTables().stream()
                        .map(table -> table.getTablePath().getFullName())
                        .collect(Collectors.toSet()));
        Set<String> tables = new HashSet<>();
        int readers = 0;
        for (Map.Entry<Integer, List<byte[]>> entry : state.getState().entrySet()) {
            if (entry.getKey() < 0) continue;
            Set<String> owned = new HashSet<>();
            for (byte[] bytes : entry.getValue()) {
                Object split = SerializationUtils.deserialize(bytes);
                Assertions.assertInstanceOf(IncrementalSplit.class, split);
                ((IncrementalSplit) split).getTableIds().forEach(id -> owned.add(id.toString()));
            }
            Assertions.assertEquals(1, owned.size());
            readers++;
            tables.addAll(owned);
        }
        Assertions.assertEquals(2, readers);
        Assertions.assertEquals(
                new HashSet<>(
                        Arrays.asList("mysql_cdc.spark_restore_a", "mysql_cdc.spark_restore_b")),
                tables);
        for (CatalogTable table : state.getTables()) {
            List<Column> columns = table.getTableSchema().getColumns();
            boolean a = table.getTablePath().getFullName().equals("mysql_cdc.spark_restore_a");
            List<String> expected =
                    a && offline
                            ? Arrays.asList("id", "name", "score", "note")
                            : a && evolved
                                    ? Arrays.asList("id", "name", "score")
                                    : Arrays.asList("id", "name");
            Assertions.assertEquals(
                    expected, columns.stream().map(Column::getName).collect(Collectors.toList()));
            Assertions.assertEquals(BasicType.INT_TYPE, columns.get(0).getDataType());
            Assertions.assertEquals(BasicType.STRING_TYPE, columns.get(1).getDataType());
            if (a && evolved)
                Assertions.assertEquals(BasicType.INT_TYPE, columns.get(2).getDataType());
            if (a && offline)
                Assertions.assertEquals(BasicType.STRING_TYPE, columns.get(3).getDataType());
        }
        return epoch;
    }
}
