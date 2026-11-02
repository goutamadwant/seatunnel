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

import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.common.utils.SerializationUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Resolves the authoritative catalogs used to analyze a typed Spark source. */
public final class SparkSourceCatalog {
    public static final String APPLIED_BOUNDARY = "seatunnel.spark.applied-schema-boundary";
    public static final String CATALOGS = "seatunnel.spark.recovered-catalogs";
    public static final String SOURCE_INDEX = "seatunnel.spark.source-index";

    private SparkSourceCatalog() {}

    @SuppressWarnings("unchecked")
    public static List<CatalogTable> resolve(
            SeaTunnelSource<?, ?, ?> source, Map<String, String> options) {
        if (options.containsKey(CATALOGS)) {
            return new ArrayList<>(
                    (List<CatalogTable>) SerializationUtils.stringToObject(options.get(CATALOGS)));
        }
        try {
            return new ArrayList<>(source.getProducedCatalogTables());
        } catch (UnsupportedOperationException legacy) {
            return CatalogTableUtil.convertDataTypeToCatalogTables(
                    source.getProducedType(), "default");
        }
    }

    public static boolean restoreCheckpointSchema(SeaTunnelSource<?, ?, ?> source) {
        return !(source instanceof org.apache.seatunnel.api.source.SupportSchemaChangeWatermarks)
                || ((org.apache.seatunnel.api.source.SupportSchemaChangeWatermarks) source)
                        .restoreCheckpointSchema();
    }

    /** A merged Catalyst shape does not identify individual multi-table layouts or routing. */
    public static boolean sameSchemas(List<CatalogTable> left, List<CatalogTable> right) {
        if (left.size() != right.size()) return false;
        Map<String, CatalogTable> byId = new HashMap<>();
        for (CatalogTable table : left) {
            if (byId.put(table.getTablePath().toString(), table) != null) return false;
        }
        for (CatalogTable table : right) {
            CatalogTable expected = byId.remove(table.getTablePath().toString());
            if (expected == null || !expected.getTableSchema().equals(table.getTableSchema()))
                return false;
        }
        return byId.isEmpty();
    }
}
