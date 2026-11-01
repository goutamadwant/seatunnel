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

package org.apache.seatunnel.translation.spark.execution;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import lombok.Data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Data
public class DatasetTableInfo {

    private Dataset<Row> dataset;
    private List<CatalogTable> catalogTables;
    private String tableName;
    private int sourceIndex = -1;
    private List<SeaTunnelTransform> schemaTransforms = Collections.emptyList();

    public void setCatalogTables(List<CatalogTable> catalogTables) {
        this.catalogTables = Collections.unmodifiableList(new ArrayList<>(catalogTables));
    }

    public DatasetTableInfo(
            Dataset<Row> dataset, List<CatalogTable> catalogTables, String tableName) {
        this.dataset = dataset;
        setCatalogTables(catalogTables);
        this.tableName = tableName;
    }

    public DatasetTableInfo(
            Dataset<Row> dataset,
            List<CatalogTable> catalogTables,
            String tableName,
            int sourceIndex,
            List<SeaTunnelTransform> schemaTransforms) {
        this(dataset, catalogTables, tableName);
        this.sourceIndex = sourceIndex;
        this.schemaTransforms = Collections.unmodifiableList(new ArrayList<>(schemaTransforms));
    }
}
