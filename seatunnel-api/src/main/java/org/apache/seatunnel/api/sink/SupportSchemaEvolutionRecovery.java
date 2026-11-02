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

package org.apache.seatunnel.api.sink;

import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;

import java.io.IOException;
import java.util.List;

/** External schema reconciliation that is safe to repeat after an engine driver failure. */
public interface SupportSchemaEvolutionRecovery extends SupportSchemaEvolutionSink {
    /**
     * Completes the ordered live schema changes from a committed engine boundary. Inspect the
     * receiver before applying DDL: a previous attempt may have applied some or all changes. Reject
     * an inconsistent or ambiguous receiver state. Ordinary local schema restoration must not call
     * this method.
     */
    void reconcileSchemaChanges(
            TableSchema before, TableSchema after, List<SchemaChangeEvent> events)
            throws IOException;
}
