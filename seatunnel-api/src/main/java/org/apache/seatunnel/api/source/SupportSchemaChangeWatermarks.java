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

package org.apache.seatunnel.api.source;

/**
 * A coordinated source with snapshot-safe schema-change watermark blocks.
 *
 * <p>Each captured table has one reader owner. A before watermark precedes that reader's ordered
 * DDL block, and an after watermark ends the block before any row using its changed schemas. The
 * complete DDL block and its after watermark run under the collector's shared checkpoint lock.
 * Other readers may continue emitting rows for their unchanged tables between blocks.
 *
 * <p>The enumerator's run method must complete a finite assignment pass without waiting for reader
 * polling. Engines may queue all initial reader requests and run that pass before starting polls,
 * and serialize enumerator callbacks with the shared checkpoint lock.
 *
 * <p>All reader/enumerator state must be snapshot-safe under that lock, including a snapshot
 * initiated from a watermark callback. An engine can then seal a global accepted prefix at a
 * watermark without admitting a later row's already advanced offset. Plain {@link
 * SupportCoordinate} does not establish this stronger contract.
 */
public interface SupportSchemaChangeWatermarks extends SupportCoordinate {
    /** Whether recovered catalogs replace live discovery when this source is restored. */
    default boolean restoreCheckpointSchema() {
        return true;
    }
}
