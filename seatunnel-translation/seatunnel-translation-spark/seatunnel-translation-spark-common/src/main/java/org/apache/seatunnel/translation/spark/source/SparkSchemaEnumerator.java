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

import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import java.io.IOException;
import java.util.List;

/** Serializes finite coordinated-source assignment passes with schema boundary snapshots. */
final class SparkSchemaEnumerator<S extends SourceSplit, T> implements SourceSplitEnumerator<S, T> {
    private final SourceSplitEnumerator<S, T> delegate;
    private final Object checkpointLock;
    private boolean started;

    SparkSchemaEnumerator(SourceSplitEnumerator<S, T> delegate, Object checkpointLock) {
        this.delegate = delegate;
        this.checkpointLock = checkpointLock;
    }

    @Override
    public void open() {
        synchronized (checkpointLock) {
            delegate.open();
        }
    }

    @Override
    public void run() throws Exception {
        synchronized (checkpointLock) {
            if (!started) {
                delegate.run();
                started = true;
            }
        }
    }

    @Override
    public void addSplitsBack(List<S> splits, int id) {
        synchronized (checkpointLock) {
            delegate.addSplitsBack(splits, id);
        }
    }

    @Override
    public void handleSplitRequest(int id) {
        synchronized (checkpointLock) {
            delegate.handleSplitRequest(id);
        }
    }

    @Override
    public void registerReader(int id) {
        synchronized (checkpointLock) {
            delegate.registerReader(id);
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        synchronized (checkpointLock) {
            return delegate.currentUnassignedSplitSize();
        }
    }

    @Override
    public T snapshotState(long id) throws Exception {
        synchronized (checkpointLock) {
            return delegate.snapshotState(id);
        }
    }

    @Override
    public void handleSourceEvent(int id, SourceEvent event) {
        synchronized (checkpointLock) {
            delegate.handleSourceEvent(id, event);
        }
    }

    @Override
    public void notifyCheckpointComplete(long id) throws Exception {
        synchronized (checkpointLock) {
            delegate.notifyCheckpointComplete(id);
        }
    }

    @Override
    public void notifyCheckpointAborted(long id) throws Exception {
        synchronized (checkpointLock) {
            delegate.notifyCheckpointAborted(id);
        }
    }

    @Override
    public void close() throws IOException {
        // Do not hold the checkpoint lock while closing background source threads.
        delegate.close();
    }
}
