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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.net.URI;

/** A real local journal whose row writes fail as they would on a full filesystem. */
public class FailingCheckpointFileSystem extends RawLocalFileSystem {
    @Override
    public String getScheme() {
        return "checkpoint-failure";
    }

    @Override
    public URI getUri() {
        return URI.create("checkpoint-failure:///");
    }

    @Override
    public FSDataOutputStream create(
            Path path,
            boolean overwrite,
            int bufferSize,
            short replication,
            long blockSize,
            Progressable progress)
            throws IOException {
        return failRows(
                path, super.create(path, overwrite, bufferSize, replication, blockSize, progress));
    }

    @Override
    public FSDataOutputStream create(
            Path path,
            FsPermission permission,
            boolean overwrite,
            int bufferSize,
            short replication,
            long blockSize,
            Progressable progress)
            throws IOException {
        FSDataOutputStream output =
                super.create(
                        path, permission, overwrite, bufferSize, replication, blockSize, progress);
        return failRows(path, output);
    }

    private FSDataOutputStream failRows(Path path, FSDataOutputStream output) {
        if (!path.getName().equals("rows")) {
            return output;
        }
        return new FSDataOutputStream(
                new FilterOutputStream(output) {
                    @Override
                    public void write(int value) throws IOException {
                        throw new IOException("simulated checkpoint disk full");
                    }

                    @Override
                    public void write(byte[] values, int offset, int length) throws IOException {
                        throw new IOException("simulated checkpoint disk full");
                    }
                },
                null);
    }
}
