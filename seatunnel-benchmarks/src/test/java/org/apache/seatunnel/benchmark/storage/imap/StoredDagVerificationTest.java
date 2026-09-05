/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.benchmark.storage.imap;

import org.apache.seatunnel.benchmark.dag.JobDagFixtureFactory;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;

import org.junit.jupiter.api.Test;

import com.hazelcast.map.IMap;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

class StoredDagVerificationTest {
    @Test
    void verifiesEveryKeyAndRejectsAnUnsampledMissingValue() throws Exception {
        Map<Long, JobDAGInfo> values = new HashMap<>();
        JobDAGInfo dag = JobDagFixtureFactory.create(1);
        long[] keys = new long[100];
        for (int i = 0; i < 100; i++) {
            keys[i] = i;
            values.put((long) i, dag);
        }
        IMapDagStorageBenchmarkWorkload workload = new IMapDagStorageBenchmarkWorkload();
        IMap map =
                (IMap)
                        Proxy.newProxyInstance(
                                IMap.class.getClassLoader(),
                                new Class<?>[] {IMap.class},
                                (proxy, method, args) -> values.get(args[0]));
        set(workload, "finishedJobDagMap", map);
        set(workload, "finishedJobDag", dag);
        set(workload, "storeKeys", keys);
        set(workload, "storedDagCountInIteration", 100);
        workload.verifyAllStoredJobDags();
        values.remove(17L);
        assertThrows(IllegalStateException.class, workload::verifyAllStoredJobDags);
        values.put(17L, JobDagFixtureFactory.create(10));
        assertThrows(IllegalStateException.class, workload::verifyAllStoredJobDags);
    }

    @Test
    void rejectsPartialBatch() {
        assertThrows(
                IllegalStateException.class,
                new IMapDagStorageBenchmarkWorkload()::verifyAllStoredJobDags);
    }

    private static void set(Object object, String name, Object value) throws Exception {
        Field field = object.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(object, value);
    }
}
