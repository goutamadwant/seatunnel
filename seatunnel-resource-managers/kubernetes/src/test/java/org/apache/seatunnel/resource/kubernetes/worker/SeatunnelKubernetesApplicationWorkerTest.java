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

package org.apache.seatunnel.resource.kubernetes.worker;

import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockConstruction;

class SeatunnelKubernetesApplicationWorkerTest {
    private static final String ADDRESS = "127.0.0.1:12345";

    @Test
    void connectsToListeningMasterService() throws Exception {
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            server.setSoTimeout(3000);
            SeatunnelKubernetesApplicationWorker.awaitMasterService(
                    "127.0.0.1:" + server.getLocalPort(), 3000);
            try (Socket connection = server.accept()) {
                assertTrue(connection.isConnected());
            }
        }
    }

    @Test
    void retriesUntilMasterServiceAcceptsConnection() {
        try (MockedConstruction<Socket> sockets =
                mockConstruction(
                        Socket.class,
                        (socket, context) -> {
                            if (context.getCount() == 1) {
                                refuseConnections(socket);
                            }
                        })) {
            SeatunnelKubernetesApplicationWorker.awaitMasterService(ADDRESS, 3000);
            assertEquals(2, sockets.constructed().size());
        }
    }

    @Test
    void failsWithinStartupDeadlineWhenMasterServiceNeverStarts() {
        try (MockedConstruction<Socket> ignored = refusingSockets()) {
            long started = System.nanoTime();
            IllegalStateException failure =
                    assertThrows(
                            IllegalStateException.class,
                            () ->
                                    SeatunnelKubernetesApplicationWorker.awaitMasterService(
                                            ADDRESS, 150));
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            assertTrue(
                    elapsedMillis >= 100, "The worker should allow its configured startup window");
            assertTrue(elapsedMillis < 3000, "The worker must not wait indefinitely");
            assertTrue(failure.getMessage().contains(ADDRESS));
            assertTrue(failure.getMessage().contains("150 ms"));
            assertInstanceOf(ConnectException.class, failure.getCause());
        }
    }

    @Test
    void interruptionStopsWaitingAndPreservesInterruptStatus() {
        try (MockedConstruction<Socket> ignored = refusingSockets()) {
            Thread.currentThread().interrupt();
            try {
                IllegalStateException failure =
                        assertThrows(
                                IllegalStateException.class,
                                () ->
                                        SeatunnelKubernetesApplicationWorker.awaitMasterService(
                                                ADDRESS, 3000));
                assertInstanceOf(InterruptedException.class, failure.getCause());
                assertTrue(Thread.currentThread().isInterrupted());
            } finally {
                Thread.interrupted();
            }
        }
    }

    private static MockedConstruction<Socket> refusingSockets() {
        return mockConstruction(Socket.class, (socket, context) -> refuseConnections(socket));
    }

    private static void refuseConnections(Socket socket) throws IOException {
        doThrow(new ConnectException("Connection refused"))
                .when(socket)
                .connect(any(SocketAddress.class), anyInt());
    }
}
