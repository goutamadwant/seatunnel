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

package org.apache.seatunnel.e2e.common.container.spark;

import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.e2e.common.container.AbstractTestContainer;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

@Slf4j
public abstract class AbstractTestSparkContainer extends AbstractTestContainer {

    private static final String DEFAULT_DOCKER_IMAGE = "bitnami/spark:2.4.6";

    protected GenericContainer<?> master;

    @Override
    protected String getDockerImage() {
        return DEFAULT_DOCKER_IMAGE;
    }

    @Override
    public void startUp() throws Exception {
        FileUtils.createNewDir(HOST_VOLUME_MOUNT_PATH);
        master =
                new GenericContainer<>(getDockerImage())
                        .withNetwork(NETWORK)
                        .withNetworkAliases("spark-master")
                        .withExposedPorts()
                        .withEnv("SPARK_MODE", "master")
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(getDockerImage())))
                        .withCreateContainerCmdModifier(cmd -> cmd.withUser("root"))
                        .withFileSystemBind(
                                HOST_VOLUME_MOUNT_PATH,
                                CONTAINER_VOLUME_MOUNT_PATH,
                                BindMode.READ_WRITE)
                        .waitingFor(
                                new LogMessageWaitStrategy()
                                        .withRegEx(".*Master: Starting Spark master at.*")
                                        .withStartupTimeout(Duration.ofMinutes(2)));
        copySeaTunnelStarterToContainer(master);
        copySeaTunnelStarterLoggingToContainer(master);

        // In most case we can just use standalone mode to execute a spark job, if we want to use
        // cluster mode, we need to
        // start a worker.
        Startables.deepStart(Stream.of(master)).join();
        // execute extra commands
        executeExtraCommands(master);
    }

    @Override
    public void tearDown() throws Exception {
        Exception failure = null;
        try {
            if (master != null && master.getContainerId() != null && !master.isRunning()) {
                master.getDockerClient().startContainerCmd(master.getContainerId()).exec();
                org.awaitility.Awaitility.await()
                        .atMost(Duration.ofMinutes(1))
                        .untilAsserted(
                                () ->
                                        org.junit.jupiter.api.Assertions.assertEquals(
                                                0, master.execInContainer("true").getExitCode()));
            }
        } catch (Exception error) {
            failure = error;
        }
        try {
            stopContainersAndDeleteVolume(master);
        } catch (Exception error) {
            if (failure == null) failure = error;
            else if (failure != error) failure.addSuppressed(error);
        }
        if (failure != null) throw failure;
    }

    @Override
    protected String getSavePointCommand() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getCancelJobCommand() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getRestoreCommand() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected List<String> getExtraStartShellCommands() {
        return Arrays.asList("--master local", "--deploy-mode client");
    }

    public void executeExtraCommands(ContainerExtendedFactory extendedFactory)
            throws IOException, InterruptedException {
        extendedFactory.extend(master);
    }

    @Override
    public Container.ExecResult executeJob(String confFile)
            throws IOException, InterruptedException {
        return executeJob(confFile, Collections.emptyList());
    }

    @Override
    public Container.ExecResult executeJob(String confFile, List<String> variables)
            throws IOException, InterruptedException {
        log.info("test in container: {}", identifier());
        return executeJob(master, confFile, null, variables);
    }

    /** Kills the owned local driver and restarts the same container, retaining checkpoint files. */
    public void restartSparkRuntimeForRecovery() {
        String containerId = master.getContainerId();
        stopSparkRuntimeForRecovery();
        master.getDockerClient().startContainerCmd(containerId).exec();
        org.awaitility.Awaitility.await()
                .atMost(Duration.ofMinutes(1))
                .untilAsserted(
                        () ->
                                org.junit.jupiter.api.Assertions.assertEquals(
                                        0, master.execInContainer("true").getExitCode()));
    }

    /** Stops only the owned runtime, preserving files for recovery or failure inspection. */
    public void stopSparkRuntimeForRecovery() {
        if (master.isRunning())
            master.getDockerClient().killContainerCmd(master.getContainerId()).exec();
    }

    public java.nio.file.Path sparkCheckpointOnHost(String relativePath) {
        java.nio.file.Path root = Paths.get(HOST_VOLUME_MOUNT_PATH).toAbsolutePath().normalize();
        java.nio.file.Path path = root.resolve(relativePath).normalize();
        if (!path.startsWith(root))
            throw new IllegalArgumentException("Checkpoint must be inside the owned test volume");
        return path;
    }

    /** Reads Spark's completed commit markers; pending offset files do not count. */
    public long latestCompletedSparkEpoch(String checkpointDirectory)
            throws IOException, InterruptedException {
        Container.ExecResult result =
                master.execInContainer("ls", "-1", checkpointDirectory + "/commits");
        if (result.getExitCode() != 0) return -1;
        return Arrays.stream(result.getStdout().split("\\R"))
                .filter(name -> name.matches("[0-9]+"))
                .mapToLong(Long::parseLong)
                .max()
                .orElse(-1);
    }

    @Override
    public String getServerLogs() {
        return master.getLogs();
    }

    @Override
    public void copyFileToContainer(String path, String targetPath) {
        ContainerUtil.copyFileIntoContainers(
                ContainerUtil.getResourcesFile(path).toPath(), targetPath, master);
    }

    @Override
    public void copyAbsolutePathToContainer(String path, String targetPath) {
        ContainerUtil.copyFileIntoContainers(Paths.get(path), targetPath, master);
    }
}
