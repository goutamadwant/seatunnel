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

package org.apache.seatunnel.core.starter.spark.execution;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PluginIdentifier;
import org.apache.seatunnel.api.common.multitable.MultiTableFailedTable;
import org.apache.seatunnel.api.common.multitable.MultiTableFailureHelper;
import org.apache.seatunnel.api.common.multitable.MultiTableFailurePhase;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SaveModeExecuteWrapper;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionRecovery;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.constants.EngineType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelFactoryDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginDiscovery;
import org.apache.seatunnel.translation.spark.execution.DatasetTableInfo;
import org.apache.seatunnel.translation.spark.sink.SparkSinkInjector;
import org.apache.seatunnel.translation.spark.source.SparkCommittedSourceState;
import org.apache.seatunnel.translation.spark.source.SparkSchemaBoundaryCoordinator;
import org.apache.seatunnel.translation.spark.source.SparkSourceCheckpoint;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.streaming.StreamingQuery;

import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode.HANDLE_SAVE_MODE_FAILED;
import static org.apache.seatunnel.api.options.ConnectorCommonOptions.PLUGIN_NAME;
import static org.apache.seatunnel.api.table.factory.FactoryUtil.discoverOptionalFactory;

@Slf4j
public class SinkExecuteProcessor
        extends SparkAbstractPluginExecuteProcessor<Optional<? extends Factory>> {

    protected SinkExecuteProcessor(
            SparkRuntimeEnvironment sparkRuntimeEnvironment,
            JobContext jobContext,
            List<? extends Config> pluginConfigs) {
        super(sparkRuntimeEnvironment, jobContext, pluginConfigs);
    }

    @Override
    protected List<Optional<? extends Factory>> initializePlugins(
            List<? extends Config> pluginConfigs) {
        List<URL> pluginJars = new ArrayList<>();
        SeaTunnelFactoryDiscovery sinkPluginDiscovery =
                new SeaTunnelFactoryDiscovery(TableSinkFactory.class);
        List<Optional<? extends Factory>> sinks =
                pluginConfigs.stream()
                        .map(
                                sinkConfig -> {
                                    pluginJars.addAll(
                                            sinkPluginDiscovery.getPluginJarPaths(
                                                    Lists.newArrayList(
                                                            PluginIdentifier.of(
                                                                    EngineType.SEATUNNEL
                                                                            .getEngine(),
                                                                    PluginType.SINK.getType(),
                                                                    sinkConfig.getString(
                                                                            PLUGIN_NAME.key())))));
                                    return discoverOptionalFactory(
                                            classLoader,
                                            TableSinkFactory.class,
                                            sinkConfig.getString(PLUGIN_NAME.key()));
                                })
                        .distinct()
                        .collect(Collectors.toList());
        sparkRuntimeEnvironment.registerPlugin(pluginJars);
        return sinks;
    }

    @Override
    public List<DatasetTableInfo> execute(List<DatasetTableInfo> upstreamDataStreams)
            throws TaskExecuteException {
        DatasetTableInfo input = upstreamDataStreams.get(upstreamDataStreams.size() - 1);
        List<MultiTableFailedTable> skippedTables = new ArrayList<>();
        boolean createdAnySink = false;
        List<StreamingQuery> queries = new ArrayList<>();
        Throwable executionFailure = null;
        try {
            for (int i = 0; i < plugins.size(); i++) {
                Config sinkConfig = pluginConfigs.get(i);
                DatasetTableInfo datasetTableInfo =
                        fromSourceTable(sinkConfig, sparkRuntimeEnvironment, upstreamDataStreams)
                                .orElse(input);
                Dataset<Row> dataset = datasetTableInfo.getDataset();
                int parallelism;
                if (sinkConfig.hasPath(EnvCommonOptions.PARALLELISM.key())) {
                    parallelism = sinkConfig.getInt(EnvCommonOptions.PARALLELISM.key());
                } else {
                    parallelism =
                            sparkRuntimeEnvironment
                                    .getSparkConf()
                                    .getInt(
                                            EnvCommonOptions.PARALLELISM.key(),
                                            EnvCommonOptions.PARALLELISM.defaultValue());
                }
                dataset.sparkSession()
                        .read()
                        .option(EnvCommonOptions.PARALLELISM.key(), parallelism);
                SeaTunnelSink sink = prepareSink(datasetTableInfo, sinkConfig, true, skippedTables);
                if (sink == null) continue;
                createdAnySink = true;
                String applicationId =
                        sparkRuntimeEnvironment
                                .getStreamingContext()
                                .sparkContext()
                                .applicationId();
                CatalogTable[] catalogTables =
                        datasetTableInfo.getCatalogTables().toArray(new CatalogTable[0]);
                if (dataset.isStreaming()) {
                    String location =
                            sparkRuntimeEnvironment
                                    .getConfig()
                                    .getString(SparkCheckpointOptions.CHECKPOINT_LOCATION.key());
                    queries.add(
                            SparkSinkInjector.inject(
                                            dataset.writeStream(),
                                            sink,
                                            catalogTables,
                                            applicationId,
                                            parallelism)
                                    .option(
                                            "checkpointLocation",
                                            new Path(location, "sink-" + i).toString())
                                    .start());
                } else {
                    SparkSinkInjector.inject(
                                    dataset.write(),
                                    sink,
                                    catalogTables,
                                    applicationId,
                                    parallelism)
                            .option("checkpointLocation", "/tmp")
                            .mode(SaveMode.Append)
                            .save();
                }
            }
            if (!createdAnySink && !skippedTables.isEmpty()) {
                throw new TaskExecuteException(
                        MultiTableFailureHelper.formatFailedTableSummary(
                                "All candidate sink tables were skipped in Spark starter.",
                                skippedTables));
            }
            if (createdAnySink && !skippedTables.isEmpty()) {
                log.warn(
                        MultiTableFailureHelper.formatFailedTableSummary(
                                "Some sink tables were skipped in Spark starter.", skippedTables));
            }
            if (!queries.isEmpty()) {
                boolean waiting = true;
                while (waiting) {
                    for (StreamingQuery query : queries) {
                        if (query.awaitTermination(100)) {
                            waiting = false;
                            break;
                        }
                    }
                }
            }
            // the sink is the last stream
            return null;
        } catch (Throwable failure) {
            executionFailure = failure;
            throw new TaskExecuteException("Spark sink execution failed", failure);
        } finally {
            SparkQueryLifecycle.stop(queries, executionFailure);
        }
    }

    void executeRecoverable(SparkQueryPlanFactory plans) {
        if (pluginConfigs.size() != 1) {
            throw new UnsupportedOperationException(
                    "Spark schema recovery requires one sink query; use a multi-table sink for routed tables");
        }
        Config sinkConfig = pluginConfigs.get(0);
        List<DatasetTableInfo> initial = plans.build(Collections.emptyMap());
        DatasetTableInfo selected =
                SparkSchemaPlan.selectPlan(
                        initial, inputs -> selectRecoveryInput(sinkConfig, inputs));
        int sourceIndex = selected.getSourceIndex();
        SparkSchemaPlan.closePlans(null, selected);
        if (sourceIndex < 0)
            throw new IllegalStateException("Spark recovery plan lost its source identity");
        String location =
                new Path(
                                sparkRuntimeEnvironment
                                        .getConfig()
                                        .getString(
                                                SparkCheckpointOptions.CHECKPOINT_LOCATION.key()),
                                "sink-0")
                        .toString();
        Configuration checkpointFilesystem =
                sparkRuntimeEnvironment.getSparkSession().sparkContext().hadoopConfiguration();
        if (sparkRuntimeEnvironment.getConfig().hasPath(Constants.HDFS_ROOT)) {
            checkpointFilesystem = new Configuration(checkpointFilesystem);
            checkpointFilesystem.set(
                    "fs.defaultFS",
                    sparkRuntimeEnvironment.getConfig().getString(Constants.HDFS_ROOT));
        }
        final Configuration filesystem = checkpointFilesystem;
        String user =
                sparkRuntimeEnvironment.getConfig().hasPath(Constants.HDFS_USER)
                        ? sparkRuntimeEnvironment.getConfig().getString(Constants.HDFS_USER)
                        : "";
        SparkSchemaBoundaryCoordinator coordinator = new SparkSchemaBoundaryCoordinator();
        AtomicReference<DatasetTableInfo> activePlan = new AtomicReference<>();
        Throwable recoveryFailure = null;
        try {
            final boolean initialize =
                    !SparkCommittedSourceState.load(filesystem, location, 0, 1, user).isPresent();
            coordinator.run(
                    filesystem,
                    location,
                    user,
                    plans.sourceCatalogs(sourceIndex),
                    new SparkSchemaBoundaryCoordinator.QueryFactory() {
                        private boolean first = true;

                        public boolean restoreCheckpointSchema() {
                            return plans.restoreCheckpointSchema(sourceIndex);
                        }

                        public StreamingQuery start(List<CatalogTable> catalogs) {
                            return start(catalogs, -1);
                        }

                        public StreamingQuery start(
                                List<CatalogTable> catalogs, long appliedBoundary) {
                            DatasetTableInfo input =
                                    buildRecoveryInput(
                                            plans,
                                            sinkConfig,
                                            Collections.singletonMap(sourceIndex, catalogs),
                                            Collections.singletonMap(sourceIndex, appliedBoundary));
                            activePlan.set(input);
                            List<MultiTableFailedTable> skipped = new ArrayList<>();
                            SeaTunnelSink sink =
                                    prepareSink(input, sinkConfig, first && initialize, skipped);
                            first = false;
                            if (sink == null)
                                throw new TaskExecuteException(
                                        MultiTableFailureHelper.formatFailedTableSummary(
                                                "All recovery sink tables were skipped.", skipped));
                            int parallelism =
                                    sinkConfig.hasPath(EnvCommonOptions.PARALLELISM.key())
                                            ? sinkConfig.getInt(EnvCommonOptions.PARALLELISM.key())
                                            : sparkRuntimeEnvironment
                                                    .getSparkConf()
                                                    .getInt(
                                                            EnvCommonOptions.PARALLELISM.key(),
                                                            EnvCommonOptions.PARALLELISM
                                                                    .defaultValue());
                            try {
                                return SparkSinkInjector.inject(
                                                input.getDataset().writeStream(),
                                                sink,
                                                input.getCatalogTables()
                                                        .toArray(new CatalogTable[0]),
                                                sparkRuntimeEnvironment
                                                        .getSparkSession()
                                                        .sparkContext()
                                                        .applicationId(),
                                                parallelism)
                                        .option("checkpointLocation", location)
                                        .start();
                            } catch (Exception error) {
                                throw new TaskExecuteException(
                                        "Cannot start rebuilt Spark sink query", error);
                            }
                        }

                        public void reconcile(SparkCommittedSourceState committed)
                                throws Exception {
                            SparkSchemaPlan.closePlans(null, activePlan.getAndSet(null));
                            SparkSourceCheckpoint boundary = committed.getCheckpoint();
                            DatasetTableInfo before = null, after = null;
                            Throwable failure = null;
                            try {
                                before =
                                        buildRecoveryInput(
                                                plans,
                                                sinkConfig,
                                                Collections.singletonMap(
                                                        sourceIndex, boundary.getBeforeTables()));
                                after =
                                        buildRecoveryInput(
                                                plans,
                                                sinkConfig,
                                                Collections.singletonMap(
                                                        sourceIndex, boundary.getTables()));
                                reconcileRecoverySinks(
                                        sinkConfig,
                                        before,
                                        after,
                                        SparkSchemaPlan.mapChanges(
                                                before,
                                                after,
                                                boundary.getEvents(),
                                                boundary.getBeforeTables()));
                            } catch (Exception | Error error) {
                                failure = error;
                                throw error;
                            } finally {
                                SparkSchemaPlan.closePlans(failure, before, after);
                            }
                        }
                    });
        } catch (Exception | Error error) {
            recoveryFailure = error;
            throw new TaskExecuteException("Spark schema recovery failed", error);
        } finally {
            SparkSchemaPlan.closePlans(recoveryFailure, activePlan.getAndSet(null));
        }
    }

    private DatasetTableInfo buildRecoveryInput(
            SparkQueryPlanFactory plans,
            Config sinkConfig,
            Map<Integer, List<CatalogTable>> catalogs) {
        return buildRecoveryInput(plans, sinkConfig, catalogs, Collections.emptyMap());
    }

    private DatasetTableInfo buildRecoveryInput(
            SparkQueryPlanFactory plans,
            Config sinkConfig,
            Map<Integer, List<CatalogTable>> catalogs,
            Map<Integer, Long> appliedBoundaries) {
        return SparkSchemaPlan.selectPlan(
                plans.build(catalogs, appliedBoundaries),
                inputs -> selectRecoveryInput(sinkConfig, inputs));
    }

    private DatasetTableInfo selectRecoveryInput(Config sinkConfig, List<DatasetTableInfo> inputs) {
        return fromSourceTable(sinkConfig, sparkRuntimeEnvironment, inputs)
                .orElse(inputs.get(inputs.size() - 1));
    }

    private void reconcileRecoverySinks(
            Config sinkConfig,
            DatasetTableInfo before,
            DatasetTableInfo after,
            List<SchemaChangeEvent> events)
            throws Exception {
        SeaTunnelSinkPluginDiscovery discovery = new SeaTunnelSinkPluginDiscovery();
        for (CatalogTable target : after.getCatalogTables()) {
            CatalogTable previous =
                    before.getCatalogTables().stream()
                            .filter(table -> table.getTablePath().equals(target.getTablePath()))
                            .findFirst()
                            .orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    "Schema change has no previous sink routing"));
            List<SchemaChangeEvent> changes =
                    events.stream()
                            .filter(event -> event.tablePath().equals(target.getTablePath()))
                            .collect(Collectors.toList());
            if (changes.isEmpty()) {
                if (previous.getTableSchema().equals(target.getTableSchema())) continue;
                throw new IllegalStateException("Changed sink schema has no mapped DDL block");
            }
            SeaTunnelSink sink =
                    FactoryUtil.createAndPrepareSink(
                            target,
                            ReadonlyConfig.fromConfig(sinkConfig),
                            classLoader,
                            sinkConfig.getString(PLUGIN_NAME.key()),
                            discovery::createPluginInstance,
                            null);
            sink.setJobContext(jobContext);
            SparkCheckpointOptions.validateRecoverySink(sink);
            if (!(sink instanceof SupportSchemaEvolutionRecovery)) {
                throw new UnsupportedOperationException(
                        "Spark schema recovery requires an idempotent schema reconciliation sink");
            }
            ((SupportSchemaEvolutionRecovery) sink)
                    .reconcileSchemaChanges(
                            previous.getTableSchema(), target.getTableSchema(), changes);
        }
    }

    protected SeaTunnelSink prepareSink(
            DatasetTableInfo datasetTableInfo,
            Config sinkConfig,
            boolean initializeSaveMode,
            List<MultiTableFailedTable> skippedTables) {
        Dataset<Row> dataset = datasetTableInfo.getDataset();
        SeaTunnelSinkPluginDiscovery sinkPluginDiscovery = new SeaTunnelSinkPluginDiscovery();
        Function<PluginIdentifier, SeaTunnelSink> fallbackCreateSink =
                sinkPluginDiscovery::createPluginInstance;
        Map<TablePath, SeaTunnelSink> sinks = new HashMap<>();
        List<MultiTableFailedTable> currentSkippedTables = new ArrayList<>();
        for (CatalogTable catalogTable : datasetTableInfo.getCatalogTables()) {
            SeaTunnelSink<Object, Object, Object, Object> sink;
            try {
                sink =
                        FactoryUtil.createAndPrepareSink(
                                catalogTable,
                                ReadonlyConfig.fromConfig(sinkConfig),
                                classLoader,
                                sinkConfig.getString(PLUGIN_NAME.key()),
                                fallbackCreateSink,
                                null);
                sink.setJobContext(jobContext);
            } catch (Exception error) {
                if (!shouldContinueOtherTables()) {
                    throw wrapThrowable(error);
                }
                logSkippedTable(
                        currentSkippedTables,
                        skippedTables,
                        catalogTable,
                        sinkConfig,
                        MultiTableFailurePhase.SINK_INIT,
                        error);
                continue;
            }
            if (dataset.isStreaming()) {
                SparkCheckpointOptions.validateRecoverySink(sink);
            }
            try {
                if (initializeSaveMode) handleSaveMode(sink);
                sinks.put(catalogTable.getTableId().toTablePath(), sink);
            } catch (Exception error) {
                if (!shouldContinueOtherTables()) {
                    throw wrapThrowable(error);
                }
                logSkippedTable(
                        currentSkippedTables,
                        skippedTables,
                        catalogTable,
                        sinkConfig,
                        MultiTableFailurePhase.SAVE_MODE,
                        error);
            }
        }
        if (sinks.isEmpty()) {
            return null;
        }
        return tryGenerateMultiTableSink(
                sinks,
                MultiTableFailureHelper.withFailedTables(
                        MultiTableFailureHelper.mergeOptions(
                                ReadonlyConfig.fromConfig(sinkConfig),
                                ReadonlyConfig.fromConfig(sparkRuntimeEnvironment.getConfig())),
                        currentSkippedTables),
                classLoader);
    }

    public void handleSaveMode(SeaTunnelSink sink) {
        if (sink instanceof SupportSaveMode) {
            Optional<SaveModeHandler> saveModeHandler =
                    ((SupportSaveMode) sink).getSaveModeHandler();
            if (saveModeHandler.isPresent()) {
                try (SaveModeHandler handler = saveModeHandler.get()) {
                    if (sparkRuntimeEnvironment.isCheckpointRecoveryEnabled()
                            && (handler.getSchemaSaveMode() == SchemaSaveMode.RECREATE_SCHEMA
                                    || handler.getDataSaveMode() == DataSaveMode.DROP_DATA
                                    || handler.getDataSaveMode()
                                            == DataSaveMode.CUSTOM_PROCESSING)) {
                        throw new UnsupportedOperationException(
                                "Spark recovery requires a non-destructive sink save mode");
                    }
                    handler.open();
                    new SaveModeExecuteWrapper(handler).execute();
                } catch (Exception e) {
                    throw new SeaTunnelRuntimeException(HANDLE_SAVE_MODE_FAILED, e);
                }
            }
        }
    }

    private boolean shouldContinueOtherTables() {
        return MultiTableFailureHelper.shouldContinueOtherTables(
                ReadonlyConfig.fromConfig(sparkRuntimeEnvironment.getConfig()));
    }

    private RuntimeException wrapThrowable(Throwable error) {
        if (error instanceof RuntimeException) {
            return (RuntimeException) error;
        }
        return new RuntimeException(error);
    }

    private void logSkippedTable(
            List<MultiTableFailedTable> currentSkippedTables,
            List<MultiTableFailedTable> skippedTables,
            CatalogTable catalogTable,
            Config sinkConfig,
            MultiTableFailurePhase phase,
            Throwable error) {
        MultiTableFailedTable failedTable =
                MultiTableFailureHelper.buildFailedTable(
                        catalogTable.getTablePath().getFullName(),
                        phase,
                        sinkConfig.getString(PLUGIN_NAME.key()),
                        error);
        currentSkippedTables.add(failedTable);
        skippedTables.add(failedTable);
        log.warn(
                "Skip failed sink table in Spark starter: {}",
                MultiTableFailureHelper.formatFailedTableLine(failedTable),
                error);
    }
}
