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
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PluginIdentifier;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.constants.EngineType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.core.starter.execution.SourceTableInfo;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSourcePluginDiscovery;
import org.apache.seatunnel.translation.spark.execution.DatasetTableInfo;
import org.apache.seatunnel.translation.spark.source.SparkSourceCatalog;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import scala.Tuple2;

import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.apache.seatunnel.api.options.ConnectorCommonOptions.PLUGIN_NAME;
import static org.apache.seatunnel.api.options.ConnectorCommonOptions.PLUGIN_OUTPUT;
import static org.apache.seatunnel.api.table.factory.FactoryUtil.ensureJobModeMatch;

@SuppressWarnings("rawtypes")
public class SourceExecuteProcessor extends SparkAbstractPluginExecuteProcessor<SourceTableInfo> {
    private Map envOption = new HashMap<String, String>();

    public SourceExecuteProcessor(
            SparkRuntimeEnvironment sparkEnvironment,
            JobContext jobContext,
            List<? extends Config> sourceConfigs) {
        super(sparkEnvironment, jobContext, sourceConfigs);
        for (Map.Entry<String, ConfigValue> entry : sparkEnvironment.getConfig().entrySet()) {
            String envKey = entry.getKey();
            String envValue = String.valueOf(entry.getValue().unwrapped());
            if (envKey != null && envValue != null) {
                envOption.put(envKey, envValue);
            }
        }
    }

    @Override
    public List<DatasetTableInfo> execute(List<DatasetTableInfo> upstreamDataStreams) {
        return executeWithCatalogs(Collections.emptyMap());
    }

    public List<DatasetTableInfo> executeWithCatalogs(
            Map<Integer, List<CatalogTable>> recoveredCatalogs) {
        return executeWithCatalogs(recoveredCatalogs, Collections.emptyMap());
    }

    public List<DatasetTableInfo> executeWithCatalogs(
            Map<Integer, List<CatalogTable>> recoveredCatalogs,
            Map<Integer, Long> appliedBoundaries) {
        List<DatasetTableInfo> sources = new ArrayList<>();
        for (int i = 0; i < plugins.size(); i++) {
            SourceTableInfo sourceTableInfo = plugins.get(i);
            SeaTunnelSource<?, ?, ?> source = sourceTableInfo.getSource();
            Config pluginConfig = pluginConfigs.get(i);
            int parallelism;
            if (pluginConfig.hasPath(EnvCommonOptions.PARALLELISM.key())) {
                parallelism = pluginConfig.getInt(EnvCommonOptions.PARALLELISM.key());
            } else {
                parallelism =
                        sparkRuntimeEnvironment
                                .getSparkConf()
                                .getInt(
                                        EnvCommonOptions.PARALLELISM.key(),
                                        EnvCommonOptions.PARALLELISM.defaultValue());
            }
            Map<String, String> sourceOptions = new HashMap<>(envOption);
            sourceOptions.put(EnvCommonOptions.PARALLELISM.key(), String.valueOf(parallelism));
            sourceOptions.put(SparkSourceCatalog.SOURCE_INDEX, Integer.toString(i));
            sourceOptions.put(
                    SparkSourceCatalog.APPLIED_BOUNDARY,
                    Long.toString(appliedBoundaries.getOrDefault(i, -1L)));
            List<CatalogTable> catalogs =
                    recoveredCatalogs.getOrDefault(i, sourceTableInfo.getCatalogTables());
            if (recoveredCatalogs.containsKey(i)) {
                sourceOptions.put(
                        SparkSourceCatalog.CATALOGS,
                        SerializationUtils.objectToString(new ArrayList<>(catalogs)));
            }
            Dataset<Row> dataset;
            if (sparkRuntimeEnvironment.isCheckpointRecoveryEnabled()) {
                dataset =
                        sparkRuntimeEnvironment
                                .getSparkSession()
                                .readStream()
                                .format(SeaTunnelSource.class.getSimpleName())
                                .option(
                                        Constants.SOURCE_SERIALIZATION,
                                        SerializationUtils.objectToString(source))
                                .options(sourceOptions)
                                .load();
            } else {
                dataset =
                        sparkRuntimeEnvironment
                                .getSparkSession()
                                .read()
                                .format(SeaTunnelSource.class.getSimpleName())
                                .option(
                                        Constants.SOURCE_SERIALIZATION,
                                        SerializationUtils.objectToString(source))
                                .options(sourceOptions)
                                .load();
            }
            sources.add(
                    new DatasetTableInfo(
                            dataset,
                            catalogs,
                            ReadonlyConfig.fromConfig(pluginConfig).get(PLUGIN_OUTPUT),
                            i,
                            Collections.emptyList()));
            registerInputTempView(pluginConfigs.get(i), dataset);
        }
        return sources;
    }

    boolean restoreCheckpointSchema(int index) {
        return SparkSourceCatalog.restoreCheckpointSchema(plugins.get(index).getSource());
    }

    List<CatalogTable> sourceCatalogs(int index) {
        return new ArrayList<>(plugins.get(index).getCatalogTables());
    }

    @Override
    protected List<SourceTableInfo> initializePlugins(List<? extends Config> pluginConfigs) {
        SeaTunnelSourcePluginDiscovery sourcePluginDiscovery = new SeaTunnelSourcePluginDiscovery();

        Function<PluginIdentifier, SeaTunnelSource> fallbackCreateSource =
                sourcePluginDiscovery::createPluginInstance;

        List<SourceTableInfo> sources = new ArrayList<>();
        Set<URL> jars = new HashSet<>();
        for (Config sourceConfig : pluginConfigs) {
            PluginIdentifier pluginIdentifier =
                    PluginIdentifier.of(
                            EngineType.SEATUNNEL.getEngine(),
                            PluginType.SOURCE.getType(),
                            sourceConfig.getString(PLUGIN_NAME.key()));
            jars.addAll(
                    sourcePluginDiscovery.getPluginJarAndDependencyPaths(
                            Lists.newArrayList(pluginIdentifier)));
            Tuple2<SeaTunnelSource<Object, SourceSplit, Serializable>, List<CatalogTable>> source =
                    FactoryUtil.createAndPrepareSource(
                            ReadonlyConfig.fromConfig(sourceConfig),
                            classLoader,
                            pluginIdentifier.getPluginName(),
                            fallbackCreateSource,
                            null,
                            null);

            source._1().setJobContext(jobContext);
            ensureJobModeMatch(jobContext, source._1());
            sources.add(new SourceTableInfo(source._1(), source._2()));
        }
        sparkRuntimeEnvironment.registerPlugin(new ArrayList<>(jars));
        return sources;
    }
}
