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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql;

import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.spark.AbstractTestSparkContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.DependencyJar;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

/** Exercises the Spark checkpoint protocol, independently of Zeta's job-id restore commands. */
@DisabledOnContainer(
        value = {},
        type = {EngineType.SEATUNNEL, EngineType.FLINK},
        disabledReason = "Spark persistent-checkpoint recovery")
public class MysqlCDCWithSparkSchemaRecoveryIT extends TestSuiteBase implements TestResource {
    private static final long ASSERT_TIMEOUT_SECONDS =
            Long.getLong("seatunnel.e2e.spark-recovery.timeout.seconds", 180L);
    private static final String CONF = "/mysqlcdc_to_mysql_with_spark_schema_recovery.conf";
    private static final String CHECKPOINT = "/tmp/seatunnel_mnt/spark-schema-recovery/sink-0";
    private final MySqlContainer mysql =
            new MySqlContainer(MySqlVersion.V8_0)
                    .withConfigurationOverride("docker/server-gtids/my.cnf")
                    .withSetupSQL("docker/setup.sql")
                    .withNetwork(NETWORK)
                    .withNetworkAliases("mysql_cdc_e2e")
                    .withDatabaseName("mysql_cdc")
                    .withUsername("mysqluser")
                    .withPassword("mysqlpw");

    @TestContainerExtension
    protected final ContainerExtendedFactory driver =
            container ->
                    DependencyJar.ofClassName("com.mysql.cj.jdbc.Driver")
                            .copyTo(container, "/tmp/seatunnel/plugins/MySQL-CDC/lib");

    @BeforeAll
    @Override
    public void startUp() {
        mysql.start();
    }

    @AfterAll
    @Override
    public void tearDown() {
        mysql.stop();
    }

    private Connection connection() throws Exception {
        return DriverManager.getConnection(
                mysql.getJdbcUrl()
                        + (mysql.getJdbcUrl().contains("?") ? "&" : "?")
                        + "connectTimeout=10000&socketTimeout=30000",
                mysql.getUsername(),
                mysql.getPassword());
    }

    private void sql(String sql) throws Exception {
        try (Connection connection = connection();
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private List<String> rows(String sql) throws Exception {
        List<String> result = new ArrayList<>();
        try (Connection connection = connection();
                Statement statement = connection.createStatement();
                ResultSet rows = statement.executeQuery(sql)) {
            while (rows.next()) {
                List<String> cells = new ArrayList<>();
                for (int i = 1; i <= rows.getMetaData().getColumnCount(); i++)
                    cells.add(String.valueOf(rows.getObject(i)));
                result.add(String.join("|", cells));
            }
        }
        return result;
    }

    private CompletableFuture<org.testcontainers.containers.Container.ExecResult> start(
            TestContainer container) {
        return start(container, CONF);
    }

    private CompletableFuture<org.testcontainers.containers.Container.ExecResult> start(
            TestContainer container, String configuration) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return container.executeJob(configuration);
                    } catch (Exception error) {
                        throw new java.util.concurrent.CompletionException(error);
                    }
                });
    }

    private void running(
            CompletableFuture<org.testcontainers.containers.Container.ExecResult> execution) {
        if (execution.isDone()) {
            try {
                Assertions.fail(
                        "Spark job terminated before its recovery assertion: " + execution.get());
            } catch (Exception error) {
                Assertions.fail("Spark recovery execution failed", error);
            }
        }
    }

    private void committedAfterRows(
            AbstractTestSparkContainer spark,
            CompletableFuture<org.testcontainers.containers.Container.ExecResult> execution,
            boolean evolved,
            boolean offline)
            throws Exception {
        long observed = spark.latestCompletedSparkEpoch(CHECKPOINT);
        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            running(execution);
                            long durable =
                                    org.apache.seatunnel.translation.spark.source
                                            .SparkRecoveryCheckpointAssertions.assertData(
                                            spark.sparkCheckpointOnHost(
                                                    "spark-schema-recovery/sink-0"),
                                            evolved,
                                            offline);
                            Assertions.assertTrue(durable > observed);
                        });
    }

    private void prepareTables() throws Exception {
        for (String name :
                Arrays.asList(
                        "spark_restore_a_sink",
                        "spark_restore_b_sink",
                        "spark_restore_a",
                        "spark_restore_b",
                        "spark_restore_audit")) sql("DROP TABLE IF EXISTS " + name);
        sql("CREATE TABLE spark_restore_a(id INT PRIMARY KEY,name VARCHAR(64))");
        sql("CREATE TABLE spark_restore_b(id INT PRIMARY KEY,name VARCHAR(64))");
        sql("CREATE TABLE spark_restore_a_sink LIKE spark_restore_a");
        sql("CREATE TABLE spark_restore_b_sink LIKE spark_restore_b");
        sql("CREATE TABLE spark_restore_audit(kind VARCHAR(10),id INT)");
        for (String table : Arrays.asList("spark_restore_a_sink", "spark_restore_b_sink")) {
            sql(
                    "CREATE TRIGGER "
                            + table
                            + "_insert AFTER INSERT ON "
                            + table
                            + " FOR EACH ROW INSERT INTO spark_restore_audit VALUES('insert',NEW.id)");
            sql(
                    "CREATE TRIGGER "
                            + table
                            + "_update AFTER UPDATE ON "
                            + table
                            + " FOR EACH ROW INSERT INTO spark_restore_audit VALUES('update',NEW.id)");
        }
        sql("INSERT INTO spark_restore_a VALUES(1,'seed-a')");
        sql("INSERT INTO spark_restore_b VALUES(10,'seed-b')");
    }

    @TestTemplate
    void keepsLiveDiscoveredSchemaWhenPropagationDisabled(TestContainer container)
            throws Exception {
        AbstractTestSparkContainer spark = (AbstractTestSparkContainer) container;
        prepareTables();
        String configuration = "/mysqlcdc_to_mysql_with_spark_disabled_schema_recovery.conf";
        CompletableFuture<org.testcontainers.containers.Container.ExecResult> execution =
                start(container, configuration);
        final CompletableFuture<org.testcontainers.containers.Container.ExecResult> initial =
                execution;
        Throwable failure = null;
        try {
            await().pollInterval(1, TimeUnit.SECONDS)
                    .atMost(ASSERT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                running(initial);
                                Assertions.assertEquals(
                                        Arrays.asList("1|seed-a"),
                                        rows("SELECT * FROM spark_restore_a_sink ORDER BY id"));
                                Assertions.assertEquals(
                                        Arrays.asList("10|seed-b"),
                                        rows("SELECT * FROM spark_restore_b_sink ORDER BY id"));
                            });
            committedAfterRows(spark, execution, false, false);
            committedAfterRows(spark, execution, false, false);
            spark.restartSparkRuntimeForRecovery();
            execution.handle((result, error) -> null).get(1, TimeUnit.MINUTES);
            sql("ALTER TABLE spark_restore_a ADD COLUMN score INT");
            sql("ALTER TABLE spark_restore_a_sink ADD COLUMN score INT");
            sql("INSERT INTO spark_restore_a VALUES(2,'restored-a',42)");
            sql("INSERT INTO spark_restore_b VALUES(20,'restored-b')");
            execution = start(container, configuration);
            final CompletableFuture<org.testcontainers.containers.Container.ExecResult> restored =
                    execution;
            await().pollInterval(1, TimeUnit.SECONDS)
                    .atMost(ASSERT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                running(restored);
                                Assertions.assertEquals(
                                        Arrays.asList("1|seed-a|null", "2|restored-a|42"),
                                        rows("SELECT * FROM spark_restore_a_sink ORDER BY id"));
                                Assertions.assertEquals(
                                        Arrays.asList("10|seed-b", "20|restored-b"),
                                        rows("SELECT * FROM spark_restore_b_sink ORDER BY id"));
                            });
            committedAfterRows(spark, execution, true, false);
            Assertions.assertEquals(
                    Arrays.asList("insert|4"),
                    rows("SELECT kind,COUNT(*) FROM spark_restore_audit GROUP BY kind"));
            Assertions.assertEquals(
                    Arrays.asList("id|int|null|NO", "name|varchar|64|YES", "score|int|null|YES"),
                    rows(
                            "SELECT COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,IS_NULLABLE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='mysql_cdc' AND TABLE_NAME='spark_restore_a_sink' ORDER BY ORDINAL_POSITION"));
        } catch (Exception | Error error) {
            failure = error;
            throw error;
        } finally {
            try {
                if (failure == null) running(execution);
                if (!execution.isDone()) {
                    spark.stopSparkRuntimeForRecovery();
                    execution.handle((result, error) -> null).get(1, TimeUnit.MINUTES);
                }
            } catch (Exception | Error cleanup) {
                if (failure != null) {
                    if (failure != cleanup) failure.addSuppressed(cleanup);
                } else throw cleanup;
            }
        }
        Assertions.assertEquals(
                Arrays.asList("1|seed-a|null", "2|restored-a|42"),
                rows("SELECT * FROM spark_restore_a_sink ORDER BY id"));
        Assertions.assertEquals(
                Arrays.asList("10|seed-b", "20|restored-b"),
                rows("SELECT * FROM spark_restore_b_sink ORDER BY id"));
        Assertions.assertEquals(
                Arrays.asList("insert|4"),
                rows("SELECT kind,COUNT(*) FROM spark_restore_audit GROUP BY kind"));
    }

    @TestTemplate
    void restoresTypedMultiTableRowsAfterLiveAndOfflineDdl(TestContainer container)
            throws Exception {
        AbstractTestSparkContainer spark = (AbstractTestSparkContainer) container;
        prepareTables();
        CompletableFuture<org.testcontainers.containers.Container.ExecResult> execution =
                start(container);
        final CompletableFuture<org.testcontainers.containers.Container.ExecResult> initial =
                execution;
        Throwable failure = null;
        try {
            await().pollInterval(1, TimeUnit.SECONDS)
                    .atMost(ASSERT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                running(initial);
                                Assertions.assertEquals(
                                        Arrays.asList("1|seed-a"),
                                        rows("SELECT * FROM spark_restore_a_sink ORDER BY id"));
                                Assertions.assertEquals(
                                        Arrays.asList("10|seed-b"),
                                        rows("SELECT * FROM spark_restore_b_sink ORDER BY id"));
                            });
            committedAfterRows(spark, execution, false, false);
            committedAfterRows(spark, execution, false, false);
            sql("ALTER TABLE spark_restore_a ADD COLUMN score INT");
            sql("INSERT INTO spark_restore_a VALUES(2,'live-a',42)");
            sql("INSERT INTO spark_restore_b VALUES(20,'live-b')");
            await().pollInterval(1, TimeUnit.SECONDS)
                    .atMost(ASSERT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                running(initial);
                                Assertions.assertEquals(
                                        Arrays.asList("2|live-a|42"),
                                        rows("SELECT * FROM spark_restore_a_sink WHERE id=2"));
                                Assertions.assertEquals(
                                        Arrays.asList("20|live-b"),
                                        rows("SELECT * FROM spark_restore_b_sink WHERE id=20"));
                            });
            committedAfterRows(spark, execution, true, false);
            spark.restartSparkRuntimeForRecovery();
            execution.handle((result, error) -> null).get(1, TimeUnit.MINUTES);
            // Live discovery now differs from the completed checkpoint's schema.
            sql("ALTER TABLE spark_restore_a ADD COLUMN note VARCHAR(16)");
            sql("INSERT INTO spark_restore_a VALUES(3,'restored-a',77,'offline-ddl')");
            sql("INSERT INTO spark_restore_b VALUES(30,'restored-b')");
            execution = start(container);
            final CompletableFuture<org.testcontainers.containers.Container.ExecResult> restored =
                    execution;
            await().pollInterval(1, TimeUnit.SECONDS)
                    .atMost(ASSERT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                running(restored);
                                Assertions.assertEquals(
                                        Arrays.asList(
                                                "1|seed-a|null|null",
                                                "2|live-a|42|null",
                                                "3|restored-a|77|offline-ddl"),
                                        rows("SELECT * FROM spark_restore_a_sink ORDER BY id"));
                                Assertions.assertEquals(
                                        Arrays.asList("10|seed-b", "20|live-b", "30|restored-b"),
                                        rows("SELECT * FROM spark_restore_b_sink ORDER BY id"));
                            });
            committedAfterRows(spark, execution, true, true);
            Assertions.assertEquals(
                    Arrays.asList(
                            "id|int|null|NO",
                            "name|varchar|64|YES",
                            "score|int|null|YES",
                            "note|varchar|16|YES"),
                    rows(
                            "SELECT COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,IS_NULLABLE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='mysql_cdc' AND TABLE_NAME='spark_restore_a_sink' ORDER BY ORDINAL_POSITION"));
            Assertions.assertEquals(
                    Arrays.asList("insert|6"),
                    rows("SELECT kind,COUNT(*) FROM spark_restore_audit GROUP BY kind"));
        } catch (Exception | Error error) {
            failure = error;
            throw error;
        } finally {
            try {
                if (failure == null) running(execution);
                if (!execution.isDone()) {
                    spark.stopSparkRuntimeForRecovery();
                    execution.handle((result, error) -> null).get(1, TimeUnit.MINUTES);
                }
            } catch (Exception | Error cleanup) {
                if (failure != null) {
                    if (failure != cleanup) failure.addSuppressed(cleanup);
                } else throw cleanup;
            }
        }
        Assertions.assertEquals(
                Arrays.asList(
                        "1|seed-a|null|null", "2|live-a|42|null", "3|restored-a|77|offline-ddl"),
                rows("SELECT * FROM spark_restore_a_sink ORDER BY id"));
        Assertions.assertEquals(
                Arrays.asList("10|seed-b", "20|live-b", "30|restored-b"),
                rows("SELECT * FROM spark_restore_b_sink ORDER BY id"));
        Assertions.assertEquals(
                Arrays.asList("insert|6"),
                rows("SELECT kind,COUNT(*) FROM spark_restore_audit GROUP BY kind"));
    }
}
