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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.UniqueDatabase;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.flink.AbstractTestFlinkContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.DependencyJar;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.SEATUNNEL},
        disabledReason =
                "Currently SPARK do not support cdc, only test the change process related to Flink.")
public class MysqlCDCWithFlinkSchemaChangeIT extends TestSuiteBase implements TestResource {
    /**
     * Flink schema evolution can restart after XA recover/rollback on loaded CI runners, so these
     * assertions need enough time for the job to recover and replay the schema-change event. The
     * long bound is a failure deadline, not the expected recovery duration.
     */
    private static final long SCHEMA_EVOLUTION_ASSERT_TIMEOUT_MILLIS = 600_000L;

    private static final long STRUCTURE_AND_DATA_ASSERT_TIMEOUT_MILLIS = 300_000L;
    /**
     * The timestamp default is evaluated by different MySQL statements during CDC replay, so this
     * tolerance allows loaded CI scheduling jitter without hiding minute-level CDC lag.
     */
    private static final int MAX_TIMESTAMP_DRIFT_SECONDS = 45;

    private static final HttpClient FLINK_REST_CLIENT =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    private static final String MYSQL_DATABASE = "shop";
    private static final String SOURCE_TABLE = "products";
    private static final String SINK_TABLE = "mysql_cdc_e2e_sink_table_with_schema_change";
    private static final String SINK_TABLE2 =
            "mysql_cdc_e2e_sink_table_with_schema_change_exactly_once";
    private static final String MYSQL_HOST = "mysql_cdc_e2e";
    private static final String MYSQL_USER_NAME = "mysqluser";
    private static final String MYSQL_USER_PASSWORD = "mysqlpw";

    private static final String DESC = "desc %s.%s";
    private static final String PROJECTION_QUERY =
            "select id,name,description,weight,add_column1,add_column2,add_column3 from %s.%s order by id;";

    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);

    private final UniqueDatabase shopDatabase =
            new UniqueDatabase(
                    MYSQL_CONTAINER, MYSQL_DATABASE, "mysqluser", "mysqlpw", MYSQL_DATABASE);

    private static MySqlContainer createMySqlContainer(MySqlVersion version) {
        return new MySqlContainer(version)
                .withConfigurationOverride("docker/server-gtids/my.cnf")
                .withSetupSQL("docker/setup.sql")
                .withNetwork(NETWORK)
                .withNetworkAliases(MYSQL_HOST)
                .withDatabaseName(MYSQL_DATABASE)
                .withUsername(MYSQL_USER_NAME)
                .withPassword(MYSQL_USER_PASSWORD)
                .withLogConsumer(
                        new Slf4jLogConsumer(DockerLoggerFactory.getLogger("mysql-docker-image")));
    }

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container ->
                    DependencyJar.ofClassName("com.mysql.cj.jdbc.Driver")
                            .copyTo(container, "/tmp/seatunnel/plugins/MySQL-CDC/lib");

    @Order(1)
    @TestTemplate
    public void testMysqlCdcWithSchemaEvolutionCase(TestContainer container) {
        // Reset database to initial state to avoid issues from previous test runs
        resetDatabaseToInitialState();

        String jobConfigFile = "/mysqlcdc_to_mysql_with_flink_schema_change.conf";
        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob(jobConfigFile);
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });

        // waiting for case1 completed
        assertSchemaEvolutionForAddColumns(MYSQL_DATABASE, SOURCE_TABLE, SINK_TABLE);

        // case2 drop columns with cdc data at same time
        shopDatabase.setTemplateName("drop_columns").createAndInitialize();

        // waiting for case2 completed
        assertTableStructureAndData(MYSQL_DATABASE, SOURCE_TABLE, SINK_TABLE);

        // case3 change column name with cdc data at same time
        shopDatabase.setTemplateName("change_columns").createAndInitialize();

        // case4 modify column data type with cdc data at same time
        shopDatabase.setTemplateName("modify_columns").createAndInitialize();

        // waiting for case3/case4 completed
        assertTableStructureAndData(MYSQL_DATABASE, SOURCE_TABLE, SINK_TABLE);

        // case5 comment changes with cdc data at same time
        shopDatabase.setTemplateName("comment_changes").createAndInitialize();

        // waiting for case5 completed – data must continue flowing after comment DDL
        assertTableStructureAndData(MYSQL_DATABASE, SOURCE_TABLE, SINK_TABLE);

        // verify the final source table comment was applied (tests comment update idempotency)
        assertSourceTableComment(
                MYSQL_DATABASE, SOURCE_TABLE, "Updated product catalog with sports equipment");
    }

    @Order(2)
    @TestTemplate
    public void testMysqlCdcWithSchemaEvolutionCaseExactlyOnce(TestContainer container) {

        shopDatabase.setTemplateName("shop").createAndInitialize();
        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob(
                                "/mysqlcdc_to_mysql_with_flink_schema_change_exactly_once.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });

        assertSchemaEvolution(MYSQL_DATABASE, SOURCE_TABLE, SINK_TABLE2);
    }

    @Order(3)
    @TestTemplate
    public void testFirstEvolvedRowsAfterSavepointRestore(TestContainer container)
            throws Exception {
        assertCollectorSchemaRestore((AbstractTestFlinkContainer) container, true, false, false);
    }

    @Order(4)
    @TestTemplate
    public void testFirstEvolvedRowsAfterSavepointRestoreExactlyOnce(TestContainer container)
            throws Exception {
        assertCollectorSchemaRestore((AbstractTestFlinkContainer) container, true, true, true);
    }

    @Order(5)
    @TestTemplate
    public void testSchemaDisabledRestoreKeepsLiveColumns(TestContainer container)
            throws Exception {
        assertCollectorSchemaRestore((AbstractTestFlinkContainer) container, false, false, false);
    }

    @Order(6)
    @TestTemplate
    public void testFirstEvolvedRowsAfterCheckpointRecovery(TestContainer container)
            throws Exception {
        assertCollectorSchemaRestore((AbstractTestFlinkContainer) container, true, false, true);
    }

    private void assertCollectorSchemaRestore(
            AbstractTestFlinkContainer container,
            boolean schemaChanges,
            boolean exactlyOnce,
            boolean failWorker)
            throws Exception {
        String config = "/mysqlcdc_to_mysql_with_flink_schema_restore.conf";
        List<String> variables =
                Arrays.asList(
                        "restore_schema_enabled=" + schemaChanges,
                        "restore_exactly_once=" + exactlyOnce);
        executeUpdate(
                "DROP TABLE IF EXISTS shop.collector_products,shop.collector_other,"
                        + "shop.collector_sink_collector_products,shop.collector_sink_collector_other,shop.collector_delivery_audit");
        executeUpdate("CREATE TABLE shop.collector_products(id INT PRIMARY KEY,name VARCHAR(64))");
        executeUpdate("CREATE TABLE shop.collector_other(id INT PRIMARY KEY,amount DECIMAL(12,2))");
        executeUpdate(
                "CREATE TABLE shop.collector_sink_collector_products LIKE shop.collector_products");
        executeUpdate("CREATE TABLE shop.collector_sink_collector_other LIKE shop.collector_other");
        if (exactlyOnce) {
            // Both insert and duplicate-key update must be audited. An upsert-only comparison
            // would conceal a replayed transaction after checkpoint recovery.
            executeUpdate("CREATE TABLE shop.collector_delivery_audit(id INT PRIMARY KEY)");
            executeUpdate(
                    "CREATE TRIGGER shop.collector_insert_audit AFTER INSERT ON shop.collector_sink_collector_products FOR EACH ROW INSERT INTO shop.collector_delivery_audit VALUES(NEW.id)");
            executeUpdate(
                    "CREATE TRIGGER shop.collector_update_audit AFTER UPDATE ON shop.collector_sink_collector_products FOR EACH ROW INSERT INTO shop.collector_delivery_audit VALUES(NEW.id)");
        }
        executeUpdate("INSERT INTO shop.collector_products VALUES(1,'snapshot')");
        executeUpdate("INSERT INTO shop.collector_other VALUES(1,12.34)");

        String job = null;
        try {
            job = submitDetached(container, config, null, variables);
            awaitCollectorRow("collector_products", 1, Arrays.asList(1, "snapshot"));
            awaitCollectorRow("collector_other", 1, Arrays.asList(1, new BigDecimal("12.34")));
            // A binlog record must arrive before taking the incremental-phase savepoint.
            executeUpdate("INSERT INTO shop.collector_products VALUES(2,'incremental')");
            awaitCollectorRow("collector_products", 2, Arrays.asList(2, "incremental"));
            if (schemaChanges) {
                executeUpdate(
                        "ALTER TABLE shop.collector_products ADD COLUMN score BIGINT DEFAULT 0");
                executeUpdate(
                        "INSERT INTO shop.collector_products VALUES(3,'evolved',1234567890123)");
                awaitCollectorRow(
                        "collector_products", 3, Arrays.asList(3, "evolved", 1234567890123L));
            }

            String savepoint = cancelWithSavepoint(container, job);
            job = null;
            if (!schemaChanges) {
                // Disabled propagation must retain the newly discovered live shape. The sink
                // is deliberately changed manually because this job does not propagate DDL.
                executeUpdate(
                        "ALTER TABLE shop.collector_products ADD COLUMN score BIGINT DEFAULT 0");
                executeUpdate(
                        "ALTER TABLE shop.collector_sink_collector_products ADD COLUMN score BIGINT DEFAULT 0");
            }
            executeUpdate(
                    "INSERT INTO shop.collector_products VALUES(4,'first-restored',1234567890124)");
            executeUpdate("INSERT INTO shop.collector_other VALUES(4,56.78)");
            executeUpdate("SET GLOBAL log_output='TABLE'");
            executeUpdate("SET GLOBAL general_log='ON'");
            String marker =
                    String.valueOf(
                            query("SELECT DATE_FORMAT(NOW(6),'%Y-%m-%d %H:%i:%s.%f')")
                                    .get(0)
                                    .get(0));
            job = submitDetached(container, config, savepoint, variables);
            String restoredJob = job;
            await().atMost(STRUCTURE_AND_DATA_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                JsonNode restored =
                                        flinkRest(
                                                        container,
                                                        "GET",
                                                        "/jobs/" + restoredJob + "/checkpoints",
                                                        null)
                                                .path("latest")
                                                .path("restored");
                                Assertions.assertTrue(restored.path("is_savepoint").asBoolean());
                                Assertions.assertEquals(
                                        savepoint, restored.path("external_path").asText());
                            });
            awaitCollectorRow(
                    "collector_products", 4, Arrays.asList(4, "first-restored", 1234567890124L));
            awaitCollectorRow("collector_other", 4, Arrays.asList(4, new BigDecimal("56.78")));
            // A failed first conversion must not be hidden by a later successful retry.
            JsonNode firstRowCheckpoints =
                    flinkRest(container, "GET", "/jobs/" + restoredJob + "/checkpoints", null);
            Assertions.assertEquals(
                    1,
                    firstRowCheckpoints.path("counts").path("restored").asInt(),
                    firstRowCheckpoints.toString());
            Assertions.assertTrue(
                    firstRowCheckpoints
                            .path("latest")
                            .path("restored")
                            .path("is_savepoint")
                            .asBoolean());
            Assertions.assertEquals(
                    savepoint,
                    firstRowCheckpoints
                            .path("latest")
                            .path("restored")
                            .path("external_path")
                            .asText());
            if (failWorker) {
                await().atMost(STRUCTURE_AND_DATA_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                        .until(
                                () ->
                                        flinkRest(
                                                                container,
                                                                "GET",
                                                                "/jobs/"
                                                                        + restoredJob
                                                                        + "/checkpoints",
                                                                null)
                                                        .path("counts")
                                                        .path("completed")
                                                        .asInt()
                                                > 0);
                int restoredCount =
                        flinkRest(container, "GET", "/jobs/" + restoredJob + "/checkpoints", null)
                                .path("counts")
                                .path("restored")
                                .asInt();
                failWorkerDuringUncommittedWrite(container, exactlyOnce);
                await().atMost(STRUCTURE_AND_DATA_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                        .untilAsserted(
                                () -> {
                                    JsonNode checkpoints =
                                            flinkRest(
                                                    container,
                                                    "GET",
                                                    "/jobs/" + restoredJob + "/checkpoints",
                                                    null);
                                    Assertions.assertTrue(
                                            checkpoints.path("counts").path("restored").asInt()
                                                    > restoredCount,
                                            checkpoints.toString());
                                    Assertions.assertFalse(
                                            checkpoints
                                                    .path("latest")
                                                    .path("restored")
                                                    .path("is_savepoint")
                                                    .asBoolean());
                                });
                awaitCollectorRow(
                        "collector_products",
                        5,
                        Arrays.asList(5, "first-after-failure", 1234567890125L));
                if (exactlyOnce) {
                    Assertions.assertEquals(
                            Arrays.asList(
                                    Collections.singletonList(1),
                                    Collections.singletonList(2),
                                    Collections.singletonList(3),
                                    Collections.singletonList(4),
                                    Collections.singletonList(5)),
                            query("SELECT id FROM shop.collector_delivery_audit ORDER BY id"));
                }
            }
            Assertions.assertIterableEquals(
                    query("SHOW COLUMNS FROM shop.collector_products"),
                    query("SHOW COLUMNS FROM shop.collector_sink_collector_products"));
            Assertions.assertIterableEquals(
                    query("SHOW COLUMNS FROM shop.collector_other"),
                    query("SHOW COLUMNS FROM shop.collector_sink_collector_other"));
            Assertions.assertTrue(
                    query(
                                    "SELECT argument FROM mysql.general_log WHERE event_time >= '"
                                            + marker
                                            + "' AND command_type='Query' AND argument REGEXP '^ALTER TABLE ' AND argument LIKE '%collector_sink_%'")
                            .isEmpty(),
                    "Restoring local collector state must not replay sink DDL");
        } finally {
            try {
                if (job != null) {
                    String canceledJob = job;
                    List<String> terminal = Arrays.asList("CANCELED", "FAILED", "FINISHED");
                    if (!terminal.contains(
                            flinkRest(container, "GET", "/jobs/" + job, null)
                                    .path("state")
                                    .asText())) {
                        flinkRest(container, "PATCH", "/jobs/" + job + "?mode=cancel", null);
                        await().atMost(
                                        STRUCTURE_AND_DATA_ASSERT_TIMEOUT_MILLIS,
                                        TimeUnit.MILLISECONDS)
                                .until(
                                        () ->
                                                terminal.contains(
                                                        flinkRest(
                                                                        container,
                                                                        "GET",
                                                                        "/jobs/" + canceledJob,
                                                                        null)
                                                                .path("state")
                                                                .asText()));
                    }
                }
            } finally {
                executeUpdate("SET GLOBAL general_log='OFF'");
            }
        }
    }

    private void awaitCollectorRow(String table, int id, List<Object> expected) {
        await().atMost(STRUCTURE_AND_DATA_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        Collections.singletonList(expected),
                                        query(
                                                "SELECT * FROM shop.collector_sink_"
                                                        + table
                                                        + " WHERE id="
                                                        + id)));
    }

    private void failWorkerDuringUncommittedWrite(
            AbstractTestFlinkContainer container, boolean exactlyOnce) throws Exception {
        boolean killed = false;
        try (Connection blocker = getJdbcConnection();
                Statement statement = blocker.createStatement()) {
            blocker.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            blocker.setAutoCommit(false);
            long blockerId;
            try (ResultSet id = statement.executeQuery("SELECT CONNECTION_ID()")) {
                Assertions.assertTrue(id.next());
                blockerId = id.getLong(1);
            }
            try {
                // Lock the absent sink/audit key so the next sink insert waits before becoming
                // visible. The XA case waits inside its active transaction's audit trigger.
                String lockedTable =
                        exactlyOnce
                                ? "collector_delivery_audit"
                                : "collector_sink_collector_products";
                try (ResultSet ignored =
                        statement.executeQuery(
                                "SELECT id FROM shop." + lockedTable + " WHERE id=5 FOR UPDATE")) {
                    Assertions.assertFalse(ignored.next());
                }
                executeUpdate(
                        "INSERT INTO shop.collector_products VALUES(5,'first-after-failure',1234567890125)");
                await().atMost(Duration.ofSeconds(30))
                        .until(
                                () ->
                                        ((Number)
                                                                query(
                                                                                "SELECT COUNT(*) FROM information_schema.innodb_trx WHERE trx_state='LOCK WAIT' AND trx_mysql_thread_id<>"
                                                                                        + blockerId
                                                                                        + " AND (trx_query LIKE '%collector_delivery_audit%' OR trx_query LIKE '%collector_sink_collector_products%')")
                                                                        .get(0)
                                                                        .get(0))
                                                        .longValue()
                                                > 0);
                Assertions.assertTrue(
                        query("SELECT id FROM shop.collector_sink_collector_products WHERE id=5")
                                .isEmpty());
                container.killTaskManager();
                killed = true;
            } finally {
                blocker.rollback();
            }
        } finally {
            if (killed) {
                container.restartTaskManager();
            }
        }
    }

    private String submitDetached(
            AbstractTestFlinkContainer container,
            String config,
            String savepoint,
            List<String> variables)
            throws Exception {
        Container.ExecResult result = container.executeJobDetached(config, savepoint, variables);
        Assertions.assertEquals(0, result.getExitCode(), result.getStdout() + result.getStderr());
        Matcher match = Pattern.compile("JobID ([0-9a-f]{32})").matcher(result.getStdout());
        Assertions.assertTrue(match.find(), result.getStdout());
        String job = match.group(1);
        await().atMost(STRUCTURE_AND_DATA_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .until(
                        () ->
                                "RUNNING"
                                        .equals(
                                                flinkRest(container, "GET", "/jobs/" + job, null)
                                                        .path("state")
                                                        .asText()));
        return job;
    }

    private String cancelWithSavepoint(AbstractTestFlinkContainer container, String job)
            throws Exception {
        String operation =
                flinkRest(
                                container,
                                "POST",
                                "/jobs/" + job + "/savepoints",
                                "{\"target-directory\":\"file:///tmp/seatunnel_mnt/collector-savepoints\",\"cancel-job\":true}")
                        .path("request-id")
                        .asText();
        AtomicReference<String> location = new AtomicReference<>();
        await().atMost(STRUCTURE_AND_DATA_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            JsonNode result =
                                    flinkRest(
                                            container,
                                            "GET",
                                            "/jobs/" + job + "/savepoints/" + operation,
                                            null);
                            Assertions.assertEquals(
                                    "COMPLETED", result.path("status").path("id").asText());
                            Assertions.assertFalse(
                                    result.path("operation").has("failure-cause"),
                                    result.toString());
                            location.set(result.path("operation").path("location").asText());
                            Assertions.assertFalse(location.get().isEmpty(), result.toString());
                        });
        await().atMost(STRUCTURE_AND_DATA_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .until(
                        () ->
                                "CANCELED"
                                        .equals(
                                                flinkRest(container, "GET", "/jobs/" + job, null)
                                                        .path("state")
                                                        .asText()));
        return location.get();
    }

    private JsonNode flinkRest(
            AbstractTestFlinkContainer container, String method, String path, String body)
            throws Exception {
        HttpRequest request =
                HttpRequest.newBuilder(
                                URI.create(
                                        "http://"
                                                + container.getJobManagerHost()
                                                + ":"
                                                + container.getJobManagerRestPort()
                                                + path))
                        .timeout(Duration.ofSeconds(30))
                        .header("Content-Type", "application/json")
                        .method(
                                method,
                                body == null
                                        ? HttpRequest.BodyPublishers.noBody()
                                        : HttpRequest.BodyPublishers.ofString(body))
                        .build();
        HttpResponse<String> response =
                FLINK_REST_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertTrue(
                response.statusCode() >= 200 && response.statusCode() < 300, response.body());
        return response.body().isEmpty()
                ? JsonUtils.toJsonNode(Collections.emptyMap())
                : JsonUtils.stringToJsonNode(response.body());
    }

    private void assertSchemaEvolution(String database, String sourceTable, String sinkTable) {
        await().atMost(SCHEMA_EVOLUTION_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                assertTableDataEqualsBySourceColumnOrder(
                                        database, sourceTable, sinkTable, null));
        awaitIncrementalStreamingReady(database, sourceTable, sinkTable);

        // case1 add columns with cdc data at same time
        shopDatabase.setTemplateName("add_columns").createAndInitialize();
        await().atMost(SCHEMA_EVOLUTION_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                assertSchemaDescriptionEqualsIgnoringColumnOrder(
                                        database, sourceTable, sinkTable));
        await().atMost(SCHEMA_EVOLUTION_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            assertTableDataEqualsBySourceColumnOrder(
                                    database, sourceTable, sinkTable, "id >= 128");

                            Assertions.assertIterableEquals(
                                    query(String.format(PROJECTION_QUERY, database, sourceTable)),
                                    query(String.format(PROJECTION_QUERY, database, sinkTable)));

                            // The default value of add_column4 is current_timestamp(), so the
                            // history data of the sink table can lag briefly behind the source
                            // table while the schema-change event is still propagating.
                            String query =
                                    String.format(
                                            "SELECT t1.id AS table1_id, t1.add_column4 AS table1_timestamp, "
                                                    + "t2.id AS table2_id, t2.add_column4 AS table2_timestamp, "
                                                    + "ABS(TIMESTAMPDIFF(SECOND, t1.add_column4, t2.add_column4)) AS time_diff "
                                                    + "FROM %s.%s t1 "
                                                    + "INNER JOIN %s.%s t2 ON t1.id = t2.id",
                                            database, sourceTable, database, sinkTable);
                            try (Connection jdbcConnection = getJdbcConnection();
                                    Statement statement = jdbcConnection.createStatement();
                                    ResultSet resultSet = statement.executeQuery(query); ) {
                                while (resultSet.next()) {
                                    int timeDiff = resultSet.getInt("time_diff");
                                    Assertions.assertTrue(
                                            timeDiff <= MAX_TIMESTAMP_DRIFT_SECONDS,
                                            "Time difference exceeds "
                                                    + MAX_TIMESTAMP_DRIFT_SECONDS
                                                    + " seconds: "
                                                    + timeDiff
                                                    + " seconds");
                                }
                            }
                        });

        // case2 drop columns with cdc data at same time
        assertCaseByDdlName("drop_columns", database, sourceTable, sinkTable);

        // case3 change column name with cdc data at same time
        assertCaseByDdlName("change_columns", database, sourceTable, sinkTable);

        // case4 modify column data type with cdc data at same time
        assertCaseByDdlName("modify_columns", database, sourceTable, sinkTable);

        // case5 comment changes with cdc data at same time
        assertCaseByDdlName("comment_changes", database, sourceTable, sinkTable);
        assertSourceTableComment(
                database, sourceTable, "Updated product catalog with sports equipment");
    }

    private void assertCaseByDdlName(
            String drop_columns, String database, String sourceTable, String sinkTable) {
        shopDatabase.setTemplateName(drop_columns).createAndInitialize();
        assertTableStructureAndData(database, sourceTable, sinkTable);
    }

    /**
     * Flink JDBC schema evolution can materialize columns in a different physical order even when
     * the effective schema matches, so normalize DESCRIBE output by column name before asserting.
     */
    private void assertSchemaDescriptionEqualsIgnoringColumnOrder(
            String database, String sourceTable, String sinkTable) {
        Assertions.assertIterableEquals(
                normalizeDescRows(query(String.format(DESC, database, sourceTable))),
                normalizeDescRows(query(String.format(DESC, database, sinkTable))));
    }

    private void assertSchemaEvolutionForAddColumns(
            String database, String sourceTable, String sinkTable) {
        await().atMost(SCHEMA_EVOLUTION_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                assertTableDataEqualsBySourceColumnOrder(
                                        database, sourceTable, sinkTable, null));
        awaitIncrementalStreamingReady(database, sourceTable, sinkTable);

        // case1 add columns with cdc data at same time
        shopDatabase.setTemplateName("add_columns").createAndInitialize();
        await().pollInterval(5, TimeUnit.SECONDS)
                .atMost(SCHEMA_EVOLUTION_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                assertSchemaDescriptionEqualsIgnoringColumnOrder(
                                        database, sourceTable, sinkTable));
        await().pollInterval(5, TimeUnit.SECONDS)
                .atMost(SCHEMA_EVOLUTION_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            assertTableDataEqualsBySourceColumnOrder(
                                    database, sourceTable, sinkTable, "id >= 128");

                            Assertions.assertIterableEquals(
                                    query(String.format(PROJECTION_QUERY, database, sourceTable)),
                                    query(String.format(PROJECTION_QUERY, database, sinkTable)));

                            // The default value of add_column4 is current_timestamp(), so the
                            // history data of the sink table can lag briefly behind the source
                            // table while the schema-change event is still propagating.
                            String query =
                                    String.format(
                                            "SELECT t1.id AS table1_id, t1.add_column4 AS table1_timestamp, "
                                                    + "t2.id AS table2_id, t2.add_column4 AS table2_timestamp, "
                                                    + "ABS(TIMESTAMPDIFF(SECOND, t1.add_column4, t2.add_column4)) AS time_diff "
                                                    + "FROM %s.%s t1 "
                                                    + "INNER JOIN %s.%s t2 ON t1.id = t2.id",
                                            database, sourceTable, database, sinkTable);
                            try (Connection jdbcConnection = getJdbcConnection();
                                    Statement statement = jdbcConnection.createStatement();
                                    ResultSet resultSet = statement.executeQuery(query); ) {
                                while (resultSet.next()) {
                                    int timeDiff = resultSet.getInt("time_diff");
                                    Assertions.assertTrue(
                                            timeDiff <= MAX_TIMESTAMP_DRIFT_SECONDS,
                                            "Time difference exceeds "
                                                    + MAX_TIMESTAMP_DRIFT_SECONDS
                                                    + " seconds: "
                                                    + timeDiff
                                                    + " seconds");
                                }
                            }
                        });
    }

    private void assertTableStructureAndData(
            String database, String sourceTable, String sinkTable) {
        await().atMost(STRUCTURE_AND_DATA_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                assertSchemaDescriptionEqualsIgnoringColumnOrder(
                                        database, sourceTable, sinkTable));
        await().atMost(STRUCTURE_AND_DATA_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                assertTableDataEqualsBySourceColumnOrder(
                                        database, sourceTable, sinkTable, null));
    }

    /**
     * Waits until the incremental binlog reader is active before issuing schema changes.
     *
     * <p>Snapshot rows can reach the sink before the binlog reader connects. If DDL is issued in
     * that gap, Debezium can initialize from the post-DDL table schema and then replay pre-DDL row
     * events with fewer columns.
     */
    private void awaitIncrementalStreamingReady(
            String database, String sourceTable, String sinkTable) {
        String source = quoteIdentifier(database) + "." + quoteIdentifier(sourceTable);
        executeUpdate(
                "insert into "
                        + source
                        + " (id, name, description, weight)"
                        + " values (100, 'streaming-ready', 'streaming-ready', 0)");

        await().atMost(SCHEMA_EVOLUTION_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                assertTableDataEqualsBySourceColumnOrder(
                                        database, sourceTable, sinkTable, "id = 100"));

        executeUpdate("delete from " + source + " where id = 100");
        await().atMost(SCHEMA_EVOLUTION_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                assertTableDataEqualsBySourceColumnOrder(
                                        database, sourceTable, sinkTable, "id = 100"));
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                MYSQL_CONTAINER.getJdbcUrl(),
                MYSQL_CONTAINER.getUsername(),
                MYSQL_CONTAINER.getPassword());
    }

    /**
     * Read both tables using the source column order so data assertions stay stable when the sink
     * keeps equivalent columns but stores them in a different physical position.
     */
    private void assertTableDataEqualsBySourceColumnOrder(
            String database, String sourceTable, String sinkTable, String whereClause) {
        List<String> sourceColumns = getColumnNames(database, sourceTable);
        Assertions.assertIterableEquals(
                query(
                        buildOrderedProjectionQuery(
                                database, sourceTable, sourceColumns, whereClause)),
                query(
                        buildOrderedProjectionQuery(
                                database, sinkTable, sourceColumns, whereClause)));
    }

    /**
     * Returns source column names from DESCRIBE so later projections follow the semantic schema.
     */
    private List<String> getColumnNames(String database, String table) {
        List<String> columnNames = new ArrayList<>();
        for (List<Object> row : query(String.format(DESC, database, table))) {
            columnNames.add(String.valueOf(row.get(0)));
        }
        return columnNames;
    }

    /** Builds an explicit projection to avoid relying on engine-specific physical column order. */
    private String buildOrderedProjectionQuery(
            String database, String table, List<String> columns, String whereClause) {
        StringBuilder queryBuilder =
                new StringBuilder("select ")
                        .append(
                                columns.stream()
                                        .map(this::quoteIdentifier)
                                        .collect(Collectors.joining(",")))
                        .append(" from ")
                        .append(quoteIdentifier(database))
                        .append(".")
                        .append(quoteIdentifier(table));
        if (whereClause != null && !whereClause.isEmpty()) {
            queryBuilder.append(" where ").append(whereClause);
        }
        return queryBuilder.append(" order by id").toString();
    }

    /** Quotes identifiers because schema-change cases rename and reposition columns dynamically. */
    private String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    /** Sorts schema rows by column name so the assertion ignores physical column placement only. */
    private List<List<Object>> normalizeDescRows(List<List<Object>> descRows) {
        List<List<Object>> normalizedRows = new ArrayList<>(descRows);
        normalizedRows.sort(Comparator.comparing(row -> String.valueOf(row.get(0))));
        return normalizedRows;
    }

    @BeforeAll
    @Override
    public void startUp() {
        log.info("The second stage: Starting Mysql containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        log.info("Mysql Containers are started");
        shopDatabase.createAndInitialize();
        log.info("Mysql ddl execution is complete");
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (MYSQL_CONTAINER != null) {
            MYSQL_CONTAINER.close();
        }
    }

    private void resetDatabaseToInitialState() {
        try {
            log.info("Resetting database to initial state...");
            // Reset to original template and recreate database
            shopDatabase.setTemplateName(MYSQL_DATABASE).createAndInitialize();
            log.info("Database reset to initial state completed");
        } catch (Exception e) {
            log.error("Failed to reset database to initial state", e);
            throw new RuntimeException("Failed to reset database to initial state", e);
        }
    }

    /**
     * Asserts that the given source table's TABLE_COMMENT in {@code information_schema.TABLES}
     * matches {@code expectedComment}. This check is performed directly against the source MySQL
     * instance; it is intentionally not compared with the sink because the JDBC sink treats comment
     * events as no-ops.
     */
    private void assertSourceTableComment(String database, String table, String expectedComment) {
        String sql =
                String.format(
                        "SELECT TABLE_COMMENT FROM information_schema.TABLES"
                                + " WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'",
                        database, table);
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            Assertions.assertTrue(rs.next(), "Table " + table + " not found in information_schema");
            Assertions.assertEquals(
                    expectedComment,
                    rs.getString("TABLE_COMMENT"),
                    "Source table comment should have been updated by the ALTER TABLE COMMENT DDL");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void executeUpdate(String sql) {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private List<List<Object>> query(String sql) {
        try (Connection connection = getJdbcConnection()) {
            ResultSet resultSet = connection.createStatement().executeQuery(sql);
            List<List<Object>> result = new ArrayList<>();
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                ArrayList<Object> objects = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    objects.add(resultSet.getObject(i));
                }
                log.debug(String.format("Print MySQL-CDC query, sql: %s, data: %s", sql, objects));
                result.add(objects);
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
