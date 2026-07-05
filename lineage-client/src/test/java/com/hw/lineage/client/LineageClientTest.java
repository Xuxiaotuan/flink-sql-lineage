/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hw.lineage.client;

import com.google.common.collect.ImmutableMap;
import com.hw.lineage.common.model.LineageResult;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

/**
 * @description: LineageClientTest
 * @author: HamaWhite
 */
public class LineageClientTest {

    private static final Logger LOG = LoggerFactory.getLogger(LineageClientTest.class);

    private static final String[] PLUGIN_CODES = {"flink1.14.x", "flink1.16.x"};

    private static final String FLINK_21_PLUGIN_CODE = "flink2.1.x";

    private static final String catalogName = "memory_catalog";

    private static final String database = "lineage_db";

    private static LineageClient client;

    @BeforeClass
    public static void setup() {
        client = new LineageClient("target/plugins");

        Map<String, String> propertiesMap = ImmutableMap.of(
                "type", "generic_in_memory",
                "default-database", database);

        Stream.of(PLUGIN_CODES).forEach(pluginCode -> {
            client.createCatalog(pluginCode, catalogName, propertiesMap);

            client.useCatalog(pluginCode, catalogName);
            // create mysql cdc table ods_mysql_users
            createTableOfOdsMysqlUsers(pluginCode);
            // create hudi sink table dwd_hudi_users
            createTableOfDwdHudiUsers(pluginCode);
        });

        client.createCatalog(FLINK_21_PLUGIN_CODE, catalogName, propertiesMap);
        client.useCatalog(FLINK_21_PLUGIN_CODE, catalogName);
        createTableOfFlink21Source(FLINK_21_PLUGIN_CODE);
        createTableOfFlink21Sink(FLINK_21_PLUGIN_CODE);
        createTableOfFlink21UsersSource(FLINK_21_PLUGIN_CODE);
        createTableOfFlink21CompanySource(FLINK_21_PLUGIN_CODE);
        createTableOfFlink21UsersSink(FLINK_21_PLUGIN_CODE);
        createTableOfFlink21StatsSink(FLINK_21_PLUGIN_CODE);
    }

    @Test
    public void testInsertSelect() {
        Stream.of(PLUGIN_CODES).forEach(this::testInsertSelect);
    }

    @Test
    public void testFlink21InsertSelect() {
        String sql = "INSERT INTO flink21_sink " +
                "SELECT " +
                "   id ," +
                "   name ," +
                "   DATE_FORMAT(birthday, 'yyyyMMdd') " +
                "FROM" +
                "   flink21_source";

        String[][] expectedArray = {
                {"flink21_source", "id", "flink21_sink", "id"},
                {"flink21_source", "name", "flink21_sink", "name"},
                {"flink21_source", "birthday", "flink21_sink", "partition", "DATE_FORMAT(birthday, 'yyyyMMdd')"}
        };

        analyzeLineage(FLINK_21_PLUGIN_CODE, sql, expectedArray);
    }

    @Test
    public void testFlink21BasicDemo() {
        String sql = "INSERT INTO flink21_users_sink " +
                "SELECT " +
                "   id ," +
                "   name ," +
                "   CAST(NULL AS STRING) ," +
                "   birthday ," +
                "   CAST(score AS STRING) ," +
                "   DATE_FORMAT(birthday, 'yyyyMMdd') " +
                "FROM flink21_users_source";

        String[][] expectedArray = {
                {"flink21_users_source", "id", "flink21_users_sink", "id"},
                {"flink21_users_source", "name", "flink21_users_sink", "name"},
                {"flink21_users_source", "birthday", "flink21_users_sink", "birthday"},
                {"flink21_users_source", "score", "flink21_users_sink", "score_text",
                        "CAST(score):VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\""},
                {"flink21_users_source", "birthday", "flink21_users_sink", "partition_day",
                        "DATE_FORMAT(birthday, 'yyyyMMdd')"}
        };

        analyzeLineage(FLINK_21_PLUGIN_CODE, sql, expectedArray);
    }

    @Test
    public void testFlink21TransformDemo() {
        String sql = "INSERT INTO flink21_stats_sink " +
                "SELECT " +
                "   id ," +
                "   UPPER(name) ," +
                "   CAST(score / 10 AS BIGINT) ," +
                "   DATE_FORMAT(birthday, 'yyyy-MM-dd') " +
                "FROM flink21_users_source";

        String[][] expectedArray = {
                {"flink21_users_source", "id", "flink21_stats_sink", "id"},
                {"flink21_users_source", "name", "flink21_stats_sink", "name_upper", "UPPER(name)"},
                {"flink21_users_source", "score", "flink21_stats_sink", "score_bucket",
                        "CAST(/(score, 10)):BIGINT"},
                {"flink21_users_source", "birthday", "flink21_stats_sink", "birthday_day",
                        "DATE_FORMAT(birthday, 'yyyy-MM-dd')"}
        };

        analyzeLineage(FLINK_21_PLUGIN_CODE, sql, expectedArray);
    }

    @Test
    public void testFlink21JoinDemo() {
        String sql = "INSERT INTO flink21_users_sink " +
                "SELECT " +
                "   u.id ," +
                "   u.name ," +
                "   c.company_name ," +
                "   u.birthday ," +
                "   CAST(u.score AS STRING) ," +
                "   DATE_FORMAT(u.birthday, 'yyyyMMdd') " +
                "FROM flink21_users_source AS u " +
                "JOIN flink21_company_source AS c " +
                "ON u.id = c.user_id";

        String[][] expectedArray = {
                {"flink21_users_source", "id", "flink21_users_sink", "id"},
                {"flink21_users_source", "name", "flink21_users_sink", "name"},
                {"flink21_company_source", "company_name", "flink21_users_sink", "company_name"},
                {"flink21_users_source", "birthday", "flink21_users_sink", "birthday"},
                {"flink21_users_source", "score", "flink21_users_sink", "score_text",
                        "CAST(score):VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\""},
                {"flink21_users_source", "birthday", "flink21_users_sink", "partition_day",
                        "DATE_FORMAT(birthday, 'yyyyMMdd')"}
        };

        analyzeLineage(FLINK_21_PLUGIN_CODE, sql, expectedArray);
    }

    private void testInsertSelect(String pluginCode) {
        String sql = "INSERT INTO dwd_hudi_users " +
                "SELECT " +
                "   id ," +
                "   name ," +
                "   name as company_name ," +
                "   birthday ," +
                "   ts ," +
                "   DATE_FORMAT(birthday, 'yyyyMMdd') " +
                "FROM" +
                "   ods_mysql_users";

        String[][] expectedArray = {
                {"ods_mysql_users", "id", "dwd_hudi_users", "id"},
                {"ods_mysql_users", "name", "dwd_hudi_users", "name"},
                {"ods_mysql_users", "name", "dwd_hudi_users", "company_name"},
                {"ods_mysql_users", "birthday", "dwd_hudi_users", "birthday"},
                {"ods_mysql_users", "ts", "dwd_hudi_users", "ts"},
                {"ods_mysql_users", "birthday", "dwd_hudi_users", "partition", "DATE_FORMAT(birthday, 'yyyyMMdd')"}
        };

        analyzeLineage(pluginCode, sql, expectedArray);
    }

    private void analyzeLineage(String pluginCode, String sql, String[][] expectedArray) {
        List<LineageResult> actualList = client.analyzeLineage(pluginCode, catalogName, database, sql);
        LOG.info("Linage Result: ");
        actualList.forEach(e -> LOG.info(e.toString()));

        List<LineageResult> expectedList = LineageResult.buildResult(catalogName, database, expectedArray);
        assertEquals(expectedList, actualList);
    }

    /**
     * Create mysql cdc table ods_mysql_users
     */
    private static void createTableOfOdsMysqlUsers(String pluginCode) {
        client.execute(pluginCode, "DROP TABLE IF EXISTS ods_mysql_users ");

        client.execute(pluginCode, "CREATE TABLE IF NOT EXISTS ods_mysql_users (" +
                "       id                  BIGINT PRIMARY KEY NOT ENFORCED ," +
                "       name                STRING                          ," +
                "       birthday            TIMESTAMP(3)                    ," +
                "       ts                  TIMESTAMP(3)                    ," +
                "       proc_time as proctime()                              " +
                ") WITH ( " +
                "       'connector' = 'mysql-cdc'            ," +
                "       'hostname'  = '127.0.0.1'       ," +
                "       'port'      = '3306'                 ," +
                "       'username'  = 'root'                 ," +
                "       'password'  = 'xxx'          ," +
                "       'server-time-zone' = 'Asia/Shanghai' ," +
                "       'database-name' = 'demo'             ," +
                "       'table-name'    = 'users' " +
                ")");
    }

    /**
     * Create Hudi sink table dwd_hudi_users
     */
    private static void createTableOfDwdHudiUsers(String pluginCode) {
        client.execute(pluginCode, "DROP TABLE IF EXISTS dwd_hudi_users");

        client.execute(pluginCode, "CREATE TABLE IF NOT EXISTS  dwd_hudi_users ( " +
                "       id                  BIGINT PRIMARY KEY NOT ENFORCED ," +
                "       name                STRING                          ," +
                "       company_name        STRING                          ," +
                "       birthday            TIMESTAMP(3)                    ," +
                "       ts                  TIMESTAMP(3)                    ," +
                "        `partition`        VARCHAR(20)                      " +
                ") PARTITIONED BY (`partition`) WITH ( " +
                "       'connector' = 'hudi'                                    ," +
                "       'table.type' = 'COPY_ON_WRITE'                          ," +
                "       'read.streaming.enabled' = 'true'                       ," +
                "       'read.streaming.check-interval' = '1'                    " +
                ")");
    }

    private static void createTableOfFlink21Source(String pluginCode) {
        client.execute(pluginCode, "DROP TABLE IF EXISTS flink21_source");

        client.execute(pluginCode, "CREATE TABLE IF NOT EXISTS flink21_source (" +
                "       id                  BIGINT PRIMARY KEY NOT ENFORCED ," +
                "       name                STRING                          ," +
                "       birthday            TIMESTAMP(3)                    " +
                ") WITH ( " +
                "       'connector' = 'datagen' " +
                ")");
    }

    private static void createTableOfFlink21Sink(String pluginCode) {
        client.execute(pluginCode, "DROP TABLE IF EXISTS flink21_sink");

        client.execute(pluginCode, "CREATE TABLE IF NOT EXISTS flink21_sink (" +
                "       id                  BIGINT PRIMARY KEY NOT ENFORCED ," +
                "       name                STRING                          ," +
                "       `partition`         VARCHAR(20)                     " +
                ") WITH ( " +
                "       'connector' = 'blackhole' " +
                ")");
    }

    private static void createTableOfFlink21UsersSource(String pluginCode) {
        client.execute(pluginCode, "DROP TABLE IF EXISTS flink21_users_source");

        client.execute(pluginCode, "CREATE TABLE IF NOT EXISTS flink21_users_source (" +
                "       id                  BIGINT                          ," +
                "       name                STRING                          ," +
                "       birthday            TIMESTAMP(3)                    ," +
                "       score               DECIMAL(10, 2)                  ," +
                "       proc_time AS PROCTIME()                              " +
                ") WITH ( " +
                "       'connector' = 'datagen' " +
                ")");
    }

    private static void createTableOfFlink21CompanySource(String pluginCode) {
        client.execute(pluginCode, "DROP TABLE IF EXISTS flink21_company_source");

        client.execute(pluginCode, "CREATE TABLE IF NOT EXISTS flink21_company_source (" +
                "       user_id             BIGINT                          ," +
                "       company_name        STRING                           " +
                ") WITH ( " +
                "       'connector' = 'datagen' " +
                ")");
    }

    private static void createTableOfFlink21UsersSink(String pluginCode) {
        client.execute(pluginCode, "DROP TABLE IF EXISTS flink21_users_sink");

        client.execute(pluginCode, "CREATE TABLE IF NOT EXISTS flink21_users_sink (" +
                "       id                  BIGINT                          ," +
                "       name                STRING                          ," +
                "       company_name        STRING                          ," +
                "       birthday            TIMESTAMP(3)                    ," +
                "       score_text          STRING                          ," +
                "       partition_day       STRING                           " +
                ") WITH ( " +
                "       'connector' = 'blackhole' " +
                ")");
    }

    private static void createTableOfFlink21StatsSink(String pluginCode) {
        client.execute(pluginCode, "DROP TABLE IF EXISTS flink21_stats_sink");

        client.execute(pluginCode, "CREATE TABLE IF NOT EXISTS flink21_stats_sink (" +
                "       id                  BIGINT                          ," +
                "       name_upper          STRING                          ," +
                "       score_bucket        BIGINT                          ," +
                "       birthday_day        STRING                           " +
                ") WITH ( " +
                "       'connector' = 'blackhole' " +
                ")");
    }

    @Test
    public void testConvertProperties() {
        Map<String, String> propertiesMap = ImmutableMap.of(
                "type", "jdbc",
                "default-database", "lineage_catalog",
                "username", "root",
                "password", "root@123456",
                "base-url", "jdbc:mysql://127.0.0.1:3306");
        String properties = propertiesMap.entrySet()
                .stream()
                .map(entry -> String.format("'%s'='%s'", entry.getKey(), entry.getValue()))
                .collect(Collectors.joining(","));

        assertThat(properties, is(
                "'type'='jdbc','default-database'='lineage_catalog','username'='root','password'='root@123456','base-url'='jdbc:mysql://127.0.0.1:3306'"));
    }

}
