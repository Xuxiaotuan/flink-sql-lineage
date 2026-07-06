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

package com.hw.lineage.flink.listener;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink21LineageDemoJob {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql(
                "CREATE TABLE listener_ods_user ("
                        + " id BIGINT,"
                        + " name STRING,"
                        + " birthday TIMESTAMP(3),"
                        + " score DECIMAL(10, 2)"
                        + ") WITH ("
                        + " 'connector' = 'datagen',"
                        + " 'rows-per-second' = '1'"
                        + ")");

        tableEnv.executeSql(
                "CREATE TABLE listener_dwd_user ("
                        + " id BIGINT,"
                        + " name_upper STRING,"
                        + " birthday_day STRING,"
                        + " score_text STRING"
                        + ") WITH ("
                        + " 'connector' = 'blackhole'"
                        + ")");

        tableEnv.executeSql(
                "INSERT INTO listener_dwd_user "
                        + "SELECT id, UPPER(name), DATE_FORMAT(birthday, 'yyyyMMdd'), CAST(score AS STRING) "
                        + "FROM listener_ods_user");
    }
}
