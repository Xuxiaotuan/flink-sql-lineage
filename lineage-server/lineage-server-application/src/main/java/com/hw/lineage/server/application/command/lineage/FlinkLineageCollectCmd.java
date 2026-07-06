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

package com.hw.lineage.server.application.command.lineage;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * Flink 2.1 listener event collected from JobCreatedEvent#lineageGraph().
 */
@Data
public class FlinkLineageCollectCmd {

    private String eventTime;

    private String eventType;

    private String jobId;

    private String jobName;

    private String executionMode;

    private Long catalogId;

    private String database;

    private String taskName;

    private String sql;

    private List<Dataset> inputs;

    private List<Dataset> outputs;

    @Data
    public static class Dataset {

        private String namespace;

        private String name;

        private String datasetClass;

        private Table table;

        private List<Column> schema;
    }

    @Data
    public static class Table {

        private String tableClass;

        private String tableKind;

        private String connector;

        private Map<String, String> options;
    }

    @Data
    public static class Column {

        private String name;

        private String type;

        private String kind;

        private String summary;
    }
}
