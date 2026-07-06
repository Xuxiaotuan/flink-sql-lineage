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

import org.apache.flink.core.execution.JobStatusChangedEvent;
import org.apache.flink.core.execution.JobStatusChangedListener;
import org.apache.flink.core.execution.JobStatusChangedListenerFactory;
import org.apache.flink.streaming.api.lineage.DatasetSchemaFacet;
import org.apache.flink.streaming.api.lineage.DatasetSchemaField;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;
import org.apache.flink.streaming.api.lineage.LineageEdge;
import org.apache.flink.streaming.api.lineage.LineageGraph;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;
import org.apache.flink.streaming.runtime.execution.JobCreatedEvent;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedSchema;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

public class Flink21LineageListenerFactory implements JobStatusChangedListenerFactory {

    private static final String DEFAULT_EVENT_FILE = "/tmp/flink21-lineage-events.jsonl";

    @Override
    public JobStatusChangedListener createListener(Context context) {
        String eventFile = context.getConfiguration()
                .getString("lineage.experiment.event-file", DEFAULT_EVENT_FILE);
        CollectorConfig collectorConfig = CollectorConfig.from(context);
        return new Flink21LineageListener(Paths.get(eventFile), collectorConfig, context.getIOExecutor());
    }

    private static final class CollectorConfig {

        private final String url;
        private final String username;
        private final String password;
        private final String sql;
        private final Long catalogId;
        private final String database;
        private final String taskName;

        private CollectorConfig(String url, String username, String password, String sql, Long catalogId,
                String database, String taskName) {
            this.url = url;
            this.username = username;
            this.password = password;
            this.sql = sql;
            this.catalogId = catalogId;
            this.database = database;
            this.taskName = taskName;
        }

        private static CollectorConfig from(Context context) {
            String sql = context.getConfiguration().getString("lineage.collector.sql", "");
            String sqlFile = context.getConfiguration().getString("lineage.collector.sql-file", "");
            if (sql.trim().isEmpty() && !sqlFile.trim().isEmpty()) {
                sql = readSqlFile(sqlFile);
            }
            return new CollectorConfig(
                    context.getConfiguration().getString("lineage.collector.url", ""),
                    context.getConfiguration().getString("lineage.collector.username", ""),
                    context.getConfiguration().getString("lineage.collector.password", ""),
                    sql,
                    parseLong(context.getConfiguration().getString("lineage.collector.catalog-id", "1")),
                    context.getConfiguration().getString("lineage.collector.database", "default"),
                    context.getConfiguration().getString("lineage.collector.task-name", ""));
        }

        private static String readSqlFile(String sqlFile) {
            try {
                return new String(Files.readAllBytes(Paths.get(sqlFile)), StandardCharsets.UTF_8);
            } catch (IOException e) {
                System.err.println("failed to read lineage collector sql file " + sqlFile + ": " + e.getMessage());
                return "";
            }
        }

        private static Long parseLong(String value) {
            try {
                return Long.valueOf(value);
            } catch (NumberFormatException e) {
                return 1L;
            }
        }

        private boolean enabled() {
            return url != null && !url.trim().isEmpty();
        }
    }

    private static final class Flink21LineageListener implements JobStatusChangedListener {

        private final Path eventFile;
        private final CollectorConfig collectorConfig;
        private final Executor ioExecutor;

        private Flink21LineageListener(Path eventFile, CollectorConfig collectorConfig, Executor ioExecutor) {
            this.eventFile = eventFile;
            this.collectorConfig = collectorConfig;
            this.ioExecutor = ioExecutor;
        }

        @Override
        public void onEvent(JobStatusChangedEvent event) {
            if (!(event instanceof JobCreatedEvent)) {
                return;
            }
            JobCreatedEvent createdEvent = (JobCreatedEvent) event;
            String payload = toJson(createdEvent) + System.lineSeparator();
            ioExecutor.execute(() -> {
                append(payload);
                postToCollector(payload);
            });
        }

        private void append(String payload) {
            try {
                Path parent = eventFile.getParent();
                if (parent != null && !Files.exists(parent)) {
                    Files.createDirectories(parent);
                }
                Files.write(eventFile, payload.getBytes(StandardCharsets.UTF_8),
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                System.out.println("flink21 lineage event written to " + eventFile);
            } catch (IOException e) {
                System.err.println("failed to write flink21 lineage event to " + eventFile + ": " + e.getMessage());
            }
        }

        private void postToCollector(String payload) {
            if (!collectorConfig.enabled()) {
                return;
            }
            HttpURLConnection connection = null;
            try {
                URL url = new URL(collectorConfig.url);
                connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("POST");
                connection.setConnectTimeout(10000);
                connection.setReadTimeout(60000);
                connection.setDoOutput(true);
                connection.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
                if (!collectorConfig.username.trim().isEmpty()) {
                    connection.setRequestProperty("Authorization", basicAuth());
                }
                try (OutputStream outputStream = connection.getOutputStream()) {
                    outputStream.write(payload.trim().getBytes(StandardCharsets.UTF_8));
                }
                int status = connection.getResponseCode();
                if (status < 200 || status >= 300) {
                    System.err.println("failed to post flink21 lineage event, status=" + status + ", url="
                            + collectorConfig.url);
                } else {
                    System.out.println("flink21 lineage event posted to " + collectorConfig.url);
                }
            } catch (IOException e) {
                System.err.println("failed to post flink21 lineage event to " + collectorConfig.url + ": "
                        + e.getMessage());
            } finally {
                if (connection != null) {
                    connection.disconnect();
                }
            }
        }

        private String basicAuth() {
            String token = collectorConfig.username + ":" + collectorConfig.password;
            return "Basic " + Base64.getEncoder().encodeToString(token.getBytes(StandardCharsets.UTF_8));
        }

        private String toJson(JobCreatedEvent event) {
            LineageGraph graph = event.lineageGraph();
            StringBuilder builder = new StringBuilder();
            builder.append('{');
            appendField(builder, "eventTime", Instant.now().toString()).append(',');
            appendField(builder, "eventType", "JobCreatedEvent").append(',');
            appendField(builder, "jobId", event.jobId().toString()).append(',');
            appendField(builder, "jobName", event.jobName()).append(',');
            appendField(builder, "executionMode", String.valueOf(event.executionMode())).append(',');
            appendField(builder, "sql", collectorConfig.sql).append(',');
            appendField(builder, "database", collectorConfig.database).append(',');
            appendField(builder, "taskName", taskName(event)).append(',');
            appendNumberField(builder, "catalogId", collectorConfig.catalogId).append(',');
            builder.append("\"inputs\":").append(sourceDatasetsToJson(graph.sources())).append(',');
            builder.append("\"outputs\":").append(vertexDatasetsToJson(graph.sinks())).append(',');
            builder.append("\"sources\":").append(sourceVerticesToJson(graph.sources())).append(',');
            builder.append("\"sinks\":").append(verticesToJson(graph.sinks())).append(',');
            builder.append("\"relations\":").append(relationsToJson(graph.relations()));
            builder.append('}');
            return builder.toString();
        }

        private String taskName(JobCreatedEvent event) {
            if (collectorConfig.taskName != null && !collectorConfig.taskName.trim().isEmpty()) {
                return collectorConfig.taskName;
            }
            return "监听器采集-" + event.jobName() + "-" + event.jobId();
        }

        private String sourceDatasetsToJson(List<SourceLineageVertex> vertices) {
            List<String> items = new ArrayList<>();
            for (SourceLineageVertex vertex : vertices) {
                for (LineageDataset dataset : vertex.datasets()) {
                    items.add(datasetToJson(dataset));
                }
            }
            return array(items);
        }

        private String vertexDatasetsToJson(List<LineageVertex> vertices) {
            List<String> items = new ArrayList<>();
            for (LineageVertex vertex : vertices) {
                for (LineageDataset dataset : vertex.datasets()) {
                    items.add(datasetToJson(dataset));
                }
            }
            return array(items);
        }

        private String sourceVerticesToJson(List<SourceLineageVertex> vertices) {
            List<String> items = new ArrayList<>();
            for (SourceLineageVertex vertex : vertices) {
                StringBuilder builder = new StringBuilder(vertexToJson(vertex));
                builder.deleteCharAt(builder.length() - 1);
                builder.append(",\"boundedness\":");
                appendString(builder, String.valueOf(vertex.boundedness()));
                builder.append('}');
                items.add(builder.toString());
            }
            return array(items);
        }

        private String verticesToJson(List<LineageVertex> vertices) {
            List<String> items = new ArrayList<>();
            for (LineageVertex vertex : vertices) {
                items.add(vertexToJson(vertex));
            }
            return array(items);
        }

        private String relationsToJson(List<LineageEdge> relations) {
            List<String> items = new ArrayList<>();
            for (LineageEdge relation : relations) {
                StringBuilder builder = new StringBuilder();
                builder.append('{');
                builder.append("\"source\":").append(vertexToJson(relation.source())).append(',');
                builder.append("\"sink\":").append(vertexToJson(relation.sink()));
                builder.append('}');
                items.add(builder.toString());
            }
            return array(items);
        }

        private String vertexToJson(LineageVertex vertex) {
            StringBuilder builder = new StringBuilder();
            builder.append('{');
            builder.append("\"datasets\":").append(datasetsToJson(vertex.datasets()));
            builder.append('}');
            return builder.toString();
        }

        private String datasetsToJson(List<LineageDataset> datasets) {
            List<String> items = new ArrayList<>();
            for (LineageDataset dataset : datasets) {
                items.add(datasetToJson(dataset));
            }
            return array(items);
        }

        private String datasetToJson(LineageDataset dataset) {
            CatalogBaseTable table = extractTable(dataset);
            StringBuilder builder = new StringBuilder();
            builder.append('{');
            appendField(builder, "namespace", dataset.namespace()).append(',');
            appendField(builder, "name", dataset.name()).append(',');
            appendField(builder, "datasetClass", dataset.getClass().getName()).append(',');
            builder.append("\"facetKeys\":").append(stringArray(dataset.facets().keySet())).append(',');
            builder.append("\"table\":").append(tableToJson(table)).append(',');
            builder.append("\"schema\":").append(schemaToJson(dataset, table));
            builder.append('}');
            return builder.toString();
        }

        private String schemaToJson(LineageDataset dataset, CatalogBaseTable table) {
            List<String> schemaFromFacets = schemaFromFacets(dataset.facets());
            if (!schemaFromFacets.isEmpty()) {
                return array(schemaFromFacets);
            }
            return array(schemaFromTable(table));
        }

        private List<String> schemaFromFacets(Map<String, LineageDatasetFacet> facets) {
            List<String> fields = new ArrayList<>();
            for (LineageDatasetFacet facet : facets.values()) {
                if (!(facet instanceof DatasetSchemaFacet)) {
                    continue;
                }
                DatasetSchemaFacet schemaFacet = (DatasetSchemaFacet) facet;
                for (DatasetSchemaField<?> field : schemaFacet.fields().values()) {
                    StringBuilder builder = new StringBuilder();
                    builder.append('{');
                    appendField(builder, "name", field.name()).append(',');
                    appendField(builder, "type", String.valueOf(field.type()));
                    builder.append('}');
                    fields.add(builder.toString());
                }
            }
            return fields;
        }

        private List<String> schemaFromTable(CatalogBaseTable table) {
            List<String> fields = new ArrayList<>();
            if (table instanceof ResolvedCatalogBaseTable) {
                ResolvedSchema schema = ((ResolvedCatalogBaseTable<?>) table).getResolvedSchema();
                for (Column column : schema.getColumns()) {
                    StringBuilder builder = new StringBuilder();
                    builder.append('{');
                    appendField(builder, "name", column.getName()).append(',');
                    appendField(builder, "type", column.getDataType().toString()).append(',');
                    appendField(builder, "kind", columnKind(column)).append(',');
                    appendField(builder, "summary", column.asSummaryString());
                    builder.append('}');
                    fields.add(builder.toString());
                }
                return fields;
            }
            if (table == null) {
                return fields;
            }
            Schema schema = table.getUnresolvedSchema();
            for (Schema.UnresolvedColumn column : schema.getColumns()) {
                StringBuilder builder = new StringBuilder();
                builder.append('{');
                appendField(builder, "name", column.getName()).append(',');
                appendField(builder, "type", unresolvedType(column)).append(',');
                appendField(builder, "kind", unresolvedColumnKind(column)).append(',');
                appendField(builder, "summary", column.toString());
                builder.append('}');
                fields.add(builder.toString());
            }
            return fields;
        }

        private String tableToJson(CatalogBaseTable table) {
            if (table == null) {
                return "null";
            }
            StringBuilder builder = new StringBuilder();
            builder.append('{');
            appendField(builder, "tableClass", table.getClass().getName()).append(',');
            appendField(builder, "tableKind", String.valueOf(table.getTableKind())).append(',');
            appendField(builder, "connector", table.getOptions().get("connector")).append(',');
            builder.append("\"options\":").append(stringMap(table.getOptions()));
            builder.append('}');
            return builder.toString();
        }

        private CatalogBaseTable extractTable(LineageDataset dataset) {
            if (!dataset.getClass().getName()
                    .startsWith("org.apache.flink.table.planner.lineage.TableLineageDataset")) {
                return null;
            }
            try {
                Object table = dataset.getClass().getMethod("table").invoke(dataset);
                if (table instanceof CatalogBaseTable) {
                    return (CatalogBaseTable) table;
                }
            } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                System.err.println("failed to extract table from lineage dataset " + dataset.name() + ": "
                        + e.getMessage());
            }
            return null;
        }

        private String columnKind(Column column) {
            if (column.isPhysical()) {
                return "PHYSICAL";
            }
            if (column.isPersisted()) {
                return "PERSISTED";
            }
            return "VIRTUAL_OR_COMPUTED";
        }

        private String unresolvedType(Schema.UnresolvedColumn column) {
            if (column instanceof Schema.UnresolvedPhysicalColumn) {
                return ((Schema.UnresolvedPhysicalColumn) column).getDataType().toString();
            }
            if (column instanceof Schema.UnresolvedMetadataColumn) {
                return ((Schema.UnresolvedMetadataColumn) column).getDataType().toString();
            }
            if (column instanceof Schema.UnresolvedComputedColumn) {
                return ((Schema.UnresolvedComputedColumn) column).getExpression().asSummaryString();
            }
            return "UNKNOWN";
        }

        private String unresolvedColumnKind(Schema.UnresolvedColumn column) {
            if (column instanceof Schema.UnresolvedPhysicalColumn) {
                return "PHYSICAL";
            }
            if (column instanceof Schema.UnresolvedMetadataColumn) {
                return "METADATA";
            }
            if (column instanceof Schema.UnresolvedComputedColumn) {
                return "COMPUTED";
            }
            return "UNKNOWN";
        }

        private String array(List<String> items) {
            StringBuilder builder = new StringBuilder();
            builder.append('[');
            for (int i = 0; i < items.size(); i++) {
                if (i > 0) {
                    builder.append(',');
                }
                builder.append(items.get(i));
            }
            builder.append(']');
            return builder.toString();
        }

        private String stringArray(Iterable<String> values) {
            List<String> items = new ArrayList<>();
            for (String value : values) {
                StringBuilder builder = new StringBuilder();
                appendString(builder, value);
                items.add(builder.toString());
            }
            return array(items);
        }

        private String stringMap(Map<String, String> values) {
            List<String> items = new ArrayList<>();
            for (Map.Entry<String, String> entry : values.entrySet()) {
                StringBuilder builder = new StringBuilder();
                appendField(builder, entry.getKey(), entry.getValue());
                items.add(builder.toString());
            }
            return "{" + String.join(",", items) + "}";
        }

        private StringBuilder appendField(StringBuilder builder, String key, String value) {
            appendString(builder, key);
            builder.append(':');
            appendString(builder, value);
            return builder;
        }

        private StringBuilder appendNumberField(StringBuilder builder, String key, Long value) {
            appendString(builder, key);
            builder.append(':');
            builder.append(value == null ? "null" : value);
            return builder;
        }

        private void appendString(StringBuilder builder, String value) {
            if (value == null) {
                builder.append("null");
                return;
            }
            builder.append('"');
            for (int i = 0; i < value.length(); i++) {
                char c = value.charAt(i);
                switch (c) {
                    case '"':
                        builder.append("\\\"");
                        break;
                    case '\\':
                        builder.append("\\\\");
                        break;
                    case '\b':
                        builder.append("\\b");
                        break;
                    case '\f':
                        builder.append("\\f");
                        break;
                    case '\n':
                        builder.append("\\n");
                        break;
                    case '\r':
                        builder.append("\\r");
                        break;
                    case '\t':
                        builder.append("\\t");
                        break;
                    default:
                        if (c < 0x20) {
                            builder.append(String.format("\\u%04x", (int) c));
                        } else {
                            builder.append(c);
                        }
                }
            }
            builder.append('"');
        }
    }
}
