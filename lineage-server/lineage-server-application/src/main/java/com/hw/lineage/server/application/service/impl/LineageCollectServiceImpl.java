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

package com.hw.lineage.server.application.service.impl;

import com.hw.lineage.common.util.Base64Utils;
import com.hw.lineage.server.application.command.catalog.CreateTableCmd;
import com.hw.lineage.server.application.command.lineage.FlinkLineageCollectCmd;
import com.hw.lineage.server.application.command.task.CreateTaskCmd;
import com.hw.lineage.server.application.command.task.UpdateTaskCmd;
import com.hw.lineage.server.application.dto.FlinkLineageCollectDTO;
import com.hw.lineage.server.application.dto.TaskDTO;
import com.hw.lineage.server.application.service.CatalogService;
import com.hw.lineage.server.application.service.LineageCollectService;
import com.hw.lineage.server.application.service.TaskService;
import com.hw.lineage.server.application.service.UserService;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

@Service("lineageCollectService")
public class LineageCollectServiceImpl implements LineageCollectService {

    private static final long DEFAULT_CATALOG_ID = 1L;

    private static final String DEFAULT_DATABASE = "default";

    @Resource
    private CatalogService catalogService;

    @Resource
    private TaskService taskService;

    @Resource
    private UserService userService;

    @Override
    public FlinkLineageCollectDTO collectFlink21Lineage(FlinkLineageCollectCmd command) {
        validate(command);

        Long catalogId = command.getCatalogId() == null ? DEFAULT_CATALOG_ID : command.getCatalogId();
        String database = StringUtils.defaultIfBlank(command.getDatabase(), DEFAULT_DATABASE);
        Long userId = userService.getCurrentUserId();

        for (FlinkLineageCollectCmd.Dataset dataset : datasets(command)) {
            CreateTableCmd createTableCmd = new CreateTableCmd();
            createTableCmd.setCatalogId(catalogId);
            createTableCmd.setDatabase(database);
            createTableCmd.setDdl(Base64Utils.encode(toCreateTableDdl(dataset)));
            createTableCmd.setUserId(userId);
            catalogService.createTable(createTableCmd);
        }

        String taskName = StringUtils.defaultIfBlank(command.getTaskName(), defaultTaskName(command));
        CreateTaskCmd createTaskCmd = new CreateTaskCmd();
        createTaskCmd.setTaskName(taskName);
        createTaskCmd.setDescr("Flink 2.1 listener collected jobId=" + command.getJobId());
        createTaskCmd.setCatalogId(catalogId);
        createTaskCmd.setDatabase(database);
        createTaskCmd.setUserId(userId);
        Long taskId = taskService.createTask(createTaskCmd);

        UpdateTaskCmd updateTaskCmd = new UpdateTaskCmd();
        updateTaskCmd.setTaskId(taskId);
        updateTaskCmd.setTaskName(taskName);
        updateTaskCmd.setDescr(createTaskCmd.getDescr());
        updateTaskCmd.setCatalogId(catalogId);
        updateTaskCmd.setSource(Base64Utils.encode(command.getSql()));
        updateTaskCmd.setUserId(userId);
        taskService.updateTask(updateTaskCmd);

        TaskDTO taskDTO = taskService.analyzeTaskLineage(taskId);
        FlinkLineageCollectDTO result = new FlinkLineageCollectDTO();
        result.setTaskId(taskDTO.getTaskId());
        result.setTaskName(taskDTO.getTaskName());
        result.setTaskStatus(taskDTO.getTaskStatus());
        result.setTaskLog(taskDTO.getTaskLog());
        return result;
    }

    private void validate(FlinkLineageCollectCmd command) {
        if (StringUtils.isBlank(command.getSql())) {
            throw new IllegalArgumentException(
                    "Flink listener event must include sql for column-level lineage replay.");
        }
        if (datasets(command).isEmpty()) {
            throw new IllegalArgumentException("Flink listener event must include input or output datasets.");
        }
    }

    private List<FlinkLineageCollectCmd.Dataset> datasets(FlinkLineageCollectCmd command) {
        List<FlinkLineageCollectCmd.Dataset> datasets = new ArrayList<>();
        datasets.addAll(command.getInputs() == null ? Collections.emptyList() : command.getInputs());
        datasets.addAll(command.getOutputs() == null ? Collections.emptyList() : command.getOutputs());
        return datasets;
    }

    private String toCreateTableDdl(FlinkLineageCollectCmd.Dataset dataset) {
        if (dataset.getSchema() == null || dataset.getSchema().isEmpty()) {
            throw new IllegalArgumentException("Dataset has no schema: " + dataset.getName());
        }

        StringJoiner columns = new StringJoiner(",\n");
        for (FlinkLineageCollectCmd.Column column : dataset.getSchema()) {
            columns.add("    `" + escapeIdentifier(column.getName()) + "` " + normalizeType(column.getType()));
        }

        StringJoiner options = new StringJoiner(",\n");
        Map<String, String> optionMap = dataset.getTable() == null ? Collections.emptyMap() : dataset.getTable()
                .getOptions();
        if (optionMap == null || optionMap.isEmpty()) {
            options.add("    'connector' = 'blackhole'");
        } else {
            optionMap.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> options.add(
                            "    '" + escapeSql(entry.getKey()) + "' = '" + escapeSql(entry.getValue()) + "'"));
        }

        return "CREATE TABLE IF NOT EXISTS `" + escapeIdentifier(tableName(dataset.getName())) + "` (\n" + columns
                + "\n) WITH (\n" + options + "\n)";
    }

    private String tableName(String identifier) {
        if (StringUtils.isBlank(identifier)) {
            throw new IllegalArgumentException("Dataset name is blank.");
        }
        String[] items = identifier.split("\\.");
        return items[items.length - 1].replace("`", "");
    }

    private String normalizeType(String type) {
        return StringUtils.defaultIfBlank(type, "STRING").replace(" NOT NULL", "");
    }

    private String escapeIdentifier(String value) {
        return StringUtils.defaultString(value).replace("`", "``");
    }

    private String escapeSql(String value) {
        return StringUtils.defaultString(value).replace("'", "''");
    }

    private String defaultTaskName(FlinkLineageCollectCmd command) {
        String name = StringUtils.defaultIfBlank(command.getJobName(), "flink-listener");
        return "监听器采集-" + name + "-" + System.currentTimeMillis();
    }
}
