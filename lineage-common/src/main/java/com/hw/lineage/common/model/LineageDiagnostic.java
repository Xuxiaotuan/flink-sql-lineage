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

package com.hw.lineage.common.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.List;

/**
 * @description: Diagnostic report for Flink SQL lineage parsing.
 * @author: HamaWhite
 */
@Data
@NoArgsConstructor
@Accessors(chain = true)
public class LineageDiagnostic {

    private String sql;

    private Long sqlId;

    private String sqlType;

    private Long startLineNumber;

    private boolean success;

    private String operationType;

    private String sinkTable;

    private String relNodeType;

    private List<String> targetColumns = new ArrayList<>();

    private List<LineageDiagnosticStep> steps = new ArrayList<>();

    private String failedStage;

    private String errorClass;

    private String errorMessage;

    public LineageDiagnostic addStep(String stage, String status, String message) {
        steps.add(new LineageDiagnosticStep(stage, status, message));
        return this;
    }
}
