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

package com.hw.lineage.server.domain.graph;

import com.hw.lineage.server.domain.graph.table.TableGraph;
import com.hw.lineage.server.domain.graph.table.TableNode;

import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * @description: GraphHelperTest
 * @author: HamaWhite
 */
public class GraphHelperTest {

    @Test
    public void testFilterMissingTableNode() {
        TableGraph graph = new TableGraph();
        TableNode source = new TableNode(1, "catalog.default.source_table");
        graph.addNode(source.getNodeName(), source);

        TableGraph filteredGraph = new GraphHelper().filter(graph, "catalog.default.missing_sink");

        assertThat(filteredGraph.queryNodeSet().isEmpty()).isTrue();
        assertThat(filteredGraph.getEdgeSet().isEmpty()).isTrue();
    }
}
