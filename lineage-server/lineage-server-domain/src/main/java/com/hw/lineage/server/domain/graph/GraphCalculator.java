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

import com.hw.lineage.server.domain.graph.basic.Edge;
import com.hw.lineage.server.domain.graph.basic.Graph;
import com.hw.lineage.server.domain.graph.basic.Node;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @description: GraphCalculator
 * @author: HamaWhite
 */
public class GraphCalculator<N extends Node, E extends Edge<N>> {

    private Graph<N, E> graph;

    private Map<Integer, N> nodeIdMap;

    private final Map<Integer, Set<Integer>> childrenMap;

    private final Set<Integer> visitIdSet;

    public GraphCalculator(Graph<N, E> graph) {
        this.graph = graph;
        this.childrenMap = new HashMap<>();
        this.visitIdSet = new HashSet<>();
        this.nodeIdMap = graph.queryNodeIdMap();
    }

    /**
     * Calculate the count of all downstream nodes for each node in the graph
     */
    public void calculateChildrenCnt() {
        Set<N> startSet = graph.queryNodeSet()
                .stream()
                .filter(node -> node.getParentIdSet().isEmpty())
                .collect(Collectors.toSet());

        startSet.forEach(this::searchChildrenSet);
        graph.queryNodeSet().forEach(node -> {
            if (!childrenMap.containsKey(node.getNodeId())) {
                searchChildrenSet(node);
            }
            node.setChildrenCnt(childrenMap.getOrDefault(node.getNodeId(), Collections.emptySet()).size());
        });
    }

    private void searchChildrenSet(N node) {
        if (node == null) {
            return;
        }
        Integer nodeId = node.getNodeId();
        if (childrenMap.containsKey(nodeId)) {
            return;
        }
        visitIdSet.add(nodeId);
        if (node.getChildIdSet().isEmpty()) {
            childrenMap.put(nodeId, Collections.emptySet());
            return;
        }
        Set<Integer> childrenSet = new HashSet<>();
        for (Integer childId : node.getChildIdSet()) {
            N childNode = nodeIdMap.get(childId);
            if (childNode == null) {
                continue;
            }
            if (!visitIdSet.contains(childId)) {
                searchChildrenSet(childNode);
            }
            childrenSet.add(childId);
            childrenSet.addAll(childrenMap.getOrDefault(childId, Collections.emptySet()));
        }
        childrenMap.put(nodeId, childrenSet);
    }

    public Map<Integer, Set<Integer>> getChildrenMap() {
        return childrenMap;
    }
}
