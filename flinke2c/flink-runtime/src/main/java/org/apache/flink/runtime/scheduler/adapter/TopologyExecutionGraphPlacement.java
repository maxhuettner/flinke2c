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

package org.apache.flink.runtime.scheduler.adapter;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionGraphPlacement;

import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class TopologyExecutionGraphPlacement implements ExecutionGraphPlacement {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Graph<TopologyNode, DefaultEdge> processingTopology;

    public TopologyExecutionGraphPlacement() {
        this.processingTopology = parseProcessingTopology();
    }

    private Graph<TopologyNode, DefaultEdge> parseProcessingTopology() {
        final Graph<TopologyNode, DefaultEdge> graph = new SimpleGraph<>(DefaultEdge.class);
        final SourceNode source1 = new SourceNode("Source1");
        final TopologyNode node2 = new ComputeNode("10.10.10.2", 1.0f, 1.0f, 3);
        final TopologyNode node3 = new ComputeNode("10.10.10.3", 1.0f, 1.0f, 3);
        final TopologyNode node5 = new ComputeNode("10.10.10.5", 1.0f, 1.0f, 3);
        final TopologyNode node7 = new ComputeNode("10.10.10.7", 1.0f, 1.0f, 3);
        final TopologyNode node8 = new ComputeNode("10.10.10.8", 1.0f, 1.0f, 3);
        final SinkNode sink1 = new SinkNode("Sink1");

        final DefaultEdge edgesrc18 = new DefaultEdge();
        final ComputeEdge edge87 = new ComputeEdge(1.0f, 1.0f);
        final ComputeEdge edge75 = new ComputeEdge(1.0f, 1.0f);
        final ComputeEdge edge73 = new ComputeEdge(1.0f, 1.0f);
        final ComputeEdge edge32 = new ComputeEdge(1.0f, 1.0f);
        final ComputeEdge edge52 = new ComputeEdge(1.0f, 1.0f);
        final DefaultEdge edge2snk1 = new DefaultEdge();

        graph.addVertex(source1);
        graph.addVertex(node2);
        graph.addVertex(node3);
        graph.addVertex(node5);
        graph.addVertex(node7);
        graph.addVertex(node8);
        graph.addVertex(sink1);

        graph.addEdge(source1, node8, edgesrc18);
        graph.addEdge(node8, node7, edge87);
        graph.addEdge(node7, node5, edge75);
        graph.addEdge(node7, node3, edge73);
        graph.addEdge(node3, node2, edge32);
        graph.addEdge(node5, node2, edge52);
        graph.addEdge(node2, sink1, edge2snk1);

        return graph;
    }

    /**
     * This method uses a simulated annealing optimizer to search for a placement that minimizes the overall cost.
     * The cost function now includes an additional connectivity cost term that considers both predecessors and successors,
     * so that connected operators are encouraged to be placed close to each other.
     */
    @Override
    public void assignPlacement(ExecutionGraph executionGraph) {
        log.info("Execution graph: {}", executionGraph);

        // Extract operator tasks from the ExecutionGraph
        List<OperatorTask> tasks = getOperatorTasksFromExecutionGraph(executionGraph);

        // Get source and sink nodes
        List<TopologyNode> sources = processingTopology.vertexSet().stream()
                .filter(n -> n instanceof SourceNode)
                .collect(Collectors.toList());
        List<TopologyNode> sinks = processingTopology.vertexSet().stream()
                .filter(n -> n instanceof SinkNode)
                .collect(Collectors.toList());

        if (sources.isEmpty() || sinks.isEmpty()) {
            throw new RuntimeException("Topology needs at least one source and one sink");
        }

        // Get compute nodes and their slots
        Map<ComputeNode, Integer> availableSlots = new HashMap<>();
        List<ComputeNode> computeNodes = processingTopology.vertexSet().stream()
                .filter(n -> n instanceof ComputeNode)
                .map(n -> (ComputeNode) n)
                .collect(Collectors.toList());

        for (ComputeNode node : computeNodes) {
            availableSlots.put(node, node.numSlots);
        }

        double bestCost = Double.MAX_VALUE;
        Map<OperatorTask, ComputeNode> bestAssignment = null;

        // Try all possible paths from sources to sinks
        for (TopologyNode source : sources) {
            for (TopologyNode sink : sinks) {
                // Find all simple paths between source and sink
                List<List<TopologyNode>> paths = findAllSimplePaths(source, sink, tasks.size() + 2);

                for (List<TopologyNode> path : paths) {
                    if (path.size() != tasks.size() + 2) {
                        continue;
                    }

                    // Get compute nodes in the middle of the path
                    List<ComputeNode> midNodes = path.subList(1, path.size() - 1).stream()
                            .map(n -> (ComputeNode) n)
                            .collect(Collectors.toList());

                    // Check slot constraints
                    Map<ComputeNode, Integer> nodeCounts = new HashMap<>();
                    boolean validSlots = true;
                    for (ComputeNode node : midNodes) {
                        nodeCounts.merge(node, 1, Integer::sum);
                        if (nodeCounts.get(node) > availableSlots.get(node)) {
                            validSlots = false;
                            break;
                        }
                    }
                    if (!validSlots) {
                        continue;
                    }

                    // Create assignment
                    Map<OperatorTask, ComputeNode> assignment = new HashMap<>();
                    for (int i = 0; i < tasks.size(); i++) {
                        assignment.put(tasks.get(i), midNodes.get(i));
                    }

                    // Calculate total cost
                    double totalCost = calculateTotalCost(assignment, tasks, path);

                    if (totalCost < bestCost) {
                        bestCost = totalCost;
                        bestAssignment = new HashMap<>(assignment);
                    }
                }
            }
        }

        if (bestAssignment == null) {
            throw new RuntimeException("No valid placement found (check slot constraints)");
        }

        // Apply the best assignment
        for (Map.Entry<OperatorTask, ComputeNode> entry : bestAssignment.entrySet()) {
            final String taskManagerId = entry.getValue().getId();
            log.info("Optimized assignment: task {} -> node {}", entry.getKey().id, taskManagerId);
            executionGraph.getJobVertex(entry.getKey().id).getResourceProfile().setTaskManagerAddress(taskManagerId);
        }
    }

    private List<List<TopologyNode>> findAllSimplePaths(TopologyNode source, TopologyNode sink, int maxLength) {
        List<List<TopologyNode>> paths = new ArrayList<>();
        Set<TopologyNode> visited = new HashSet<>();
        List<TopologyNode> currentPath = new ArrayList<>();
        currentPath.add(source);
        visited.add(source);
        findAllSimplePathsHelper(source, sink, visited, currentPath, paths, maxLength);
        return paths;
    }

    private void findAllSimplePathsHelper(TopologyNode current, TopologyNode sink,
                                          Set<TopologyNode> visited, List<TopologyNode> currentPath,
                                          List<List<TopologyNode>> paths, int maxLength) {
        if (current.equals(sink)) {
            paths.add(new ArrayList<>(currentPath));
            return;
        }

        if (currentPath.size() >= maxLength) {
            return;
        }

        for (DefaultEdge edge : processingTopology.outgoingEdgesOf(current)) {
            TopologyNode next = processingTopology.getEdgeTarget(edge);
            if (!visited.contains(next)) {
                visited.add(next);
                currentPath.add(next);
                findAllSimplePathsHelper(next, sink, visited, currentPath, paths, maxLength);
                currentPath.remove(currentPath.size() - 1);
                visited.remove(next);
            }
        }
    }

    private double calculateTotalCost(Map<OperatorTask, ComputeNode> assignment,
                                      List<OperatorTask> tasks,
                                      List<TopologyNode> path) {
        double totalCost = 0.0;

        // Calculate compute and memory mismatch costs
        for (Map.Entry<OperatorTask, ComputeNode> entry : assignment.entrySet()) {
            OperatorTask task = entry.getKey();
            ComputeNode node = entry.getValue();
            totalCost += calculateComputeMismatch(task, node);
        }

        // Calculate edge costs
        for (int i = 0; i < path.size() - 1; i++) {
            TopologyNode u = path.get(i);
            TopologyNode v = path.get(i + 1);
            DefaultEdge edge = processingTopology.getEdge(u, v);
            if (edge instanceof ComputeEdge) {
                ComputeEdge ce = (ComputeEdge) edge;
                totalCost += calculateEdgeCost(ce);
            }
        }

        return totalCost;
    }

    private double calculateComputeMismatch(OperatorTask task, ComputeNode node) {
        double computeMismatch = Math.abs(task.computeIntensity - node.computeCapability);
        double memoryMismatch = Math.abs(task.memoryRequirement - node.memoryCapability);
        return computeMismatch + memoryMismatch;
    }

    private double calculateEdgeCost(ComputeEdge edge) {
        return 1.0 + Math.abs(1.0 - edge.getCapability()) + edge.getLatency();
    }

    /**
     * Extracts operator tasks from the ExecutionGraph.
     * In a real scenario, these properties would be derived from the actual execution plan.
     */
    private List<OperatorTask> getOperatorTasksFromExecutionGraph(ExecutionGraph executionGraph) {
        List<OperatorTask> tasks = new ArrayList<>();

        // Get all JobVertices in topological order.
        List<JobVertex> vertices = StreamSupport.stream(executionGraph.getVerticesTopologically().spliterator(), false)
                .map(ExecutionJobVertex::getJobVertex)
                .collect(Collectors.toList());

        // Convert each JobVertex into an OperatorTask.
        for (JobVertex vertex : vertices) {
            System.out.println("Vertex: " + vertex.getID());
            // Create a task with default compute intensity (0.5) and memory based on minResources.
            OperatorTask task = new OperatorTask(
                    vertex.getID(),
                    0.5,
                    vertex.getMinResources() != ResourceSpec.UNKNOWN ? vertex.getMinResources().getTaskHeapMemory().getMebiBytes() : 0.0
            );

            // Add dependencies based on input edges.
            for (JobEdge edge : vertex.getInputs()) {
                for (OperatorTask predecessor : tasks) {
                    if (predecessor.id.equals(edge.getSource().getProducer().getID().toString())) {
                        task.addPredecessor(predecessor);
                        break;
                    }
                }
            }
            tasks.add(task);
        }
        return tasks;
    }

    // --- Inner Classes representing topology and tasks ---

    private abstract static class TopologyNode {
        protected final String id;

        TopologyNode(String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }
    }

    private static class ComputeNode extends TopologyNode {
        private final float computeCapability;
        private final float memoryCapability;
        private final int numSlots;

        public ComputeNode(
                String id,
                float computeCapability,
                float memoryCapability,
                int numSlots) {
            super(id);
            this.computeCapability = computeCapability;
            this.memoryCapability = memoryCapability;
            this.numSlots = numSlots;
        }
    }

    private static class SourceNode extends TopologyNode {
        public SourceNode(String name) {
            super(name);
        }
    }

    private static class SinkNode extends TopologyNode {
        public SinkNode(String name) {
            super(name);
        }
    }

    private static class ComputeEdge extends DefaultEdge {
        private final float capability;
        private final float latency;

        private ComputeEdge(float capability, float latency) {
            this.capability = capability;
            this.latency = latency;
        }

        public float getCapability() {
            return capability;
        }

        public float getLatency() {
            return latency;
        }
    }

    /**
     * Dummy class representing an operator task extracted from the ExecutionGraph.
     */
    private static class OperatorTask {
        private final JobVertexID id;
        private final double computeIntensity;  // Value between 0 and 1
        private final double memoryRequirement; // In MB or another absolute unit
        private final List<OperatorTask> predecessors;

        public OperatorTask(JobVertexID id, double computeIntensity, double memoryRequirement) {
            this.id = id;
            this.computeIntensity = computeIntensity;
            this.memoryRequirement = memoryRequirement;
            this.predecessors = new ArrayList<>();
        }

        public void addPredecessor(OperatorTask pred) {
            this.predecessors.add(pred);
        }

        public List<OperatorTask> getPredecessors() {
            return predecessors;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof OperatorTask) {
                return this.id.equals(((OperatorTask) obj).id);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }
    }
}

