package org.apache.flink.runtime.scheduler.adapter;

import org.apache.flink.runtime.jobgraph.JobEdge;

import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionGraphPlacement;

import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.stream.Collectors;

//import org.jgrapht.nio.Attribute;
//import org.jgrapht.nio.DefaultAttribute;
//import org.jgrapht.nio.graphml.GraphMLImporter;

import static org.apache.flink.configuration.ClusterOptions.PLACEMENT_METHOD;

/**
 * An ExecutionGraphPlacement implementation that assigns pipeline operators
 * to physical compute nodes using either top-down or bottom-up BFS mapping.
 * Source and sink are auto-detected in the topology as indicator nodes.
 */
public class TopDownBottomUpExecutionGraphPlacement implements ExecutionGraphPlacement {
    private static final Logger LOG = LoggerFactory.getLogger(TopDownBottomUpExecutionGraphPlacement.class);

    private final Graph<TopologyNode, DefaultEdge> processingTopology;
    private final ClusterOptions.PlacementMethod placementMethod;

    public TopDownBottomUpExecutionGraphPlacement(ClusterOptions.PlacementMethod placementMethod, String graphMlPath) {
        this.placementMethod = placementMethod;
        this.processingTopology = loadTopologyFromGraphML(graphMlPath);
    }

    private Graph<TopologyNode, DefaultEdge> loadTopologyFromGraphML(String path) {
//        Graph<TopologyNode, DefaultEdge> graph = new DefaultDirectedGraph<>(DefaultEdge.class);
//        // Importer to create our topology nodes
//        GraphMLImporter<TopologyNode, DefaultEdge> importer = new GraphMLImporter<>(
//                (id, attrs) -> {
//                    String type = attrs.getOrDefault("type", DefaultAttribute.createAttribute("compute")).getValue();
//                    switch (type.toLowerCase()) {
//                        case "source": return new SourceNode(id);
//                        case "sink":   return new SinkNode(id);
//                        default:
//                            int slots = Integer.parseInt(attrs.getOrDefault("slots", DefaultAttribute.createAttribute("1")).getValue());
//                            double comp = Double.parseDouble(attrs.getOrDefault("computeCapability", DefaultAttribute.createAttribute("1.0")).getValue());
//                            double mem  = Double.parseDouble(attrs.getOrDefault("memoryCapability", DefaultAttribute.createAttribute("1.0")).getValue());
//                            return new ComputeNode(id, comp, mem, slots);
//                    }
//                },
//                (from, to, label, attrs) -> graph.getEdgeFactory().createEdge(from, to)
//        );
//        try (InputStream in = getClass().getResourceAsStream(path)) {
//            if (in == null) {
//                throw new IllegalArgumentException("Cannot find topology at " + path);
//            }
//            importer.importGraph(graph, new InputStreamReader(in));
//        } catch (IOException e) {
//            throw new UncheckedIOException("Failed to load GraphML", e);
//        }
//        return graph;
        // Hardcoded directed topology; replace with GraphML import if desired
        Graph<TopologyNode, DefaultEdge> graph = new DefaultDirectedGraph<>(DefaultEdge.class);
        SourceNode src = new SourceNode("Source1");
        ComputeNode n2 = new ComputeNode("10.10.10.2", 1.0, 1.0, 2);
        ComputeNode n3 = new ComputeNode("10.10.10.3", 1.0, 1.0, 2);
        ComputeNode n5 = new ComputeNode("10.10.10.5", 1.0, 1.0, 2);
        ComputeNode n7 = new ComputeNode("10.10.10.7", 1.0, 1.0, 2);
        ComputeNode n8 = new ComputeNode("10.10.10.8", 1.0, 1.0, 2);
        SinkNode sink = new SinkNode("Sink1");

        graph.addVertex(src);
        graph.addVertex(n2);
        graph.addVertex(n3);
        graph.addVertex(n5);
        graph.addVertex(n7);
        graph.addVertex(n8);
        graph.addVertex(sink);

        graph.addEdge(src, n8);
        graph.addEdge(n8, n7);
        graph.addEdge(n7, n3);
        graph.addEdge(n7, n5);
        graph.addEdge(n3, n2);
        graph.addEdge(n5, n2);
        graph.addEdge(n2, sink);

        return graph;
    }

    @Override
    public void assignPlacement(ExecutionGraph executionGraph) {
        JobVertexID srcId = null;
        JobVertexID sinkId = null;
        Map<JobVertexID, Set<JobVertexID>> dependencies = new HashMap<>();
        Map<JobVertexID, Set<JobVertexID>> reverseDependencies = new HashMap<>();

        for (ExecutionJobVertex ejv : executionGraph.getVerticesTopologically()) {
            JobVertex jobVertex = ejv.getJobVertex();
            JobVertexID id = jobVertex.getID();

            if (jobVertex.getInputs().isEmpty()) {
                srcId = id;
            } else if (jobVertex.getProducedDataSets().isEmpty()) {
                sinkId = id;
            }

            dependencies.put(id, new HashSet<>());
            reverseDependencies.put(id, new HashSet<>());

            for (JobEdge input : jobVertex.getInputs()) {
                JobVertex sourceVertex = input.getSource().getProducer();
                dependencies.get(id).add(sourceVertex.getID());
                reverseDependencies.get(sourceVertex.getID()).add(id);
            }
        }

        if (srcId == null || sinkId == null) {
            throw new IllegalStateException("Could not detect source or sink in ExecutionGraph");
        }

        List<JobVertexID> opOrder = new ArrayList<>();
        Set<JobVertexID> opVisited = new HashSet<>();
        Deque<JobVertexID> opQueue = new ArrayDeque<>();
        opQueue.add(srcId);

        while (!opQueue.isEmpty()) {
            JobVertexID current = opQueue.poll();
            if (!opVisited.add(current)) continue;

            if (current != srcId && current != sinkId) {
                opOrder.add(current);
            }

            for (JobVertexID dep : reverseDependencies.get(current)) {
                if (!opVisited.contains(dep)) {
                    opQueue.add(dep);
                }
            }
        }

        TopologyNode topoSource = processingTopology
                .vertexSet()
                .stream()
                .filter(n -> n instanceof SourceNode)
                .findFirst()
                .orElseThrow(() -> new NoSuchElementException("No source indicator in topology"));
        TopologyNode topoSink = processingTopology
                .vertexSet()
                .stream()
                .filter(n -> n instanceof SinkNode)
                .findFirst()
                .orElseThrow(() -> new NoSuchElementException("No sink indicator in topology"));

        List<JobVertexID> finalOpOrder = new ArrayList<>();
        finalOpOrder.add(srcId);
        finalOpOrder.addAll(opOrder);
        finalOpOrder.add(sinkId);

        TopologyNode start = (placementMethod
                == ClusterOptions.PlacementMethod.TOP_DOWN) ? topoSink : topoSource;
        Deque<TopologyNode> topoQueue = new ArrayDeque<>();
        Set<TopologyNode> topoVisited = new HashSet<>();
        List<ComputeNode> bfsNodes = new ArrayList<>();
        topoQueue.add(start);

        while (!topoQueue.isEmpty()) {
            TopologyNode node = topoQueue.poll();
            if (!topoVisited.add(node)) continue;
            if (node instanceof ComputeNode) {
                bfsNodes.add((ComputeNode) node);
            }

            if (placementMethod == ClusterOptions.PlacementMethod.TOP_DOWN) {
                for (DefaultEdge e : processingTopology.incomingEdgesOf(node)) {
                    TopologyNode nb = processingTopology.getEdgeSource(e);
                    if (!topoVisited.contains(nb)) topoQueue.add(nb);
                }
            } else {
                for (DefaultEdge e : processingTopology.outgoingEdgesOf(node)) {
                    TopologyNode nb = processingTopology.getEdgeTarget(e);
                    if (!topoVisited.contains(nb)) topoQueue.add(nb);
                }
            }
        }

        if (placementMethod == ClusterOptions.PlacementMethod.TOP_DOWN) {
            Collections.reverse(finalOpOrder);
        }

        Map<ComputeNode, Integer> slots = processingTopology.vertexSet().stream()
                .filter(n -> n instanceof ComputeNode)
                .map(n -> (ComputeNode) n)
                .collect(Collectors.toMap(n -> n, n -> n.numSlots));

        LOG.debug("Placement method: {}", placementMethod);
        int idx = 0;
        for (JobVertexID vertexId : finalOpOrder) {
            while (idx < bfsNodes.size() && slots.get(bfsNodes.get(idx)) <= 0) {
                idx++;
            }
            if (idx >= bfsNodes.size()) {
                throw new RuntimeException("Not enough free slots for placement");
            }
            ComputeNode target = bfsNodes.get(idx);
            slots.put(target, slots.get(target) - 1);
            executionGraph.getJobVertex(vertexId)
                    .getResourceProfile()
                    .setTaskManagerAddress(target.getId());
            LOG.debug("Assigned operator {} to compute node {}", vertexId, target.getId());
        }
    }

    private abstract static class TopologyNode {
        private final String id;

        protected TopologyNode(String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }
    }

    private static class SourceNode extends TopologyNode {
        public SourceNode(String id) {
            super(id);
        }
    }

    private static class SinkNode extends TopologyNode {
        public SinkNode(String id) {
            super(id);
        }
    }

    private static class ComputeNode extends TopologyNode {
        final double computeCapability;
        final double memoryCapability;
        final int numSlots;

        public ComputeNode(String id, double compCap, double memCap, int slots) {
            super(id);
            this.computeCapability = compCap;
            this.memoryCapability = memCap;
            this.numSlots = slots;
        }
    }
}
