package org.apache.flink.runtime.scheduler.adapter;

import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionGraphPlacement;

import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.nio.Attribute;
import org.jgrapht.nio.graphml.GraphMLImporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.Locale;
import java.util.stream.Collectors;

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
        if (path == null || path.trim().isEmpty()) {
            throw new IllegalArgumentException("GraphML path must be provided");
        }

        Graph<String, DefaultEdge> rawGraph = new DefaultDirectedGraph<>(DefaultEdge.class);
        Map<String, Map<String, Attribute>> vertexAttributes = new HashMap<>();
        GraphMLImporter<String, DefaultEdge> importer = new GraphMLImporter<>();
        importer.setVertexFactory(id -> id);
        importer.addVertexWithAttributesConsumer((vertex, attributes) -> {
            Map<String, Attribute> attributeCopy = new HashMap<>();
            if (attributes != null) {
                attributeCopy.putAll(attributes);
            }
            vertexAttributes.put(vertex, attributeCopy);
        });
        importer.addVertexAttributeConsumer((pair, attribute) -> {
            String vertexId = pair.getFirst();
            String attrKey = pair.getSecond();
            vertexAttributes
                    .computeIfAbsent(vertexId, ignored -> new HashMap<>())
                    .put(attrKey, attribute);
        });

        try (InputStream stream = openGraphMlStream(path);
                InputStreamReader reader = new InputStreamReader(stream, StandardCharsets.UTF_8)) {
            importer.importGraph(rawGraph, reader);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load topology GraphML from " + path, e);
        }

        Graph<TopologyNode, DefaultEdge> graph = new DefaultDirectedGraph<>(DefaultEdge.class);
        Map<String, TopologyNode> nodeMapping = new HashMap<>();

        for (String vertexId : rawGraph.vertexSet()) {
            Map<String, Attribute> attributes = vertexAttributes.getOrDefault(vertexId, Collections.emptyMap());
            TopologyNode node = createTopologyNode(vertexId, attributes);
            graph.addVertex(node);
            nodeMapping.put(vertexId, node);
        }

        for (DefaultEdge edge : rawGraph.edgeSet()) {
            TopologyNode source = nodeMapping.get(rawGraph.getEdgeSource(edge));
            TopologyNode target = nodeMapping.get(rawGraph.getEdgeTarget(edge));
            graph.addEdge(source, target);
        }

        return graph;
    }

    private TopologyNode createTopologyNode(String vertexId, Map<String, Attribute> attributes) {
        String type = readAttribute(attributes, "type");
        if (type == null) {
            throw new IllegalArgumentException("Missing 'type' attribute for vertex " + vertexId);
        }

        String nodeId = Optional.ofNullable(readAttribute(attributes, "id")).filter(s -> !s.isEmpty()).orElse(vertexId);
        String normalizedType = type.trim().toLowerCase(Locale.ROOT);

        switch (normalizedType) {
            case "source":
                return new SourceNode(nodeId);
            case "sink":
                return new SinkNode(nodeId);
            case "compute":
            case "compute_node":
            case "node":
                double computeCapability = parseDouble(firstNonEmpty(
                        readAttribute(attributes, "computeCapability"),
                        readAttribute(attributes, "compute_capability")), 1.0);
                double memoryCapability = parseDouble(firstNonEmpty(
                        readAttribute(attributes, "memoryCapability"),
                        readAttribute(attributes, "memory_capability")), 1.0);
                int slots = parseInt(firstNonEmpty(
                        readAttribute(attributes, "slots"),
                        readAttribute(attributes, "numSlots"),
                        readAttribute(attributes, "num_slots")), 1);
                return new ComputeNode(nodeId, computeCapability, memoryCapability, slots);
            default:
                throw new IllegalArgumentException(
                        "Unsupported topology node type '" + type + "' for vertex " + vertexId);
        }
    }

    private String readAttribute(Map<String, Attribute> attributes, String key) {
        if (attributes == null || attributes.isEmpty()) {
            return null;
        }
        Attribute attribute = attributes.get(key);
        if (attribute == null) {
            attribute = attributes.get(key.toLowerCase(Locale.ROOT));
        }
        if (attribute == null) {
            for (Map.Entry<String, Attribute> entry : attributes.entrySet()) {
                if (key.equalsIgnoreCase(entry.getKey())) {
                    attribute = entry.getValue();
                    break;
                }
            }
        }
        if (attribute == null) {
            return null;
        }
        return attribute.getValue();
    }

    private String firstNonEmpty(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (value != null && !value.trim().isEmpty()) {
                return value;
            }
        }
        return null;
    }

    private double parseDouble(String value, double defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid double value '" + value + "' in GraphML", e);
        }
    }

    private int parseInt(String value, int defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer value '" + value + "' in GraphML", e);
        }
    }

    private InputStream openGraphMlStream(String graphMlPath) throws IOException {
        try {
            Path filePath = Paths.get(graphMlPath);
            if (Files.exists(filePath)) {
                return Files.newInputStream(filePath);
            }
        } catch (InvalidPathException e) {
            LOG.debug("Provided GraphML path '{}' is not a file path", graphMlPath, e);
        }

        InputStream resource = TopDownBottomUpExecutionGraphPlacement.class
                .getClassLoader()
                .getResourceAsStream(graphMlPath);
        if (resource != null) {
            return resource;
        }

        throw new IOException("Topology GraphML not found at " + graphMlPath);
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
