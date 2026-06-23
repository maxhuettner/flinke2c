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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * An ExecutionGraphPlacement implementation that assigns pipeline operators
 * to physical compute nodes using either top-down or bottom-up BFS mapping.
 * Source and sink are auto-detected in the topology as indicator nodes.
 *
 * <p>Sticky placement (since 2026-06): when a sidecar TSV at
 * {@code cluster.placement.sticky-state-path} records a prior assignment, this
 * class pins each operator to its previous host if that host is still eligible
 * (not {@code excluded} in the graphml) and has a free slot. Only operators
 * whose previous host is no longer eligible (the controller-flagged culprit's
 * operators), plus genuinely new operators, fall through to the capability-sort
 * fill in Pass 2. The new assignment is persisted before returning. This means
 * a redeploy after the monitor marks one host {@code excluded} relocates only
 * that host's operators — the rest of the job stays put.
 *
 * <p>First placement (no state file): Pass 1 pins nothing and Pass 2 reduces
 * to the original positional fill. Same behaviour as before the sticky change.
 */
public class TopDownBottomUpExecutionGraphPlacement implements ExecutionGraphPlacement {
    private static final Logger LOG = LoggerFactory.getLogger(TopDownBottomUpExecutionGraphPlacement.class);

    private final Graph<TopologyNode, DefaultEdge> processingTopology;
    private final ClusterOptions.PlacementMethod placementMethod;
    private final boolean capabilitySort;
    private final Path stickyStatePath; // null if sticky disabled

    public TopDownBottomUpExecutionGraphPlacement(ClusterOptions.PlacementMethod placementMethod, String graphMlPath) {
        this(placementMethod, graphMlPath, true, null);
    }

    public TopDownBottomUpExecutionGraphPlacement(
            ClusterOptions.PlacementMethod placementMethod,
            String graphMlPath,
            boolean capabilitySort) {
        this(placementMethod, graphMlPath, capabilitySort, null);
    }

    public TopDownBottomUpExecutionGraphPlacement(
            ClusterOptions.PlacementMethod placementMethod,
            String graphMlPath,
            boolean capabilitySort,
            String stickyStatePath) {
        this.placementMethod = placementMethod;
        this.processingTopology = loadTopologyFromGraphML(graphMlPath);
        this.capabilitySort = capabilitySort;
        this.stickyStatePath = resolveStickyStatePath(stickyStatePath, graphMlPath);
    }

    /**
     * Default location for the sticky state file: same directory as the graphml,
     * filename {@code placement-state.tsv}. Returns {@code null} if no usable
     * filesystem path can be derived (e.g. graphml was loaded from the classpath).
     */
    private static Path resolveStickyStatePath(String configured, String graphMlPath) {
        if (configured != null && !configured.trim().isEmpty()) {
            try {
                return Paths.get(configured.trim());
            } catch (InvalidPathException e) {
                LOG.warn("Invalid cluster.placement.sticky-state-path '{}' — sticky disabled", configured);
                return null;
            }
        }
        if (graphMlPath == null || graphMlPath.trim().isEmpty()) return null;
        try {
            Path gml = Paths.get(graphMlPath);
            Path parent = gml.toAbsolutePath().getParent();
            if (parent == null) return null;
            return parent.resolve("placement-state.tsv");
        } catch (InvalidPathException e) {
            LOG.info("Graphml path '{}' is not a filesystem path — sticky placement disabled", graphMlPath);
            return null;
        }
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
                boolean excluded = parseBoolean(readAttribute(attributes, "excluded"), false);
                return new ComputeNode(nodeId, computeCapability, memoryCapability, slots, excluded);
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

    private boolean parseBoolean(String value, boolean defaultValue) {
        if (value == null || value.trim().isEmpty()) return defaultValue;
        String v = value.trim();
        if ("true".equalsIgnoreCase(v) || "1".equals(v) || "yes".equalsIgnoreCase(v)) return true;
        if ("false".equalsIgnoreCase(v) || "0".equals(v) || "no".equalsIgnoreCase(v)) return false;
        return defaultValue;
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
        // ----- Build operator order (same as before: source -> reverse-BFS BFS-fillers -> sink) -----
        JobVertexID srcId = null;
        JobVertexID sinkId = null;
        Map<JobVertexID, Set<JobVertexID>> dependencies = new HashMap<>();
        Map<JobVertexID, Set<JobVertexID>> reverseDependencies = new HashMap<>();
        Map<JobVertexID, String> idToName = new HashMap<>();

        for (ExecutionJobVertex ejv : executionGraph.getVerticesTopologically()) {
            JobVertex jobVertex = ejv.getJobVertex();
            JobVertexID id = jobVertex.getID();
            idToName.put(id, jobVertex.getName());

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

        List<JobVertexID> finalOpOrder = new ArrayList<>();
        finalOpOrder.add(srcId);
        finalOpOrder.addAll(opOrder);
        finalOpOrder.add(sinkId);

        // ----- Build the compute-node ordering (BFS from source or sink) -----
        TopologyNode topoSource = processingTopology
                .vertexSet()
                .stream()
                .filter(SourceNode.class::isInstance)
                .findFirst()
                .orElseThrow(() -> new NoSuchElementException("No source indicator in topology"));
        TopologyNode topoSink = processingTopology
                .vertexSet()
                .stream()
                .filter(SinkNode.class::isInstance)
                .findFirst()
                .orElseThrow(() -> new NoSuchElementException("No sink indicator in topology"));

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

        // ----- Filter excluded nodes; capability-sort the remainder -----
        List<ComputeNode> eligibleNodes = bfsNodes.stream()
                .filter(n -> !n.excluded)
                .collect(Collectors.toList());

        List<String> excludedIds = bfsNodes.stream()
                .filter(n -> n.excluded)
                .map(ComputeNode::getId)
                .collect(Collectors.toList());
        if (!excludedIds.isEmpty()) {
            LOG.info("Excluding {} from placement (excluded=true in graphml)", excludedIds);
        }

        if (capabilitySort) {
            eligibleNodes.sort(Comparator.comparingDouble((ComputeNode n) -> n.computeCapability).reversed());
            LOG.info("Capability-sorted eligible compute nodes: {}",
                    eligibleNodes.stream()
                            .map(n -> n.getId() + "(cap=" + n.computeCapability + ")")
                            .collect(Collectors.joining(", ")));
        }

        Map<String, ComputeNode> nodeById = new HashMap<>();
        for (ComputeNode n : eligibleNodes) {
            nodeById.put(n.getId(), n);
        }

        Map<ComputeNode, Integer> slots = eligibleNodes.stream()
                .collect(Collectors.toMap(n -> n, n -> n.numSlots));

        LOG.debug("Placement method: {}, capability-sort: {}, sticky-state-path: {}",
                placementMethod, capabilitySort, stickyStatePath);

        // ----- Pass 1: sticky pin to previous host -----
        Map<String, String> prevOpToHost = loadStickyState();
        Map<String, Long> opNameCounts = finalOpOrder.stream()
                .map(idToName::get)
                .collect(Collectors.groupingBy(n -> n, Collectors.counting()));

        Set<JobVertexID> pinned = new HashSet<>();
        int stickyPinCount = 0;
        for (JobVertexID vertexId : finalOpOrder) {
            String opName = idToName.get(vertexId);
            if (opName == null) continue;
            // Ambiguous names: identical strings → can't tell which prior assignment maps to which now.
            if (opNameCounts.get(opName) > 1) {
                LOG.warn("Operator name '{}' is not unique in this job; falling back to positional placement for those operators",
                        opName);
                continue;
            }
            String prevHost = prevOpToHost.get(opName);
            if (prevHost == null) continue;
            ComputeNode candidate = nodeById.get(prevHost);
            if (candidate == null) {
                LOG.debug("Sticky miss for {}: previous host {} is not eligible (excluded or absent)", opName, prevHost);
                continue;
            }
            Integer free = slots.get(candidate);
            if (free == null || free <= 0) {
                LOG.debug("Sticky miss for {}: previous host {} has no free slot", opName, prevHost);
                continue;
            }
            executionGraph.getJobVertex(vertexId)
                    .getResourceProfile()
                    .setTaskManagerAddress(candidate.getId());
            slots.put(candidate, free - 1);
            pinned.add(vertexId);
            stickyPinCount++;
            LOG.debug("Sticky-pinned operator {} ({}) -> {}", opName, vertexId, candidate.getId());
        }
        LOG.info("Sticky placement: {}/{} operators pinned to previous host",
                stickyPinCount, finalOpOrder.size());

        // ----- Pass 2: capability-sort fill for unpinned operators -----
        int idx = 0;
        for (JobVertexID vertexId : finalOpOrder) {
            if (pinned.contains(vertexId)) continue;
            while (idx < eligibleNodes.size() && slots.get(eligibleNodes.get(idx)) <= 0) {
                idx++;
            }
            if (idx >= eligibleNodes.size()) {
                throw new RuntimeException("Not enough free slots for placement");
            }
            ComputeNode target = eligibleNodes.get(idx);
            slots.put(target, slots.get(target) - 1);
            executionGraph.getJobVertex(vertexId)
                    .getResourceProfile()
                    .setTaskManagerAddress(target.getId());
            LOG.debug("Pass-2 assigned operator {} ({}) to compute node {}",
                    idToName.get(vertexId), vertexId, target.getId());
        }

        // ----- Persist sticky state for the next placement -----
        Map<String, String> newOpToHost = new LinkedHashMap<>();
        for (JobVertexID vertexId : finalOpOrder) {
            String opName = idToName.get(vertexId);
            if (opName == null) continue;
            String host = executionGraph.getJobVertex(vertexId).getResourceProfile().getTaskManagerAddress();
            if (host != null) {
                newOpToHost.put(opName, host);
            }
        }
        saveStickyState(newOpToHost);
    }

    // ----- Sticky state TSV helpers ---------------------------------------

    /**
     * Load the previous operator-name -> host map from the TSV sidecar. Returns
     * an empty map if the file doesn't exist or is unreadable (treated as "no
     * sticky info" — falls through to positional fill). Lines are
     * {@code host\toperatorName}; lines starting with {@code #} are comments.
     */
    private Map<String, String> loadStickyState() {
        Map<String, String> out = new HashMap<>();
        if (stickyStatePath == null) return out;
        if (!Files.exists(stickyStatePath)) {
            LOG.info("Sticky state file {} not present — Pass 1 will pin nothing (first placement)", stickyStatePath);
            return out;
        }
        try {
            List<String> lines = Files.readAllLines(stickyStatePath, StandardCharsets.UTF_8);
            for (String line : lines) {
                String s = line.trim();
                if (s.isEmpty() || s.startsWith("#")) continue;
                int tab = s.indexOf('\t');
                if (tab < 0) continue;
                String host = s.substring(0, tab).trim();
                String opName = s.substring(tab + 1).trim();
                if (host.isEmpty() || opName.isEmpty()) continue;
                out.put(opName, host);
            }
            LOG.info("Loaded sticky state from {}: {} entries", stickyStatePath, out.size());
        } catch (IOException e) {
            LOG.warn("Could not read sticky state from {} — falling back to positional placement", stickyStatePath, e);
            return new HashMap<>();
        }
        return out;
    }

    /**
     * Persist the freshly-computed operator-name -> host map. Atomic move via
     * a sibling {@code .tmp} so a concurrent reader (next placement on a
     * different JM thread, if any) never sees a half-written file.
     */
    private void saveStickyState(Map<String, String> opToHost) {
        if (stickyStatePath == null) return;
        try {
            Path parent = stickyStatePath.getParent();
            if (parent != null) Files.createDirectories(parent);
            Path tmp = stickyStatePath.resolveSibling(stickyStatePath.getFileName() + ".tmp");
            try (BufferedWriter w = Files.newBufferedWriter(tmp, StandardCharsets.UTF_8)) {
                w.write("# placement-state.tsv  —  <hostId>\\t<operatorName>\n");
                w.write("# auto-generated by TopDownBottomUpExecutionGraphPlacement on each assignPlacement\n");
                for (Map.Entry<String, String> e : opToHost.entrySet()) {
                    w.write(e.getValue());
                    w.write('\t');
                    w.write(e.getKey());
                    w.write('\n');
                }
            }
            try {
                Files.move(tmp, stickyStatePath,
                        StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException e) {
                Files.move(tmp, stickyStatePath, StandardCopyOption.REPLACE_EXISTING);
            }
            LOG.debug("Persisted sticky state ({} entries) to {}", opToHost.size(), stickyStatePath);
        } catch (IOException e) {
            LOG.warn("Could not write sticky state to {} — next placement will fall back to positional fill",
                    stickyStatePath, e);
        }
    }

    // ----- Topology node types --------------------------------------------

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
        final boolean excluded;

        public ComputeNode(String id, double compCap, double memCap, int slots, boolean excluded) {
            super(id);
            this.computeCapability = compCap;
            this.memoryCapability = memCap;
            this.numSlots = slots;
            this.excluded = excluded;
        }
    }
}
