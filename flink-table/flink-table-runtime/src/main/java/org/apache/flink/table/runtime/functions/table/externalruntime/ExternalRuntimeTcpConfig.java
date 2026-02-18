package org.apache.flink.table.runtime.functions.table.externalruntime;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

final class ExternalRuntimeTcpConfig {
    private static final int DEFAULT_BUFFER_SIZE = 512 * 1024;
    private static final int DEFAULT_CONNECT_TIMEOUT_MS = 10_000;
    private static final int DEFAULT_BATCH_SIZE = 2048;
    private static final int DEFAULT_REORDER_MAX_BUFFER = 100000;
    private static final int DEFAULT_FAILOVER_RECONNECT_BACKOFF_MS = 1000;
    private static final int DEFAULT_FAILOVER_POLL_TIMEOUT_MS = 10;
    private static final boolean DEFAULT_POST_ROLE_ONLY = true;

    private final List<ExternalRuntimeEndpoint> runtimes;
    private final int runtimeParallelism;
    private final boolean autoParallelismEnabled;
    private final int batchSize;
    private final int bufferSize;
    private final int connectTimeoutMs;
    private final int readTimeoutMs;
    private final int failoverReconnectBackoffMs;
    private final int failoverPollTimeoutMs;

    private final String functionClass;
    private final String functionKind;

    private final List<Integer> argFieldIndices;
    private final List<String> argFieldNames;
    private final List<String> argFieldTypes;

    private final List<Integer> resultFieldIndices;
    private final List<String> resultFieldNames;
    private final List<String> resultFieldTypes;

    private final List<Integer> resultUdfFieldIndices;
    private final List<String> resultUdfFieldTypes;

    private final boolean reorderResponses;
    private final int reorderMaxBuffer;

    private final boolean postRoleOnly;

    private ExternalRuntimeTcpConfig(
            int bufferSize,
            int connectTimeoutMs,
            int readTimeoutMs,
            int failoverReconnectBackoffMs,
            int failoverPollTimeoutMs,
            String functionClass,
            String functionKind,
            List<Integer> argFieldIndices,
            List<String> argFieldNames,
            List<String> argFieldTypes,
            List<Integer> resultFieldIndices,
            List<String> resultFieldNames,
            List<String> resultFieldTypes,
            List<String> resultUdfFieldTypes,
            List<Integer> resultUdfFieldIndices,
            boolean reorderResponses,
            int reorderMaxBuffer,
            boolean postRoleOnly,
            List<ExternalRuntimeEndpoint> runtimes,
            int runtimeParallelism,
            boolean autoParallelismEnabled,
            int batchSize) {
        this.bufferSize = bufferSize;
        this.connectTimeoutMs = connectTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
        this.failoverReconnectBackoffMs = failoverReconnectBackoffMs;
        this.failoverPollTimeoutMs = failoverPollTimeoutMs;

        this.functionClass = functionClass;
        this.functionKind = functionKind;

        this.argFieldIndices = argFieldIndices;
        this.argFieldNames = argFieldNames;
        this.argFieldTypes = argFieldTypes;

        this.resultFieldIndices = resultFieldIndices;
        this.resultFieldNames = resultFieldNames;
        this.resultFieldTypes = resultFieldTypes;

        this.resultUdfFieldIndices = resultUdfFieldIndices;
        this.resultUdfFieldTypes = resultUdfFieldTypes;

        this.reorderResponses = reorderResponses;
        this.reorderMaxBuffer = reorderMaxBuffer;

        this.runtimes = runtimes;
        this.runtimeParallelism = runtimeParallelism;
        this.autoParallelismEnabled = autoParallelismEnabled;
        this.batchSize = batchSize;

        this.postRoleOnly = postRoleOnly;
    }

    static ExternalRuntimeTcpConfig from(String conf) {
        final Map<String, String> map = parse(conf);

        final int bufferSize = parseInt(map.get("buffersize"), DEFAULT_BUFFER_SIZE);
        final int connectTimeoutMs = parseInt(map.get("connecttimeoutms"), DEFAULT_CONNECT_TIMEOUT_MS);
        final int readTimeoutMs = parseInt(map.get("readtimeoutms"), 0);
        final int failoverReconnectBackoffMs =
                parseInt(
                        firstNonNull(map, "failoverreconnectbackoffms", "autofailoverreconnectbackoffms"),
                        DEFAULT_FAILOVER_RECONNECT_BACKOFF_MS);
        final int failoverPollTimeoutMs =
                parseInt(firstNonNull(map, "failoverpolltimeoutms", "autofailoverpolltimeoutms"), DEFAULT_FAILOVER_POLL_TIMEOUT_MS);

        final String functionClass = firstNonNull(map, "class", "functionclass");
        final String functionKind = firstNonNull(map, "type", "functionkind");

        final List<Integer> argFieldIndices = parseIntList(map.get("argfieldindices"));
        final List<String> argFieldNames = parseStringList(map.get("argfieldnames"));
        final List<String> argFieldTypes = parseStringList(map.get("argfieldtypes"));

        final List<Integer> resultFieldIndices = parseIntList(map.get("resultfieldindices"));
        final List<String> resultFieldNames = parseStringList(map.get("resultfieldnames"));
        final List<String> resultFieldTypes = parseStringList(map.get("resultfieldtypes"));

        final List<Integer> resultUdfFieldIndices = parseIntList(map.get("resultudffieldindices"));
        final List<String> resultUdfFieldTypes = parseStringList(map.get("resultudffieldtypes"));

        final int reorderMaxBuffer = parseInt(map.get("reordermax"), DEFAULT_REORDER_MAX_BUFFER);

        final boolean postRoleOnly = parseBoolean(map.get("postroleonly"), DEFAULT_POST_ROLE_ONLY);

        final List<ExternalRuntimeEndpoint> runtimes = parseRuntimes(map.get("runtimes"));
        final ParallelismSpec parallelismSpec =
                parseParallelismSpec(
                        firstNonNull(
                                map,
                                "runtimeparallelism",
                                "externalparallelism",
                                "parallelism",
                                "parallel"));
        final boolean autoParallelismEnabled =
                parseBoolean(
                        firstNonNull(map, "autoparallelism", "parallelismauto"),
                        parallelismSpec.autoParallelism);
        final int batchSize = parseInt(firstNonNull(map, "batchsize", "batchSize"), DEFAULT_BATCH_SIZE);

        return new ExternalRuntimeTcpConfig(
                bufferSize,
                connectTimeoutMs,
                readTimeoutMs,
                failoverReconnectBackoffMs,
                failoverPollTimeoutMs,
                functionClass,
                functionKind,
                argFieldIndices,
                argFieldNames,
                argFieldTypes,
                resultFieldIndices,
                resultFieldNames,
                resultFieldTypes,
                resultUdfFieldTypes,
                resultUdfFieldIndices,
                false,
                reorderMaxBuffer,
                postRoleOnly,
                runtimes,
                parallelismSpec.parallelism,
                autoParallelismEnabled,
                batchSize);
    }

    private static ParallelismSpec parseParallelismSpec(String value) {
        if (value == null || value.isEmpty()) {
            return new ParallelismSpec(0, false);
        }
        final String normalized = value.trim().toLowerCase(Locale.ROOT);
        if (normalized.isEmpty()) {
            return new ParallelismSpec(0, false);
        }
        if ("auto".equals(normalized)) {
            return new ParallelismSpec(0, true);
        }
        return new ParallelismSpec(Integer.parseInt(normalized), false);
    }

    private static List<ExternalRuntimeEndpoint> parseRuntimes(String value) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "ExternalRuntimeOperator requires runtimes=<host:port> or runtimes=<host:send:recv> in conf.");
        }
        final List<ExternalRuntimeEndpoint> runtimes = new ArrayList<>();
        final String[] entries = value.split(",");
        for (String entry : entries) {
            final String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            final String[] parts = trimmed.split(":");
            if (parts.length == 1) {
                throw new IllegalArgumentException(
                        "Invalid runtimes entry: " + trimmed + " (expected host:port or host:send:recv)");
            } else if (parts.length == 2) {
                final String h = parts[0].trim();
                final int port = Integer.parseInt(parts[1].trim());
                runtimes.add(new ExternalRuntimeEndpoint(h, port, port));
            } else if (parts.length == 3) {
                final String h = parts[0].trim();
                final int send = Integer.parseInt(parts[1].trim());
                final int recv = Integer.parseInt(parts[2].trim());
                runtimes.add(new ExternalRuntimeEndpoint(h, send, recv));
            } else {
                throw new IllegalArgumentException(
                        "Invalid runtimes entry: " + trimmed + " (expected host:port or host:send:recv)");
            }
        }
        if (runtimes.isEmpty()) {
            throw new IllegalArgumentException(
                    "ExternalRuntimeOperator requires at least one runtime entry in runtimes=...");
        }
        return runtimes;
    }

    protected static final class ExternalRuntimeEndpoint {
        private final String host;
        private final int sendPort;
        private final int receivePort;

        private ExternalRuntimeEndpoint(String host, int sendPort, int receivePort) {
            this.host = host;
            this.sendPort = sendPort;
            this.receivePort = receivePort;
        }

        String getHost() {
            return this.host;
        }

        int getSendPort() {
            return this.sendPort;
        }

        int getReceivePort() {
            return this.receivePort;
        }
    }

    private static final class ParallelismSpec {
        private final int parallelism;
        private final boolean autoParallelism;

        private ParallelismSpec(int parallelism, boolean autoParallelism) {
            this.parallelism = parallelism;
            this.autoParallelism = autoParallelism;
        }
    }

    private static Map<String, String> parse(String conf) {
        final Map<String, String> map = new HashMap<>();
        if (conf == null || conf.isEmpty()) {
            return map;
        }
        final String[] parts = conf.split(";");
        for (String part : parts) {
            final String trimmed = part.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            final int idx = trimmed.indexOf('=');
            if (idx <= 0 || idx == trimmed.length() - 1) {
                continue;
            }
            final String key = trimmed.substring(0, idx).trim().toLowerCase(Locale.ROOT);
            final String value = trimmed.substring(idx + 1).trim();
            if (!key.isEmpty()) {
                map.put(key, value);
            }
        }
        return map;
    }

    private static String firstNonNull(Map<String, String> map, String... keys) {
        for (String key : keys) {
            final String value = map.get(key);
            if (value != null && !value.isEmpty()) {
                return value;
            }
        }
        return null;
    }

    private static Integer parseInt(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        return Integer.parseInt(value);
    }

    private static List<Integer> parseIntList(String value) {
        final List<Integer> result = new ArrayList<>();
        if (value == null || value.isEmpty()) {
            return result;
        }
        final String[] parts = value.split(",");
        for (String part : parts) {
            final String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                result.add(Integer.parseInt(trimmed));
            }
        }
        return result;
    }

    private static List<String> parseStringList(String value) {
        final List<String> result = new ArrayList<>();
        if (value == null || value.isEmpty()) {
            return result;
        }
        final String[] parts = value.split(",");
        for (String part : parts) {
            final String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                result.add(URLDecoder.decode(trimmed, StandardCharsets.UTF_8));
            }
        }
        return result;
    }

    private static int parseInt(String value, int defaultValue) {
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }

    private static boolean parseBoolean(String value, boolean defaultValue) {
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value);
    }

    public int getConnectTimeoutMs() {
        return this.connectTimeoutMs;
    }

    public int getBufferSize() {
        return this.bufferSize;
    }

    public ExternalRuntimeEndpoint selectEndpoint(int subtaskIndex) {
        return selectEndpoints(subtaskIndex, 1).get(0);
    }

    public List<ExternalRuntimeEndpoint> selectEndpoints(int subtaskIndex, int totalSubtasks) {
        if (runtimes.isEmpty()) {
            throw new IllegalStateException("No external runtime endpoints configured.");
        }
        // With auto failover/auto parallelism enabled, all runtimes assigned to this
        // subtask must stay visible as candidate endpoints. Runtime parallelism still
        // controls the initial active width in PRE, but should not remove backups.
        final boolean includeAllCandidates = autoParallelismEnabled;
        final int max =
                includeAllCandidates
                        ? runtimes.size()
                        : (runtimeParallelism > 0
                                ? Math.min(runtimeParallelism, runtimes.size())
                                : runtimes.size());
        if (max <= 0) {
            throw new IllegalStateException("External runtime parallelism must be > 0.");
        }
        final int normalizedSubtasks = Math.max(1, totalSubtasks);
        final int normalizedIndex = Math.floorMod(subtaskIndex, normalizedSubtasks);
        final List<ExternalRuntimeEndpoint> selected = new ArrayList<>();
        for (int i = 0; i < max; i++) {
            if (Math.floorMod(i, normalizedSubtasks) == normalizedIndex) {
                selected.add(runtimes.get(i));
            }
        }
        if (!selected.isEmpty()) {
            return selected;
        }
        final int fallbackIdx = Math.floorMod(subtaskIndex, max);
        selected.add(runtimes.get(fallbackIdx));
        return selected;
    }

    public int getRuntimeParallelism() {
        return runtimeParallelism;
    }

    public int getBatchSize() {
        final int runtimeCount = effectiveRuntimeCount();
        final int perRuntime = batchSize / runtimeCount;
        return Math.max(1, perRuntime);
    }

    private int effectiveRuntimeCount() {
        final int max =
                runtimeParallelism > 0 ? Math.min(runtimeParallelism, runtimes.size()) : runtimes.size();
        return Math.max(1, max);
    }

    public int selectEndpointIndex(long rowId, int endpointsCount) {
        if (endpointsCount <= 0) {
            throw new IllegalStateException("External runtime endpoints count must be > 0.");
        }
        return Math.floorMod(rowId, endpointsCount);
    }

    public boolean isReorderResponses() {
        return false;
    }

    public boolean isAutoParallelismEnabled() {
        return autoParallelismEnabled;
    }

    public boolean isAutoFailoverEnabled() {
        // kept for compatibility, failover is implied by auto parallelism
        return autoParallelismEnabled;
    }

    public int getFailoverReconnectBackoffMs() {
        return failoverReconnectBackoffMs;
    }

    public int getFailoverPollTimeoutMs() {
        return failoverPollTimeoutMs;
    }

    public int getReadTimeoutMs() {
        return this.readTimeoutMs;
    }

    public int getReorderMaxBuffer() {
        return this.reorderMaxBuffer;
    }

    public boolean isPostRoleOnly() {
        return this.postRoleOnly;
    }

    public String getFunctionClass() {
        return this.functionClass;
    }

    public String getFunctionKind() {
        return this.functionKind;
    }

    public List<Integer> getArgFieldIndices() {
        return argFieldIndices;
    }

    public List<String> getArgFieldNames() {
        return argFieldNames;
    }

    public List<String> getArgFieldTypes() {
        return argFieldTypes;
    }

    public List<Integer> getResultFieldIndices() {
        return resultFieldIndices;
    }

    public List<String> getResultFieldNames() {
        return resultFieldNames;
    }

    public List<String> getResultFieldTypes() {
        return resultFieldTypes;
    }

    public List<Integer> getResultUdfFieldIndices() {
        return resultUdfFieldIndices;
    }

    public List<String> getResultUdfFieldTypes() {
        return resultUdfFieldTypes;
    }
}
