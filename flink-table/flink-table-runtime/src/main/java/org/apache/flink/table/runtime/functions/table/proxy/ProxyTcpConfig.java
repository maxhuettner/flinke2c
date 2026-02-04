package org.apache.flink.table.runtime.functions.table.proxy;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

final class ProxyTcpConfig {
    private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
    private static final int DEFAULT_CONNECT_TIMEOUT_MS = 10_000;

    private static final int DEFAULT_REORDER_MAX_BUFFER = 10000;
    private static final boolean DEFAULT_POST_ROLE_ONLY = true;

    private final List<ProxyEndpoint> proxies;
    private final ProxyEndpoint selectedProxy;
    private final int bufferSize;
    private final int connectTimeoutMs;
    private final int readTimeoutMs;

    private final Integer calcFieldIndex;
    private final String calcFieldName;
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

    private ProxyTcpConfig(
            int bufferSize,
            int connectTimeoutMs,
            int readTimeoutMs,
            Integer calcFieldIndex,
            String calcFieldName,
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
            List<ProxyEndpoint> proxies,
            ProxyEndpoint selectedProxy) {
        this.bufferSize = bufferSize;
        this.connectTimeoutMs = connectTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;

        this.calcFieldIndex = calcFieldIndex;
        this.calcFieldName = calcFieldName;
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

        this.proxies = proxies;
        this.selectedProxy = selectedProxy;

        this.postRoleOnly = postRoleOnly;
    }

    static ProxyTcpConfig from(String conf) {
        final Map<String, String> map = parse(conf);

        final int bufferSize = parseInt(map.get("buffersize"), DEFAULT_BUFFER_SIZE);
        final int connectTimeoutMs = parseInt(map.get("connecttimeoutms"), DEFAULT_CONNECT_TIMEOUT_MS);
        final int readTimeoutMs = parseInt(map.get("readtimeoutms"), 0);

        final boolean flushOnWrite = parseBoolean(map.get("flush"), false);

        final Integer calcFieldIndex = parseInt(map.get("calcfieldindex"));
        final String calcFieldName = map.get("calcfieldname");

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

        final boolean reorderResponses = parseBoolean(firstNonNull(map, "reorder", "correlate"), false);
        final int reorderMaxBuffer = parseInt(map.get("reordermax"), DEFAULT_REORDER_MAX_BUFFER);

        final boolean postRoleOnly = parseBoolean(map.get("postroleonly"), DEFAULT_POST_ROLE_ONLY);

        final List<ProxyEndpoint> proxies = parseProxies(map.get("proxies"));
        final ProxyEndpoint selectedProxy = proxies.get(0);

        return new ProxyTcpConfig(
                bufferSize,
                connectTimeoutMs,
                readTimeoutMs,
                calcFieldIndex,
                calcFieldName,
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
                reorderResponses,
                reorderMaxBuffer,
                postRoleOnly,
                proxies,
                selectedProxy);
    }

    private static List<ProxyEndpoint> parseProxies(String value) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "ProxyOperator requires proxies=<host:port> or proxies=<host:send:recv> in conf.");
        }
        final List<ProxyEndpoint> proxies = new ArrayList<>();
        final String[] entries = value.split(",");
        for (String entry : entries) {
            final String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            final String[] parts = trimmed.split(":");
            if (parts.length == 1) {
                throw new IllegalArgumentException(
                        "Invalid proxies entry: " + trimmed + " (expected host:port or host:send:recv)");
            } else if (parts.length == 2) {
                final String h = parts[0].trim();
                final int port = Integer.parseInt(parts[1].trim());
                proxies.add(new ProxyEndpoint(h, port, port));
            } else if (parts.length == 3) {
                final String h = parts[0].trim();
                final int send = Integer.parseInt(parts[1].trim());
                final int recv = Integer.parseInt(parts[2].trim());
                proxies.add(new ProxyEndpoint(h, send, recv));
            } else {
                throw new IllegalArgumentException(
                        "Invalid proxies entry: " + trimmed + " (expected host:port or host:send:recv)");
            }
        }
        if (proxies.isEmpty()) {
            throw new IllegalArgumentException(
                    "ProxyOperator requires at least one proxy entry in proxies=...");
        }
        return proxies;
    }

    protected static final class ProxyEndpoint {
        private final String host;
        private final int sendPort;
        private final int receivePort;

        private ProxyEndpoint(String host, int sendPort, int receivePort) {
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

    public ProxyEndpoint getSelectedProxy() {
        return this.selectedProxy;
    }

    public boolean isReorderResponses() {
        return this.reorderResponses;
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
