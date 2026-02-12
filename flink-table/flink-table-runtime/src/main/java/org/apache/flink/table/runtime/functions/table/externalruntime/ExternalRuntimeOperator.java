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

package org.apache.flink.table.runtime.functions.table.externalruntime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.functions.table.externalruntime.ExternalRuntimeBinaryCodec.WireType;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** Shared base class for external runtime PRE/POST operators. */
@Internal
abstract class ExternalRuntimeOperator extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData> {

    protected static final Logger LOG = LoggerFactory.getLogger(ExternalRuntimeOperator.class);
    private static final long serialVersionUID = 1L;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    protected enum Role {
        PRE,
        POST
    }

    protected final String conf;
    protected final RowType inputRowType;
    protected final RowType resultRowType;

    protected transient ExternalRuntimeTcpConfig tcpConfig;

    protected transient List<Integer> payloadFieldIndices;
    protected transient int[] payloadFieldIndicesArray;
    protected transient List<String> payloadFieldNames;
    protected transient List<LogicalType> payloadWriteTypes;
    protected transient LogicalTypeRoot[] payloadSourceRoots;
    protected transient int[] payloadTargetPrecision;
    protected transient int[] payloadTargetScale;
    protected transient int[] payloadSourcePrecision;
    protected transient int[] payloadSourceScale;
    protected transient int[] payloadTimestampPrecision;
    protected transient WireType[] payloadWireTypes;

    protected transient List<Integer> resultFieldIndices;
    protected transient List<LogicalType> resultFieldTypes;
    protected transient List<LogicalType> resultReadTypes;
    protected transient List<LogicalType> postFieldTypes;
    protected transient WireType[] resultWireTypes;

    protected transient RowData.FieldGetter[] resultFieldGetters;
    protected transient RowData.FieldGetter[] fullRowFieldGetters;
    protected transient int[] resultPosByInputIndex;
    protected transient boolean resultReplacesAllFields;

    protected transient GenericRowData insertPlaceholder;
    protected transient GenericRowData updateAfterPlaceholder;
    protected transient GenericRowData updateBeforePlaceholder;
    protected transient GenericRowData deletePlaceholder;
    protected transient int inputFieldCount;

    protected transient Socket socket;

    protected transient ExternalRuntimeBinaryCodec codec;

    protected ExternalRuntimeOperator(String conf, RowType rowType) {
        this(conf, rowType, rowType);
    }

    protected ExternalRuntimeOperator(String conf, RowType inputRowType, @Nullable RowType resultRowType) {
        this.conf = conf == null ? "" : conf;
        this.inputRowType = Objects.requireNonNull(inputRowType, "inputRowType");
        this.resultRowType = resultRowType == null ? this.inputRowType : resultRowType;
    }

    // ------------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------------

    @Override
    public final void open() throws Exception {
        super.open();
        this.tcpConfig = ExternalRuntimeTcpConfig.from(conf);
        initializePayloadFields();
        openInternal();
    }

    @Override
    public final void processElement(StreamRecord<RowData> element) throws Exception {
        processElementInternal(element);
    }

    protected void processElementInternal(StreamRecord<RowData> element) throws Exception {
        final RowData outRow = processRow(element.getValue());
        output.collect(element.replace(outRow));
    }

    @Override
    public final void close() throws Exception {
        IOException error = null;
        try {
            closeInternal();
        } catch (IOException e) {
            error = e;
        }

        error = suppress(error, closeQuietly(socket));
        socket = null;

        super.close();

        if (error != null) {
            throw error;
        }
    }

    protected abstract Role role();

    protected abstract void openInternal() throws Exception;

    protected abstract RowData processRow(RowData inRow) throws Exception;

    protected abstract void closeInternal() throws Exception;

    // ------------------------------------------------------------------------
    // Shared schema + metadata initialization
    // ------------------------------------------------------------------------

    protected final void initializePayloadFields() {
        List<LogicalType> fieldTypes;
        LogicalTypeRoot[] payloadTargetRoots;

        final List<RowType.RowField> inputFields = inputRowType.getFields();
        final List<RowType.RowField> resultFields = resultRowType.getFields();
        final int fieldCount = inputFields.size();
        this.inputFieldCount = fieldCount;

        final List<Integer> indices = new ArrayList<>(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            indices.add(i);
        }
        this.payloadFieldIndices = indices;
        this.payloadFieldIndicesArray = toIntArray(indices);

        fieldTypes = indices.stream()
                .map(i -> inputFields.get(i).getType())
                .collect(Collectors.toList());

        this.payloadFieldNames = new ArrayList<>(indices.size());
        for (int idx : indices) {
            payloadFieldNames.add(inputFields.get(idx).getName());
        }

        final Map<Integer, LogicalType> argTypeByIndex = new HashMap<>();
        if (tcpConfig.getArgFieldTypes() != null && !tcpConfig.getArgFieldTypes().isEmpty()) {
            final ClassLoader cl = getRuntimeContext().getUserCodeClassLoader();

            if (tcpConfig.getArgFieldIndices() != null
                    && !tcpConfig.getArgFieldIndices().isEmpty()
                    && tcpConfig.getArgFieldIndices().size() == tcpConfig.getArgFieldTypes().size()) {

                for (int i = 0; i < tcpConfig.getArgFieldIndices().size(); i++) {
                    final Integer idx = tcpConfig.getArgFieldIndices().get(i);
                    final String typeString = tcpConfig.getArgFieldTypes().get(i);
                    if (idx == null || idx < 0 || idx >= fieldCount || typeString == null
                            || typeString.trim().isEmpty()) {
                        continue;
                    }
                    argTypeByIndex.put(idx, LogicalTypeParser.parse(typeString, cl));
                }

            } else if (tcpConfig.getArgFieldNames() != null
                    && !tcpConfig.getArgFieldNames().isEmpty()
                    && tcpConfig.getArgFieldNames().size() == tcpConfig.getArgFieldTypes().size()) {

                final Map<String, Integer> nameToIndex = new HashMap<>();
                for (int i = 0; i < fieldCount; i++) {
                    final String name = inputFields.get(i).getName();
                    if (name != null) {
                        nameToIndex.put(name.toLowerCase(Locale.ROOT), i);
                    }
                }

                for (int i = 0; i < tcpConfig.getArgFieldNames().size(); i++) {
                    final String name = tcpConfig.getArgFieldNames().get(i);
                    final String typeString = tcpConfig.getArgFieldTypes().get(i);
                    if (name == null || name.trim().isEmpty()) {
                        continue;
                    }
                    final Integer idx = nameToIndex.get(name.toLowerCase(Locale.ROOT));
                    if (idx == null || idx < 0 || idx >= fieldCount || typeString == null
                            || typeString.trim().isEmpty()) {
                        continue;
                    }
                    argTypeByIndex.put(idx, LogicalTypeParser.parse(typeString, cl));
                }
            }
        }

        this.payloadWriteTypes = new ArrayList<>(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            final LogicalType argType = argTypeByIndex.get(i);
            if (argType != null) {
                payloadWriteTypes.add(argType);
            } else {
                payloadWriteTypes.add(inputFields.get(i).getType());
            }
        }

        final int payloadSize = payloadFieldIndices.size();
        final WireType[] payloadWireTypes = new WireType[payloadSize];
        for (int i = 0; i < payloadSize; i++) {
            payloadWireTypes[i] = ExternalRuntimeBinaryCodec.wireTypeFor(payloadWriteTypes.get(i));
        }
        this.payloadWireTypes = payloadWireTypes;

        this.payloadSourceRoots = new LogicalTypeRoot[payloadSize];
        payloadTargetRoots = new LogicalTypeRoot[payloadSize];
        this.payloadTargetPrecision = new int[payloadSize];
        this.payloadTargetScale = new int[payloadSize];
        this.payloadSourcePrecision = new int[payloadSize];
        this.payloadSourceScale = new int[payloadSize];
        this.payloadTimestampPrecision = new int[payloadSize];

        for (int i = 0; i < payloadSize; i++) {
            final LogicalType sourceType = fieldTypes.get(i);
            final LogicalType targetType = payloadWriteTypes.get(i);

            payloadSourceRoots[i] = sourceType.getTypeRoot();
            payloadTargetRoots[i] = targetType.getTypeRoot();

            payloadTargetPrecision[i] = -1;
            payloadTargetScale[i] = -1;
            payloadSourcePrecision[i] = -1;
            payloadSourceScale[i] = -1;
            payloadTimestampPrecision[i] = -1;

            if (payloadTargetRoots[i] == LogicalTypeRoot.DECIMAL) {
                final DecimalType dt = (DecimalType) targetType;
                payloadTargetPrecision[i] = dt.getPrecision();
                payloadTargetScale[i] = dt.getScale();
            }
            if (payloadSourceRoots[i] == LogicalTypeRoot.DECIMAL) {
                final DecimalType dt = (DecimalType) sourceType;
                payloadSourcePrecision[i] = dt.getPrecision();
                payloadSourceScale[i] = dt.getScale();
            }
            if (isTimestampRoot(payloadSourceRoots[i])) {
                if (sourceType instanceof TimestampType) {
                    payloadTimestampPrecision[i] = ((TimestampType) sourceType).getPrecision();
                } else if (sourceType instanceof LocalZonedTimestampType) {
                    payloadTimestampPrecision[i] = ((LocalZonedTimestampType) sourceType).getPrecision();
                }
            }
        }

        this.resultFieldIndices = new ArrayList<>(resultFields.size());
        for (int i = 0; i < resultFields.size(); i++) {
            resultFieldIndices.add(i);
        }
        this.resultFieldTypes = resultFieldIndices.stream()
                .map(i -> resultFields.get(i).getType())
                .collect(Collectors.toList());

        final Map<Integer, LogicalType> udfTypeByIndex = new HashMap<>();
        final ClassLoader cl = getRuntimeContext().getUserCodeClassLoader();
        boolean mappedByIndex = false;

        if (tcpConfig.getResultFieldIndices() != null
                && !tcpConfig.getResultFieldIndices().isEmpty()
                && tcpConfig.getResultUdfFieldTypes() != null
                && tcpConfig.getResultFieldIndices().size() == tcpConfig.getResultUdfFieldTypes().size()) {
            for (int i = 0; i < tcpConfig.getResultFieldIndices().size(); i++) {
                final Integer idx = tcpConfig.getResultFieldIndices().get(i);
                final String typeString = tcpConfig.getResultUdfFieldTypes().get(i);
                if (idx == null || idx < 0 || idx >= fieldCount) {
                    continue;
                }
                if (typeString == null || typeString.trim().isEmpty()) {
                    continue;
                }
                udfTypeByIndex.put(idx, LogicalTypeParser.parse(typeString, cl));
                mappedByIndex = true;
            }
        } else if (tcpConfig.getResultUdfFieldIndices() != null
                && tcpConfig.getResultUdfFieldTypes() != null
                && tcpConfig.getResultUdfFieldIndices().size() == tcpConfig.getResultUdfFieldTypes().size()) {
            for (int i = 0; i < tcpConfig.getResultUdfFieldIndices().size(); i++) {
                final Integer idx = tcpConfig.getResultUdfFieldIndices().get(i);
                final String typeString = tcpConfig.getResultUdfFieldTypes().get(i);
                if (idx == null || idx < 0 || idx >= fieldCount) {
                    continue;
                }
                if (typeString == null || typeString.trim().isEmpty()) {
                    continue;
                }
                udfTypeByIndex.put(idx, LogicalTypeParser.parse(typeString, cl));
                mappedByIndex = true;
            }
        }

        if (!mappedByIndex
                && tcpConfig.getResultFieldNames() != null
                && tcpConfig.getResultUdfFieldTypes() != null
                && tcpConfig.getResultFieldNames().size() == tcpConfig.getResultUdfFieldTypes().size()) {
            final Map<String, Integer> nameToIndex = new HashMap<>();
            for (int i = 0; i < fieldCount; i++) {
                final String name = inputFields.get(i).getName();
                if (name != null) {
                    nameToIndex.put(name.toLowerCase(Locale.ROOT), i);
                }
            }
            for (int i = 0; i < tcpConfig.getResultFieldNames().size(); i++) {
                final String name = tcpConfig.getResultFieldNames().get(i);
                final String typeString = tcpConfig.getResultUdfFieldTypes().get(i);
                if (name == null || name.trim().isEmpty()) {
                    continue;
                }
                final Integer idx = nameToIndex.get(name.toLowerCase(Locale.ROOT));
                if (idx == null || idx < 0 || idx >= fieldCount || typeString == null || typeString.trim().isEmpty()) {
                    continue;
                }
                udfTypeByIndex.put(idx, LogicalTypeParser.parse(typeString, cl));
            }
        }

        this.postFieldTypes = new ArrayList<>(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            final LogicalType udfType = udfTypeByIndex.get(i);
            if (udfType != null) {
                postFieldTypes.add(udfType);
            } else {
                postFieldTypes.add(inputFields.get(i).getType());
            }
        }

        this.resultReadTypes = new ArrayList<>(resultFieldIndices.size());
        for (int i = 0; i < resultFieldIndices.size(); i++) {
            final int idx = resultFieldIndices.get(i);
            this.resultReadTypes.add(postFieldTypes.get(idx));
        }

        final WireType[] resultWireTypes = new WireType[resultReadTypes.size()];
        for (int i = 0; i < resultReadTypes.size(); i++) {
            resultWireTypes[i] = ExternalRuntimeBinaryCodec.wireTypeFor(resultReadTypes.get(i));
        }
        this.resultWireTypes = resultWireTypes;

        this.resultFieldGetters = new RowData.FieldGetter[resultFieldTypes.size()];
        for (int i = 0; i < resultFieldTypes.size(); i++) {
            resultFieldGetters[i] = RowData.createFieldGetter(resultFieldTypes.get(i), i);
        }
        this.fullRowFieldGetters = new RowData.FieldGetter[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            fullRowFieldGetters[i] = RowData.createFieldGetter(inputFields.get(i).getType(), i);
        }

        this.resultPosByInputIndex = new int[fieldCount];
        Arrays.fill(resultPosByInputIndex, -1);
        boolean replacesAll = resultFieldIndices.size() == fieldCount;
        for (int i = 0; i < resultFieldIndices.size(); i++) {
            final int idx = resultFieldIndices.get(i);
            if (idx < 0 || idx >= fieldCount) {
                replacesAll = false;
                continue;
            }
            resultPosByInputIndex[idx] = i;
            if (replacesAll && idx != i) {
                replacesAll = false;
            }
        }
        this.resultReplacesAllFields = replacesAll;
    }

    protected static int[] toIntArray(List<Integer> ints) {
        final int[] out = new int[ints.size()];
        for (int i = 0; i < ints.size(); i++) {
            out[i] = ints.get(i);
        }
        return out;
    }

    protected final String buildConfigJson() {
        if (role() == Role.POST && tcpConfig.isPostRoleOnly()) {
            return "{\"role\":\"post\"}";
        }

        final Map<String, Object> root = new LinkedHashMap<>();
        root.put("role", role() == Role.PRE ? "pre" : "post");

        if (notEmpty(tcpConfig.getFunctionClass())) {
            root.put("functionClass", tcpConfig.getFunctionClass());
        }
        if (notEmpty(tcpConfig.getFunctionKind())) {
            root.put("functionKind", tcpConfig.getFunctionKind());
        }

        root.put("externalOnly", true);
        root.put("reorderResponses", tcpConfig.isReorderResponses());
        root.put("countedResponses", true);
        if (tcpConfig.getBatchSize() > 0) {
            root.put("batchSize", tcpConfig.getBatchSize());
        }

        final List<Map<String, Object>> functionArgs = buildFunctionArgs();
        if (!functionArgs.isEmpty()) {
            root.put("functionArgs", functionArgs);
        }

        final List<Map<String, Object>> functionResults = buildFunctionResults();
        if (!functionResults.isEmpty()) {
            root.put("functionResults", functionResults);
        }

        root.put("preFields", buildPreFields());
        root.put("postFields", buildPostFields());
        root.put("postHeaderFields", buildPostHeaderFields());

        try {
            return MAPPER.writeValueAsString(root);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize external runtime config JSON", e);
        }
    }

    private static boolean notEmpty(String s) {
        return s != null && !s.isEmpty();
    }

    private List<Map<String, Object>> buildFunctionArgs() {
        final int maxArgs = Math.max(
                tcpConfig.getArgFieldIndices().size(),
                Math.max(tcpConfig.getArgFieldNames().size(), tcpConfig.getArgFieldTypes().size()));
        if (maxArgs == 0) {
            return List.of();
        }

        final List<Map<String, Object>> out = new ArrayList<>(maxArgs);
        for (int i = 0; i < maxArgs; i++) {
            final Map<String, Object> arg = new LinkedHashMap<>(2);
            if (i < tcpConfig.getArgFieldNames().size()) {
                arg.put("name", tcpConfig.getArgFieldNames().get(i));
            }
            if (i < tcpConfig.getArgFieldTypes().size()) {
                arg.put("type", tcpConfig.getArgFieldTypes().get(i));
            }
            out.add(arg);
        }
        return out;
    }

    private List<Map<String, Object>> buildFunctionResults() {
        final int maxResults = Math.max(
                tcpConfig.getResultFieldIndices().size(),
                Math.max(
                        tcpConfig.getResultFieldNames().size(),
                        Math.max(
                                tcpConfig.getResultFieldTypes().size(),
                                tcpConfig.getResultUdfFieldTypes().size())));
        if (maxResults == 0) {
            return List.of();
        }

        final List<Map<String, Object>> out = new ArrayList<>(maxResults);
        for (int i = 0; i < maxResults; i++) {
            final Map<String, Object> res = new LinkedHashMap<>(3);

            if (i < tcpConfig.getResultFieldNames().size()) {
                res.put("outputName", tcpConfig.getResultFieldNames().get(i));
            }

            if (i < tcpConfig.getResultUdfFieldTypes().size()) {
                res.put("outputType", tcpConfig.getResultUdfFieldTypes().get(i));
            } else if (i < tcpConfig.getResultFieldTypes().size()) {
                res.put("outputType", tcpConfig.getResultFieldTypes().get(i));
            }

            final int udfFieldIndex = i < tcpConfig.getResultUdfFieldIndices().size()
                    ? tcpConfig.getResultUdfFieldIndices().get(i)
                    : -1;
            if (udfFieldIndex >= 0) {
                res.put("udfFieldIndex", udfFieldIndex);
            }

            out.add(res);
        }
        return out;
    }

    private List<Map<String, Object>> buildPreFields() {
        final List<RowType.RowField> fields = inputRowType.getFields();
        final int extra = 2;
        final List<Map<String, Object>> out = new ArrayList<>(
                extra + (payloadFieldIndices == null ? 0 : payloadFieldIndices.size()));

        out.add(fieldEntry("__op", "INT32"));

        out.add(fieldEntry("__rowId", "INT64"));

        for (int i = 0; i < payloadFieldIndices.size(); i++) {
            final int idx = payloadFieldIndices.get(i);
            final RowType.RowField field = fields.get(idx);
            final LogicalType wireType = payloadWriteTypes != null && idx < payloadWriteTypes.size()
                    ? payloadWriteTypes.get(idx)
                    : field.getType();

            final String name = resolvePayloadFieldName(i, field);
            out.add(fieldEntry(name, wireTypeFor(wireType)));
        }

        return out;
    }

    private List<Map<String, Object>> buildPostFields() {
        final List<RowType.RowField> fields = inputRowType.getFields();
        final int extra = 2;
        final List<Map<String, Object>> out = new ArrayList<>(
                extra + (resultFieldIndices == null ? 0 : resultFieldIndices.size()));

        out.add(fieldEntry("__op", "INT32"));

        out.add(fieldEntry("__rowId", "INT64"));

        for (int i = 0; i < resultFieldIndices.size(); i++) {
            final int idx = resultFieldIndices.get(i);
            final RowType.RowField field = fields.get(idx);
            final LogicalType postType = postFieldTypes != null && idx < postFieldTypes.size()
                    ? postFieldTypes.get(idx)
                    : field.getType();

            out.add(fieldEntry(field.getName(), wireTypeFor(postType)));
        }

        return out;
    }

    private List<Map<String, Object>> buildPostHeaderFields() {
        final int extra = 2;
        final List<Map<String, Object>> out = new ArrayList<>(extra + 1);

        out.add(fieldEntry("__op", "INT32"));
        out.add(fieldEntry("__rowId", "INT64"));
        out.add(fieldEntry("__count", "INT32"));

        return out;
    }

    private static Map<String, Object> fieldEntry(String name, String wireType) {
        final Map<String, Object> m = new LinkedHashMap<>(2);
        m.put("name", name);
        m.put("wireType", wireType);
        return m;
    }

    private String resolvePayloadFieldName(int payloadIndex, RowType.RowField fallbackField) {
        if (payloadFieldNames != null
                && payloadIndex >= 0
                && payloadIndex < payloadFieldNames.size()) {
            final String name = payloadFieldNames.get(payloadIndex);
            if (name != null && !name.isEmpty()) {
                return name;
            }
        }
        return fallbackField.getName();
    }

    protected static String wireTypeFor(LogicalType type) {
        if (type == null) {
            return "NIL";
        }
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return "BOOL";
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return "INT32";
            case BIGINT:
                return "INT64";
            case FLOAT:
                return "FLOAT32";
            case DOUBLE:
                return "FLOAT64";
            case CHAR:
            case VARCHAR:
                return "STRING";
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return "TIMESTAMP_MILLIS";
            case DECIMAL: {
                final DecimalType dt = (DecimalType) type;
                return dt.getPrecision() <= 18 ? "DECIMAL_UNSCALED_I64" : "DECIMAL_UNSCALED_BYTES";
            }
            case BINARY:
            case VARBINARY:
                return "BYTES";
            default:
                return "NIL";
        }
    }

    protected static void writeLengthPrefixedJson(OutputStream out, String json) throws IOException {
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        final DataOutputStream dos = new DataOutputStream(out);
        dos.writeInt(bytes.length);
        dos.write(bytes);
        dos.flush();
    }

    protected static String jsonEscape(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    protected static boolean isStringRoot(LogicalTypeRoot root) {
        return root == LogicalTypeRoot.CHAR || root == LogicalTypeRoot.VARCHAR;
    }

    protected static boolean isTimestampRoot(LogicalTypeRoot root) {
        return root == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
                || root == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
    }

    protected static Socket connectSocket(String host, int port, int connectTimeoutMs) throws IOException {
        final Socket socket = new Socket();
        socket.setTcpNoDelay(true);
        socket.connect(new InetSocketAddress(host, port), connectTimeoutMs);
        return socket;
    }

    protected static IOException closeQuietly(AutoCloseable c) {
        if (c == null) {
            return null;
        }
        try {
            c.close();
            return null;
        } catch (IOException e) {
            return e;
        } catch (Exception e) {
            return new IOException(e);
        }
    }

    protected static IOException flushAndClose(Object o) {
        if (o == null) {
            return null;
        }
        IOException err = null;
        try {
            if (o instanceof Flushable) {
                ((Flushable) o).flush();
            }
        } catch (IOException e) {
            err = e;
        }
        if (o instanceof AutoCloseable) {
            err = suppress(err, closeQuietly((AutoCloseable) o));
        }
        return err;
    }

    protected static IOException suppress(IOException existing, IOException next) {
        if (existing == null) {
            return next;
        }
        if (next != null) {
            existing.addSuppressed(next);
        }
        return existing;
    }
}
