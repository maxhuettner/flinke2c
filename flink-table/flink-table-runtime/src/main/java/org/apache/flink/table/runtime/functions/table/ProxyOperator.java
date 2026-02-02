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

package org.apache.flink.table.runtime.functions.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.types.RowKind;

import org.apache.flink.table.runtime.functions.table.ProxyBinaryCodec.WireType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A proxy operator that forwards records through an external TCP process.
 *
 * <p>PRE sends input rows (binary framed format) to an external process.
 * POST receives processed rows (binary framed format) and emits them downstream.
 *
 * <p>PRE sends a JSON config preamble (length-prefixed, big-endian int32 length)
 * before the binary framed stream begins. POST can send a minimal preamble (role only)
 * if configured.
 */
@Internal
public class ProxyOperator extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(ProxyOperator.class);
    private static final long serialVersionUID = 1L;

    public enum Side {
        PRE,
        POST
    }

    private final String conf;
    private final Side side;
    private final RowType inputRowType;
    private final RowType resultRowType;

    private transient ProxyTcpConfig tcpConfig;

    private transient List<Integer> payloadFieldIndices;
    private transient int[] payloadFieldIndicesArray;
    private transient List<String> payloadFieldNames;
    private transient List<LogicalType> payloadWriteTypes;
    private transient LogicalTypeRoot[] payloadSourceRoots;
    private transient int[] payloadTargetPrecision;
    private transient int[] payloadTargetScale;
    private transient int[] payloadSourcePrecision;
    private transient int[] payloadSourceScale;
    private transient int[] payloadTimestampPrecision;
    private transient WireType[] payloadWireTypes;

    private transient List<Integer> resultFieldIndices;
    private transient List<LogicalType> resultFieldTypes;
    private transient List<LogicalType> resultReadTypes;
    private transient List<LogicalType> postFieldTypes;
    private transient WireType[] resultWireTypes;

    private transient RowData.FieldGetter[] resultFieldGetters;
    private transient RowData.FieldGetter[] fullRowFieldGetters;

    private transient GenericRowData insertPlaceholder;
    private transient GenericRowData updateAfterPlaceholder;
    private transient GenericRowData updateBeforePlaceholder;
    private transient GenericRowData deletePlaceholder;
    private transient int inputFieldCount;

    // shared socket
    private transient Socket socket;

    // PRE (binary writer + batch state)
    private transient BufferedOutputStream out;
    private transient ProxyBinaryCodec codec;
    private transient int batchMaxRows;
    private transient int batchRowIndex;
    private transient long nextRowId;
    private transient boolean flushTimerScheduled;

    // POST (binary reader)
    private transient BufferedInputStream in;
    private transient long expectedRowId;
    private transient Map<Long, RowData> reorderBuffer;

    public ProxyOperator(String conf, Side side, RowType rowType) {
        this(conf, side, rowType, rowType);
    }

    public ProxyOperator(String conf, Side side, RowType inputRowType, @Nullable RowType resultRowType) {
        this.conf = conf == null ? "" : conf;
        this.side = side == null ? Side.PRE : side;
        this.inputRowType = Objects.requireNonNull(inputRowType, "inputRowType");
        this.resultRowType = resultRowType == null ? this.inputRowType : resultRowType;
    }

    @Override
    public void open() throws Exception {
        super.open();

        this.tcpConfig = ProxyTcpConfig.from(conf);
        initializePayloadFields();

        if (side == Side.PRE) {
            openPre();
        } else {
            openPost();
        }
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        if (side == Side.PRE) {
            appendRowToBinary(element.getValue());
            final RowData placeholder = createPlaceholderRow(element.getValue().getRowKind());
            output.collect(element.replace(placeholder));
        } else {
            final RowData proxyRow;
            if (tcpConfig.reorderResponses) {
                proxyRow = readNextOrderedRow(element.getValue().getRowKind());
            } else {
                proxyRow = readNextRow(element.getValue().getRowKind());
            }

            // IMPORTANT: merge returned fields into the incoming (placeholder) row
            // so the output row always matches the full input row arity.
            final RowData outRow = mergeProxyRow(element.getValue(), proxyRow);
            output.collect(element.replace(outRow));
        }
    }

    private void initializePayloadFields() {
        List<LogicalType> fieldTypes;
        LogicalTypeRoot[] payloadTargetRoots;
        final List<RowType.RowField> inputFields = inputRowType.getFields();
        final List<RowType.RowField> resultFields = resultRowType.getFields();
        final int fieldCount = inputFields.size();
        this.inputFieldCount = fieldCount;

        // payload indices (currently: all fields)
        final List<Integer> indices = new ArrayList<>(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            indices.add(i);
        }
        this.payloadFieldIndices = indices;
        this.payloadFieldIndicesArray = toIntArray(indices);

        fieldTypes =
                indices.stream()
                        .map(i -> inputFields.get(i).getType())
                        .collect(Collectors.toList());

        this.payloadFieldNames = new ArrayList<>(indices.size());
        for (int idx : indices) {
            payloadFieldNames.add(inputFields.get(idx).getName());
        }

        // optional override arg types
        final Map<Integer, LogicalType> argTypeByIndex = new HashMap<>();
        if (tcpConfig.argFieldTypes != null && !tcpConfig.argFieldTypes.isEmpty()) {
            final ClassLoader cl = getRuntimeContext().getUserCodeClassLoader();

            if (tcpConfig.argFieldIndices != null
                    && !tcpConfig.argFieldIndices.isEmpty()
                    && tcpConfig.argFieldIndices.size() == tcpConfig.argFieldTypes.size()) {

                for (int i = 0; i < tcpConfig.argFieldIndices.size(); i++) {
                    final Integer idx = tcpConfig.argFieldIndices.get(i);
                    final String typeString = tcpConfig.argFieldTypes.get(i);
                    if (idx == null || idx < 0 || idx >= fieldCount) {
                        continue;
                    }
                    if (typeString == null || typeString.trim().isEmpty()) {
                        continue;
                    }
                    argTypeByIndex.put(idx, LogicalTypeParser.parse(typeString, cl));
                }

            } else if (tcpConfig.argFieldNames != null
                    && !tcpConfig.argFieldNames.isEmpty()
                    && tcpConfig.argFieldNames.size() == tcpConfig.argFieldTypes.size()) {

                final Map<String, Integer> nameToIndex = new HashMap<>();
                for (int i = 0; i < fieldCount; i++) {
                    final String name = inputFields.get(i).getName();
                    if (name != null) {
                        nameToIndex.put(name.toLowerCase(Locale.ROOT), i);
                    }
                }

                for (int i = 0; i < tcpConfig.argFieldNames.size(); i++) {
                    final String name = tcpConfig.argFieldNames.get(i);
                    final String typeString = tcpConfig.argFieldTypes.get(i);
                    if (name == null || name.trim().isEmpty()) {
                        continue;
                    }
                    final Integer idx = nameToIndex.get(name.toLowerCase(Locale.ROOT));
                    if (idx == null || idx < 0 || idx >= fieldCount) {
                        continue;
                    }
                    if (typeString == null || typeString.trim().isEmpty()) {
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

        // PRE: wire types for payload fields (excluding __op/__rowId)
        final int payloadSize = payloadFieldIndices.size();
        final WireType[] payloadWireTypes = new WireType[payloadSize];
        for (int i = 0; i < payloadSize; i++) {
            payloadWireTypes[i] = ProxyBinaryCodec.wireTypeFor(payloadWriteTypes.get(i));
        }
        this.payloadWireTypes = payloadWireTypes;

        // caches for fast decimal/timestamp paths used by codec writer
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
                    payloadTimestampPrecision[i] =
                            ((LocalZonedTimestampType) sourceType).getPrecision();
                }
            }
        }

        // result fields (POST output schema)
        this.resultFieldIndices = new ArrayList<>(resultFields.size());
        for (int i = 0; i < resultFields.size(); i++) {
            resultFieldIndices.add(i);
        }
        this.resultFieldTypes =
                resultFieldIndices.stream()
                        .map(i -> resultFields.get(i).getType())
                        .collect(Collectors.toList());

        // optional override udf types
        final Map<Integer, LogicalType> udfTypeByIndex = new HashMap<>();
        final ClassLoader cl = getRuntimeContext().getUserCodeClassLoader();
        boolean mappedByIndex = false;

        if (tcpConfig.resultFieldIndices != null
                && !tcpConfig.resultFieldIndices.isEmpty()
                && tcpConfig.resultUdfFieldTypes != null
                && tcpConfig.resultFieldIndices.size() == tcpConfig.resultUdfFieldTypes.size()) {
            for (int i = 0; i < tcpConfig.resultFieldIndices.size(); i++) {
                final Integer idx = tcpConfig.resultFieldIndices.get(i);
                final String typeString = tcpConfig.resultUdfFieldTypes.get(i);
                if (idx == null || idx < 0 || idx >= fieldCount) {
                    continue;
                }
                if (typeString == null || typeString.trim().isEmpty()) {
                    continue;
                }
                udfTypeByIndex.put(idx, LogicalTypeParser.parse(typeString, cl));
                mappedByIndex = true;
            }
        } else if (tcpConfig.resultUdfFieldIndices != null
                && tcpConfig.resultUdfFieldTypes != null
                && tcpConfig.resultUdfFieldIndices.size() == tcpConfig.resultUdfFieldTypes.size()) {
            for (int i = 0; i < tcpConfig.resultUdfFieldIndices.size(); i++) {
                final Integer idx = tcpConfig.resultUdfFieldIndices.get(i);
                final String typeString = tcpConfig.resultUdfFieldTypes.get(i);
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
                && tcpConfig.resultFieldNames != null
                && tcpConfig.resultUdfFieldTypes != null
                && tcpConfig.resultFieldNames.size() == tcpConfig.resultUdfFieldTypes.size()) {
            final Map<String, Integer> nameToIndex = new HashMap<>();
            for (int i = 0; i < fieldCount; i++) {
                final String name = inputFields.get(i).getName();
                if (name != null) {
                    nameToIndex.put(name.toLowerCase(Locale.ROOT), i);
                }
            }
            for (int i = 0; i < tcpConfig.resultFieldNames.size(); i++) {
                final String name = tcpConfig.resultFieldNames.get(i);
                final String typeString = tcpConfig.resultUdfFieldTypes.get(i);
                if (name == null || name.trim().isEmpty()) {
                    continue;
                }
                final Integer idx = nameToIndex.get(name.toLowerCase(Locale.ROOT));
                if (idx == null || idx < 0 || idx >= fieldCount) {
                    continue;
                }
                if (typeString == null || typeString.trim().isEmpty()) {
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

        // IMPORTANT: align resultReadTypes with resultFieldIndices
        this.resultReadTypes = new ArrayList<>(resultFieldIndices.size());
        for (int i = 0; i < resultFieldIndices.size(); i++) {
            final int idx = resultFieldIndices.get(i);
            this.resultReadTypes.add(postFieldTypes.get(idx));
        }

        // wire types for POST read
        final WireType[] resultWireTypes = new WireType[resultReadTypes.size()];
        for (int i = 0; i < resultReadTypes.size(); i++) {
            resultWireTypes[i] = ProxyBinaryCodec.wireTypeFor(resultReadTypes.get(i));
        }
        this.resultWireTypes = resultWireTypes;

        // field getters for merge
        this.resultFieldGetters = new RowData.FieldGetter[resultFieldTypes.size()];
        for (int i = 0; i < resultFieldTypes.size(); i++) {
            resultFieldGetters[i] = RowData.createFieldGetter(resultFieldTypes.get(i), i);
        }
        this.fullRowFieldGetters = new RowData.FieldGetter[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            fullRowFieldGetters[i] = RowData.createFieldGetter(inputFields.get(i).getType(), i);
        }
    }

    private RowData mergeProxyRow(RowData baseRow, RowData proxyRow) {
        final int fieldCount = inputRowType.getFieldCount();
        final GenericRowData outRow = new GenericRowData(fieldCount);
        outRow.setRowKind(proxyRow.getRowKind());

        for (int i = 0; i < fieldCount; i++) {
            outRow.setField(i, fullRowFieldGetters[i].getFieldOrNull(baseRow));
        }
        for (int i = 0; i < resultFieldIndices.size(); i++) {
            final int targetIndex = resultFieldIndices.get(i);
            outRow.setField(targetIndex, resultFieldGetters[i].getFieldOrNull(proxyRow));
        }
        return outRow;
    }

    private RowData createPlaceholderRow(RowKind kind) {
        switch (kind) {
            case INSERT:
                if (insertPlaceholder == null) {
                    insertPlaceholder = new GenericRowData(inputFieldCount);
                    insertPlaceholder.setRowKind(RowKind.INSERT);
                }
                return insertPlaceholder;
            case UPDATE_AFTER:
                if (updateAfterPlaceholder == null) {
                    updateAfterPlaceholder = new GenericRowData(inputFieldCount);
                    updateAfterPlaceholder.setRowKind(RowKind.UPDATE_AFTER);
                }
                return updateAfterPlaceholder;
            case UPDATE_BEFORE:
                if (updateBeforePlaceholder == null) {
                    updateBeforePlaceholder = new GenericRowData(inputFieldCount);
                    updateBeforePlaceholder.setRowKind(RowKind.UPDATE_BEFORE);
                }
                return updateBeforePlaceholder;
            case DELETE:
                if (deletePlaceholder == null) {
                    deletePlaceholder = new GenericRowData(inputFieldCount);
                    deletePlaceholder.setRowKind(RowKind.DELETE);
                }
                return deletePlaceholder;
            default:
                final GenericRowData row = new GenericRowData(inputFieldCount);
                row.setRowKind(kind);
                return row;
        }
    }

    @Override
    public void close() throws Exception {
        IOException error = null;

        try {
            if (side == Side.PRE) {
                closePre();
            } else {
                closePost();
            }
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

    // ------------------------------------------------------------------------
    // PRE
    // ------------------------------------------------------------------------

    private void openPre() throws Exception {
        final ProxyTcpConfig.ProxyEndpoint proxy = tcpConfig.selectedProxy;
        final int port = proxy.sendPort;
        this.socket = connectSocket(proxy.host, port, tcpConfig.connectTimeoutMs);
        this.out = new BufferedOutputStream(socket.getOutputStream(), tcpConfig.bufferSize);

        // preamble
        final String configJson = buildConfigJson(side);
        writeLengthPrefixedJson(out, configJson);

        this.batchMaxRows = tcpConfig.batchMaxRows;
        this.batchRowIndex = 0;
        this.nextRowId = 0L;

        // codec (writer configured; reader null)
        this.codec = new ProxyBinaryCodec(
                tcpConfig.reorderResponses,
                tcpConfig.maxFrameSize,
                payloadWireTypes,
                payloadWriteTypes.toArray(new LogicalType[0]),
                payloadSourceRoots,
                payloadTargetPrecision,
                payloadTargetScale,
                payloadSourcePrecision,
                payloadSourceScale,
                payloadTimestampPrecision,
                null, null, null);

        this.flushTimerScheduled = false;
        scheduleFlushTimerIfNeeded();

        LOG.info(
                "ProxyOperator {} connected to {}:{} (rowType={}, batchMaxRows={}, sentConfigBytes={})",
                side, proxy.host, port, inputRowType, batchMaxRows,
                configJson.getBytes(StandardCharsets.UTF_8).length);
    }

    private void appendRowToBinary(RowData row) throws IOException {
        if (batchRowIndex >= batchMaxRows) {
            flushBinaryBatch();
        }

        // framed row
        codec.writeFramedRow(out, row, payloadFieldIndicesArray, nextRowId);
        if (tcpConfig.reorderResponses) {
            nextRowId++;
        }

        batchRowIndex++;

        if (tcpConfig.flushOnWrite) {
            flushBinaryBatch();
        }
    }

    private static int[] toIntArray(List<Integer> ints) {
        final int[] out = new int[ints.size()];
        for (int i = 0; i < ints.size(); i++) {
            out[i] = ints.get(i);
        }
        return out;
    }

    private void flushBinaryBatch() throws IOException {
        if (batchRowIndex <= 0) {
            return;
        }
        out.flush();
        batchRowIndex = 0;
    }

    private void closePre() throws IOException {
        IOException error = null;

        flushTimerScheduled = false;
        error = suppress(error, tryFlushRemaining());

        error = suppress(error, flushAndClose(out));
        out = null;

        codec = null;

        if (error != null) {
            throw error;
        }
    }

    private IOException tryFlushRemaining() {
        try {
            flushBinaryBatch();
            return null;
        } catch (IOException e) {
            return e;
        }
    }

    private void scheduleFlushTimerIfNeeded() {
        if (side != Side.PRE) {
            return;
        }
        if (tcpConfig.flushIntervalMs <= 0) {
            return;
        }
        if (flushTimerScheduled) {
            return;
        }
        flushTimerScheduled = true;
        final long now = getProcessingTimeService().getCurrentProcessingTime();
        getProcessingTimeService().registerTimer(now + tcpConfig.flushIntervalMs, this::onFlushTimer);
    }

    private void onFlushTimer(long timestamp) throws Exception {
        flushTimerScheduled = false;
        if (side != Side.PRE) {
            return;
        }
        if (out == null || codec == null) {
            return;
        }
        if (batchRowIndex > 0) {
            flushBinaryBatch();
        }
        scheduleFlushTimerIfNeeded();
    }

    // ------------------------------------------------------------------------
    // POST
    // ------------------------------------------------------------------------

    private void openPost() throws IOException {
        final ProxyTcpConfig.ProxyEndpoint proxy = tcpConfig.selectedProxy;
        final int port = proxy.receivePort;
        this.socket = connectSocket(proxy.host, port, tcpConfig.connectTimeoutMs);

        if (tcpConfig.readTimeoutMs > 0) {
            socket.setSoTimeout(tcpConfig.readTimeoutMs);
        }

        this.in = new BufferedInputStream(socket.getInputStream(), tcpConfig.bufferSize);

        // send preamble
        final BufferedOutputStream postOut =
                new BufferedOutputStream(socket.getOutputStream(), tcpConfig.bufferSize);
        final String configJson = buildConfigJson(side);
        writeLengthPrefixedJson(postOut, configJson);

        // codec (reader configured; writer null)
        this.codec = new ProxyBinaryCodec(
                tcpConfig.reorderResponses,
                tcpConfig.maxFrameSize,
                null, null, null, null, null, null, null, null,
                resultWireTypes,
                resultReadTypes.toArray(new LogicalType[0]),
                resultFieldTypes.toArray(new LogicalType[0]));

        this.expectedRowId = 0L;
        this.reorderBuffer = tcpConfig.reorderResponses ? new HashMap<>() : null;

        LOG.info(
                "ProxyOperator {} connected to {}:{} (rowType={}, sentConfigBytes={})",
                side, proxy.host, port, inputRowType,
                configJson.getBytes(StandardCharsets.UTF_8).length);
    }

    private RowData readNextRow(RowKind fallbackKind) throws IOException {
        return readNextRowWithId(fallbackKind).row;
    }

    private RowWithId readNextRowWithId(RowKind fallbackKind) throws IOException {
        if (codec == null || in == null) {
            throw new IOException("ProxyOperator codec not initialized");
        }

        final ProxyBinaryCodec.RowWithId decoded = codec.readFramedRow(in, fallbackKind);

        final int fieldCount = resultFieldTypes.size();
        final GenericRowData outRow = new GenericRowData(fieldCount);
        outRow.setRowKind(decoded.kind);

        for (int i = 0; i < fieldCount; i++) {
            outRow.setField(i, decoded.fields[i]);
        }

        return new RowWithId(decoded.rowId, outRow);
    }

    private RowData readNextOrderedRow(RowKind fallbackKind) throws IOException {
        final long targetId = expectedRowId++;
        RowData ready = reorderBuffer.remove(targetId);
        while (ready == null) {
            final RowWithId next = readNextRowWithId(fallbackKind);
            if (next.rowId == targetId) {
                ready = next.row;
            } else {
                reorderBuffer.put(next.rowId, next.row);
                if (reorderBuffer.size() > tcpConfig.reorderMaxBuffer) {
                    throw new IOException(
                            "ProxyOperator reorder buffer exceeded "
                                    + tcpConfig.reorderMaxBuffer
                                    + " entries; increase reorderMax or enforce ordered responses.");
                }
            }
        }
        return ready;
    }

    private static final class RowWithId {
        private final long rowId;
        private final RowData row;

        private RowWithId(long rowId, RowData row) {
            this.rowId = rowId;
            this.row = row;
        }
    }

    private void closePost() throws IOException {
        IOException error = null;

        codec = null;

        error = suppress(error, closeQuietly(in));
        in = null;

        if (error != null) {
            throw error;
        }
    }

    // ------------------------------------------------------------------------
    // Config JSON preamble
    // ------------------------------------------------------------------------

    private String buildConfigJson(Side side) {
        if (side == Side.POST && tcpConfig.postRoleOnly) {
            return "{\"role\":\"post\"}";
        }

        // minimal JSON (no dependency). Escape strings.
        final StringBuilder sb = new StringBuilder(128);
        sb.append('{')
                .append("\"role\":\"").append(side == Side.PRE ? "pre" : "post").append('"');
        if (tcpConfig.functionClass != null && !tcpConfig.functionClass.isEmpty()) {
            sb.append(",\"functionClass\":\"")
                    .append(jsonEscape(tcpConfig.functionClass))
                    .append('"');
        }
        if (tcpConfig.functionKind != null && !tcpConfig.functionKind.isEmpty()) {
            sb.append(",\"functionKind\":\"")
                    .append(jsonEscape(tcpConfig.functionKind))
                    .append('"');
        }
        sb.append(",\"externalOnly\":true");
        sb.append(",\"reorderResponses\":").append(tcpConfig.reorderResponses);
        appendFunctionArgMetadata(sb);
        appendFunctionResultMetadata(sb);
        appendPreFieldMetadata(sb);
        appendPostFieldMetadata(sb);
        sb.append('}');
        return sb.toString();
    }

    private void appendFunctionArgMetadata(StringBuilder sb) {
        final int maxArgs = Math.max(
                tcpConfig.argFieldIndices.size(),
                Math.max(tcpConfig.argFieldNames.size(), tcpConfig.argFieldTypes.size()));
        if (maxArgs == 0) {
            return;
        }
        sb.append(",\"functionArgs\":[");
        for (int i = 0; i < maxArgs; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append('{');
            boolean wrote = false;
            if (i < tcpConfig.argFieldNames.size()) {
                sb.append("\"name\":\"")
                        .append(jsonEscape(tcpConfig.argFieldNames.get(i)))
                        .append('"');
                wrote = true;
            }
            if (i < tcpConfig.argFieldTypes.size()) {
                if (wrote) {
                    sb.append(',');
                }
                sb.append("\"type\":\"")
                        .append(jsonEscape(tcpConfig.argFieldTypes.get(i)))
                        .append('"');
            }
            sb.append('}');
        }
        sb.append(']');
    }

    private void appendFunctionResultMetadata(StringBuilder sb) {
        final int maxResults = Math.max(
                tcpConfig.resultFieldIndices.size(),
                Math.max(
                        tcpConfig.resultFieldNames.size(),
                        Math.max(
                                tcpConfig.resultFieldTypes.size(),
                                tcpConfig.resultUdfFieldTypes.size())));
        if (maxResults == 0) {
            return;
        }
        sb.append(",\"functionResults\":[");
        for (int i = 0; i < maxResults; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append('{');
            boolean wrote = false;
            if (i < tcpConfig.resultFieldNames.size()) {
                sb.append("\"outputName\":\"")
                        .append(jsonEscape(tcpConfig.resultFieldNames.get(i)))
                        .append('"');
                wrote = true;
            }
            if (i < tcpConfig.resultUdfFieldTypes.size()) {
                if (wrote) {
                    sb.append(',');
                }
                sb.append("\"outputType\":\"")
                        .append(jsonEscape(tcpConfig.resultUdfFieldTypes.get(i)))
                        .append('"');
                wrote = true;
            } else if (i < tcpConfig.resultFieldTypes.size()) {
                if (wrote) {
                    sb.append(',');
                }
                sb.append("\"outputType\":\"")
                        .append(jsonEscape(tcpConfig.resultFieldTypes.get(i)))
                        .append('"');
                wrote = true;
            }
            final int udfFieldIndex =
                    i < tcpConfig.resultUdfFieldIndices.size()
                            ? tcpConfig.resultUdfFieldIndices.get(i)
                            : -1;
            if (udfFieldIndex >= 0) {
                if (wrote) {
                    sb.append(',');
                }
                sb.append("\"udfFieldIndex\":").append(udfFieldIndex);
            }
            sb.append('}');
        }
        sb.append(']');
    }

    private void appendPreFieldMetadata(StringBuilder sb) {
        final List<RowType.RowField> fields = inputRowType.getFields();
        sb.append(",\"preFields\":[");

        // index 0: op
        sb.append("{\"name\":\"__op\",\"wireType\":\"INT32\"}");

        // index 1: rowId (if enabled)
        if (tcpConfig.reorderResponses) {
            sb.append(",{\"name\":\"__rowId\",\"wireType\":\"INT64\"}");
        }

        // payload fields
        for (int i = 0; i < payloadFieldIndices.size(); i++) {
            final int idx = payloadFieldIndices.get(i);
            final RowType.RowField field = fields.get(idx);
            final LogicalType wireType =
                    payloadWriteTypes != null && idx < payloadWriteTypes.size()
                            ? payloadWriteTypes.get(idx)
                            : field.getType();
            sb.append(",{\"name\":\"")
                    .append(jsonEscape(resolvePayloadFieldName(i, field)))
                    .append("\",\"wireType\":\"")
                    .append(wireTypeFor(wireType))
                    .append("\"}");
        }
        sb.append(']');
    }

    private void appendPostFieldMetadata(StringBuilder sb) {
        final List<RowType.RowField> fields = inputRowType.getFields();
        sb.append(",\"postFields\":[");

        // index 0: op
        sb.append("{\"name\":\"__op\",\"wireType\":\"INT32\"}");

        // index 1: rowId (if enabled)
        if (tcpConfig.reorderResponses) {
            sb.append(",{\"name\":\"__rowId\",\"wireType\":\"INT64\"}");
        }

        // result fields follow the order of resultFieldIndices / resultFieldTypes
        for (int i = 0; i < resultFieldIndices.size(); i++) {
            final int idx = resultFieldIndices.get(i);
            final RowType.RowField field = fields.get(idx);
            final LogicalType postType =
                    postFieldTypes != null && idx < postFieldTypes.size()
                            ? postFieldTypes.get(idx)
                            : field.getType();
            sb.append(",{\"name\":\"")
                    .append(jsonEscape(field.getName()))
                    .append("\",\"wireType\":\"")
                    .append(wireTypeFor(postType))
                    .append("\"}");
        }
        sb.append(']');
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

    private static String wireTypeFor(LogicalType type) {
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

    private static void writeLengthPrefixedJson(OutputStream out, String json) throws IOException {
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        final DataOutputStream dos = new DataOutputStream(out);
        dos.writeInt(bytes.length); // big-endian length
        dos.write(bytes);
        dos.flush();
    }

    private static String jsonEscape(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    // ------------------------------------------------------------------------
    // Type helpers + casting used by ProxyBinaryCodec decode path
    // ------------------------------------------------------------------------

    private static boolean isStringRoot(LogicalTypeRoot root) {
        return root == LogicalTypeRoot.CHAR || root == LogicalTypeRoot.VARCHAR;
    }

    private static boolean isTimestampRoot(LogicalTypeRoot root) {
        return root == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
                || root == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
    }

    static Object castValue(Object value, LogicalType sourceType, LogicalType targetType) {
        final LogicalTypeRoot sourceRoot = sourceType.getTypeRoot();
        final LogicalTypeRoot targetRoot = targetType.getTypeRoot();

        if (targetRoot == LogicalTypeRoot.DECIMAL) {
            final DecimalType dt = (DecimalType) targetType;
            if (value instanceof DecimalData) {
                return DecimalDataUtils.castFrom((DecimalData) value, dt.getPrecision(), dt.getScale());
            }
            if (value instanceof StringData) {
                return DecimalDataUtils.castFrom(value.toString(), dt.getPrecision(), dt.getScale());
            }
            if (value instanceof Number) {
                if (sourceRoot == LogicalTypeRoot.FLOAT || sourceRoot == LogicalTypeRoot.DOUBLE) {
                    return DecimalDataUtils.castFrom(((Number) value).doubleValue(), dt.getPrecision(), dt.getScale());
                }
                return DecimalDataUtils.castFrom(((Number) value).longValue(), dt.getPrecision(), dt.getScale());
            }
        }

        if (sourceRoot == LogicalTypeRoot.DECIMAL) {
            final DecimalData dec = (DecimalData) value;
            final long integral = DecimalDataUtils.castToIntegral(dec);
            switch (targetRoot) {
                case BIGINT:
                    return integral;
                case INTEGER:
                    return (int) integral;
                case SMALLINT:
                    return (short) integral;
                case TINYINT:
                    return (byte) integral;
                case FLOAT:
                    return (float) DecimalDataUtils.doubleValue(dec);
                case DOUBLE:
                    return DecimalDataUtils.doubleValue(dec);
                default:
                    break;
            }
        }

        if (value instanceof Number) {
            final Number number = (Number) value;
            switch (targetRoot) {
                case BIGINT:
                    return number.longValue();
                case INTEGER:
                    return number.intValue();
                case SMALLINT:
                    return number.shortValue();
                case TINYINT:
                    return number.byteValue();
                case FLOAT:
                    return number.floatValue();
                case DOUBLE:
                    return number.doubleValue();
                default:
                    break;
            }
        }

        if (isStringRoot(targetRoot)) {
            if (value instanceof StringData) {
                return value;
            }
            return StringData.fromString(String.valueOf(value));
        }

        throw new TableException(
                "ProxyOperator cannot cast "
                        + sourceType.asSerializableString()
                        + " to "
                        + targetType.asSerializableString());
    }

    // ------------------------------------------------------------------------
    // IO helpers
    // ------------------------------------------------------------------------

    private static Socket connectSocket(String host, int port, int connectTimeoutMs) throws IOException {
        final Socket socket = new Socket();
        socket.setTcpNoDelay(true);
        socket.connect(new InetSocketAddress(host, port), connectTimeoutMs);
        return socket;
    }

    private static IOException closeQuietly(AutoCloseable c) {
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

    private static IOException flushAndClose(Object o) {
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

    private static IOException suppress(IOException existing, IOException next) {
        if (existing == null) {
            return next;
        }
        if (next != null) {
            existing.addSuppressed(next);
        }
        return existing;
    }

    // ------------------------------------------------------------------------
    // Config
    // ------------------------------------------------------------------------

    private static final class ProxyTcpConfig {
        private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
        private static final int DEFAULT_CONNECT_TIMEOUT_MS = 10_000;
        private static final int DEFAULT_MAX_FRAME_SIZE = 64 * 1024 * 1024;

        // batching defaults (you can tune)
        private static final int DEFAULT_BATCH_MAX_ROWS = 8192;
        private static final int DEFAULT_REORDER_MAX_BUFFER = 10000;
        private static final int DEFAULT_FLUSH_INTERVAL_MS = 0;
        private static final boolean DEFAULT_POST_ROLE_ONLY = true;

        private final List<ProxyEndpoint> proxies;
        private final ProxyEndpoint selectedProxy;
        private final int bufferSize;
        private final int connectTimeoutMs;
        private final int readTimeoutMs;
        private final int maxFrameSize;

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

        // batching knobs
        private final boolean flushOnWrite;
        private final int batchMaxRows;
        private final int flushIntervalMs;

        private final boolean reorderResponses;
        private final int reorderMaxBuffer;

        private final boolean postRoleOnly;

        private ProxyTcpConfig(
                int bufferSize,
                int connectTimeoutMs,
                int readTimeoutMs,
                int maxFrameSize,
                boolean flushOnWrite,
                int batchMaxRows,
                int flushIntervalMs,
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
            this.maxFrameSize = maxFrameSize;

            this.flushOnWrite = flushOnWrite;
            this.batchMaxRows = batchMaxRows;
            this.flushIntervalMs = flushIntervalMs;

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
            final int maxFrameSize = parseInt(map.get("maxframesize"), DEFAULT_MAX_FRAME_SIZE);

            final boolean flushOnWrite = parseBoolean(map.get("flush"), false);
            final int batchMaxRows = parseInt(map.get("batchmaxrows"), DEFAULT_BATCH_MAX_ROWS);
            final int flushIntervalMs = parseInt(map.get("flushintervalms"), DEFAULT_FLUSH_INTERVAL_MS);

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
                    maxFrameSize,
                    flushOnWrite,
                    batchMaxRows,
                    flushIntervalMs,
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

        private static final class ProxyEndpoint {
            private final String host;
            private final int sendPort;
            private final int receivePort;

            private ProxyEndpoint(String host, int sendPort, int receivePort) {
                this.host = host;
                this.sendPort = sendPort;
                this.receivePort = receivePort;
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
    }
}
