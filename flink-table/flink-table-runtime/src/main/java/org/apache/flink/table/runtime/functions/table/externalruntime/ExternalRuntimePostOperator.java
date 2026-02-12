package org.apache.flink.table.runtime.functions.table.externalruntime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.table.runtime.functions.table.externalruntime.ExternalRuntimeBinaryCodec.WireType;

import javax.annotation.Nullable;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** POST: receives processed rows from the external runtime and merges them into incoming rows. */
@Internal
public final class ExternalRuntimePostOperator extends ExternalRuntimeOperator {

    private static final long serialVersionUID = 1L;

    private transient List<ExternalRuntimeTcpConfig.ExternalRuntimeEndpoint> endpoints;
    private transient List<BufferedInputStream> ins;
    private transient List<BufferedOutputStream> outs;
    private transient List<Socket> sockets;
    private transient long expectedRowId;
    private transient List<Map<Long, ResponseBlock>> reorderBlockBuffers;
    private transient boolean reuseObjects;
    private transient GenericRowData reuseRow;
    private transient GenericRowData reuseMergedRow;
    private transient ExternalRuntimeBinaryCodec headerCodec;
    private transient GenericRowData reuseHeaderRow;
    private transient RowData.FieldGetter countGetter;

    public ExternalRuntimePostOperator(String conf, RowType rowType) {
        this(conf, rowType, rowType);
    }

    public ExternalRuntimePostOperator(String conf, RowType inputRowType, @Nullable RowType resultRowType) {
        super(conf, inputRowType, resultRowType);
    }

    @Override
    protected Role role() {
        return Role.POST;
    }

    @Override
    protected void openInternal() throws Exception {
        final int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        final int totalSubtasks = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
        this.endpoints = tcpConfig.selectEndpoints(subtaskIndex, totalSubtasks);
        this.ins = new ArrayList<>(endpoints.size());
        this.outs = new ArrayList<>(endpoints.size());
        this.sockets = new ArrayList<>(endpoints.size());
        final String configJson = buildConfigJson();
        for (ExternalRuntimeTcpConfig.ExternalRuntimeEndpoint endpoint : endpoints) {
            final int port = endpoint.getReceivePort();
            final Socket sock =
                    connectSocket(endpoint.getHost(), port, tcpConfig.getConnectTimeoutMs());
            if (tcpConfig.getReadTimeoutMs() > 0) {
                sock.setSoTimeout(tcpConfig.getReadTimeoutMs());
            }
            final BufferedInputStream in =
                    new BufferedInputStream(sock.getInputStream(), tcpConfig.getBufferSize());
            final BufferedOutputStream postOut =
                    new BufferedOutputStream(sock.getOutputStream(), tcpConfig.getBufferSize());
            writeLengthPrefixedJson(postOut, configJson);
            postOut.flush();
            sockets.add(sock);
            ins.add(in);
            outs.add(postOut);
        }
        if (!sockets.isEmpty()) {
            this.socket = sockets.get(0);
        }

        this.reuseObjects =
                getRuntimeContext().isObjectReuseEnabled() && !tcpConfig.isReorderResponses();

        this.codec =
                new ExternalRuntimeBinaryCodec(
                        true,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        resultWireTypes,
                        resultReadTypes.toArray(new LogicalType[0]),
                        resultFieldTypes.toArray(new LogicalType[0]),
                        reuseObjects);

        if (reuseObjects) {
            this.reuseRow = new GenericRowData(resultFieldTypes.size());
        }

        final WireType[] headerWireTypes = new WireType[] {WireType.INT32};
        final LogicalType[] headerTypes = new LogicalType[] {new IntType()};
        this.headerCodec =
                new ExternalRuntimeBinaryCodec(
                        true,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        headerWireTypes,
                        headerTypes,
                        headerTypes,
                        false);
        this.countGetter = RowData.createFieldGetter(headerTypes[0], 0);
        if (reuseObjects) {
            this.reuseHeaderRow = new GenericRowData(1);
        }

        this.expectedRowId = 0L;
        if (tcpConfig.isReorderResponses()) {
            this.reorderBlockBuffers = new ArrayList<>(endpoints.size());
            for (int i = 0; i < endpoints.size(); i++) {
                reorderBlockBuffers.add(new HashMap<>());
            }
        } else {
            this.reorderBlockBuffers = null;
        }

        LOG.info(
                "ExternalRuntimePostOperator connected to {} runtime(s) (rowType={}, sentConfigBytes={})",
                endpoints.size(),
                inputRowType,
                configJson.getBytes(StandardCharsets.UTF_8).length);
    }

    @Override
    protected RowData processRow(RowData inRow) throws Exception {
        throw new UnsupportedOperationException(
                "ExternalRuntimePostOperator emits counted responses via processElementInternal.");
    }

    @Override
    protected void processElementInternal(StreamRecord<RowData> element) throws Exception {
        final RowData inRow = element.getValue();
        final RowKind fallbackKind = inRow.getRowKind();
        if (ins == null || ins.isEmpty()) {
            throw new IOException("ExternalRuntimePostOperator input stream not initialized");
        }
        final int endpointIndex = tcpConfig.selectEndpointIndex(expectedRowId, ins.size());
        if (tcpConfig.isReorderResponses()) {
            final ResponseBlock block =
                    readNextOrderedBlock(endpointIndex, fallbackKind, expectedRowId);
            expectedRowId++;
            for (int i = 0; i < block.rows.size(); i++) {
                final RowData outRow = mergeExternalRuntimeRow(inRow, block.rows.get(i));
                output.collect(element.copy(outRow));
            }
            return;
        }

        readAndEmitBlock(endpointIndex, fallbackKind, expectedRowId, element, inRow);
        expectedRowId++;
    }

    private RowWithId readNextRowWithId(BufferedInputStream in, RowKind fallbackKind)
            throws IOException {
        if (codec == null || in == null) {
            throw new IOException("ExternalRuntimePostOperator codec not initialized");
        }

        final ExternalRuntimeBinaryCodec.RowWithId decoded =
                codec.readFramedRow(in, fallbackKind, reuseRow);
        return new RowWithId(decoded.rowId, decoded.row);
    }

    private Header readNextHeader(BufferedInputStream in, RowKind fallbackKind) throws IOException {
        if (headerCodec == null || in == null || countGetter == null) {
            throw new IOException("ExternalRuntimePostOperator header codec not initialized");
        }
        final ExternalRuntimeBinaryCodec.RowWithId decoded =
                headerCodec.readFramedRow(in, fallbackKind, reuseHeaderRow);
        final Object countValue = countGetter.getFieldOrNull(decoded.row);
        if (!(countValue instanceof Integer)) {
            throw new IOException("ExternalRuntimePostOperator invalid count field: " + countValue);
        }
        return new Header(decoded.rowId, (Integer) countValue);
    }

    private ResponseBlock readNextBlock(int endpointIndex, RowKind fallbackKind) throws IOException {
        final BufferedInputStream in = ins.get(endpointIndex);
        final Header header = readNextHeader(in, fallbackKind);
        if (header.count < 0) {
            throw new IOException("ExternalRuntimePostOperator received negative count: " + header.count);
        }

        final List<RowData> rows = new ArrayList<>(header.count);
        for (int i = 0; i < header.count; i++) {
            final RowWithId decoded = readNextRowWithId(in, fallbackKind);
            rows.add(decoded.row);
        }
        return new ResponseBlock(header.rowId, rows);
    }

    private void readAndEmitBlock(
            int endpointIndex,
            RowKind fallbackKind,
            long targetRowId,
            StreamRecord<RowData> element,
            RowData baseRow)
            throws IOException {
        final BufferedInputStream in = ins.get(endpointIndex);
        final Header header = readNextHeader(in, fallbackKind);
        if (header.rowId != targetRowId) {
            throw new IOException(
                    "ExternalRuntimePostOperator expected rowId "
                            + targetRowId
                            + " but received "
                            + header.rowId);
        }
        if (header.count < 0) {
            throw new IOException("ExternalRuntimePostOperator received negative count: " + header.count);
        }
        for (int i = 0; i < header.count; i++) {
            final RowWithId decoded = readNextRowWithId(in, fallbackKind);
            final RowData outRow = mergeExternalRuntimeRow(baseRow, decoded.row);
            output.collect(element.copy(outRow));
        }
    }

    private ResponseBlock readNextOrderedBlock(
            int endpointIndex, RowKind fallbackKind, long targetId) throws IOException {
        final Map<Long, ResponseBlock> buffer = reorderBlockBuffers.get(endpointIndex);
        ResponseBlock ready = buffer.remove(targetId);
        while (ready == null) {
            final ResponseBlock next = readNextBlock(endpointIndex, fallbackKind);
            if (next.rowId == targetId) {
                ready = next;
            } else {
                buffer.put(next.rowId, next);
                if (buffer.size() > tcpConfig.getReorderMaxBuffer()) {
                    throw new IOException(
                            "ExternalRuntimePostOperator reorder buffer exceeded "
                                    + tcpConfig.getReorderMaxBuffer()
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

    private static final class Header {
        private final long rowId;
        private final int count;

        private Header(long rowId, int count) {
            this.rowId = rowId;
            this.count = count;
        }
    }

    private static final class ResponseBlock {
        private final long rowId;
        private final List<RowData> rows;

        private ResponseBlock(long rowId, List<RowData> rows) {
            this.rowId = rowId;
            this.rows = rows;
        }
    }

    @Override
    protected void closeInternal() throws Exception {
        IOException error = null;

        codec = null;
        reuseRow = null;
        headerCodec = null;
        reuseHeaderRow = null;
        countGetter = null;
        reorderBlockBuffers = null;

        if (outs != null) {
            for (BufferedOutputStream out : outs) {
                error = suppress(error, flushAndClose(out));
            }
        }
        outs = null;

        if (ins != null) {
            for (BufferedInputStream in : ins) {
                error = suppress(error, closeQuietly(in));
            }
        }
        ins = null;

        if (sockets != null) {
            for (Socket sock : sockets) {
                error = suppress(error, closeQuietly(sock));
            }
        }
        sockets = null;
        socket = null;

        if (error != null) {
            throw error;
        }
    }

    private RowData mergeExternalRuntimeRow(RowData baseRow, RowData externalRow) {
        if (resultReplacesAllFields) {
            return externalRow;
        }
        final int fieldCount = inputRowType.getFieldCount();
        if (reuseObjects && reuseMergedRow == null) {
            reuseMergedRow = new GenericRowData(fieldCount);
        }
        final GenericRowData outRow = reuseObjects ? reuseMergedRow : new GenericRowData(fieldCount);
        outRow.setRowKind(externalRow.getRowKind());

        for (int i = 0; i < fieldCount; i++) {
            final int resultPos = resultPosByInputIndex[i];
            if (resultPos >= 0) {
                outRow.setField(i, resultFieldGetters[resultPos].getFieldOrNull(externalRow));
            } else {
                outRow.setField(i, fullRowFieldGetters[i].getFieldOrNull(baseRow));
            }
        }
        return outRow;
    }

}
