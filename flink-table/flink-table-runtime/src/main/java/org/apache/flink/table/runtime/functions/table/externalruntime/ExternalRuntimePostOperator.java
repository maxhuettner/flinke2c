package org.apache.flink.table.runtime.functions.table.externalruntime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
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
    private transient byte[] frameBuf;
    private transient StreamRecord<RowData> reuseStreamRecord;

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
            this.reuseStreamRecord = new StreamRecord<>(null);
        }

        this.frameBuf = new byte[tcpConfig.getBufferSize()];

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
                emitWithTimestamp(block.rows.get(i), element);
            }
            return;
        }

        readAndEmitBlock(endpointIndex, fallbackKind, expectedRowId, element);
        expectedRowId++;
    }

    private ResponseBlock readNextBlock(int endpointIndex, RowKind fallbackKind) throws IOException {
        final BufferedInputStream in = ins.get(endpointIndex);

        final int batchLen = readIntBE(in);
        ensureFrameBuf(batchLen);
        readFully(in, frameBuf, 0, batchLen);

        int pos = 0;

        pos += 4; // __op
        final long blockRowId = readLongBE(frameBuf, pos);
        pos += 8;

        final boolean countIsNull = (frameBuf[pos] & 1) != 0;
        pos += 1;

        if (countIsNull) {
            throw new IOException("ExternalRuntimePostOperator received null count");
        }

        final int count = readIntBE(frameBuf, pos);
        pos += 4;

        if (count < 0) {
            throw new IOException("ExternalRuntimePostOperator received negative count: " + count);
        }

        final List<RowData> rows = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            pos = decodeRowFromBuffer(frameBuf, pos, fallbackKind, rows);
        }

        return new ResponseBlock(blockRowId, rows);
    }

    private int decodeRowFromBuffer(byte[] buffer, int pos, RowKind fallbackKind, List<RowData> rows) throws IOException {
        final int op = readIntBE(buffer, pos);
        pos += 4;
        pos += 8; // skip __rowId

        final int nFields = resultFieldTypes.size();
        final int nullBytes = (nFields + 7) >>> 3;
        final int nullBitmapPos = pos;
        pos += nullBytes;

        final GenericRowData outRow = new GenericRowData(nFields);

        for (int i = 0; i < nFields; i++) {
            if (isNullBitSet(buffer, nullBitmapPos, i)) {
                outRow.setField(i, null);
                continue;
            }

            final WireType wt = resultWireTypes[i];
            pos = decodeFieldValueAndAdvance(buffer, pos, wt, resultReadTypes.get(i), resultFieldTypes.get(i), outRow, i);
        }

        outRow.setRowKind(ExternalRuntimeBinaryCodec.opToRowKind(op, fallbackKind));
        rows.add(outRow);
        return pos;
    }

    private void readAndEmitBlock(
            int endpointIndex,
            RowKind fallbackKind,
            long targetRowId,
            StreamRecord<RowData> element)
            throws IOException {
        final BufferedInputStream in = ins.get(endpointIndex);

        final int batchLen = readIntBE(in);
        ensureFrameBuf(batchLen);
        readFully(in, frameBuf, 0, batchLen);

        int pos = 0;

        pos += 4; // __op
        final long blockRowId = readLongBE(frameBuf, pos);
        pos += 8;

        if (blockRowId != targetRowId) {
            throw new IOException(
                    "ExternalRuntimePostOperator expected rowId "
                            + targetRowId
                            + " but received "
                            + blockRowId);
        }

        final boolean countIsNull = (frameBuf[pos] & 1) != 0;
        pos += 1;

        if (countIsNull) {
            throw new IOException("ExternalRuntimePostOperator received null count");
        }

        final int count = readIntBE(frameBuf, pos);
        pos += 4;

        if (count < 0) {
            throw new IOException("ExternalRuntimePostOperator received negative count: " + count);
        }
        if (count == 0) {
            return;
        }

        final GenericRowData rowToEmit = reuseObjects ? reuseRow : null;
        final boolean hasTimestamp = element.hasTimestamp();
        final long timestamp = hasTimestamp ? element.getTimestamp() : 0L;

        if (reuseObjects && reuseStreamRecord != null) {
            for (int i = 0; i < count; i++) {
                pos = decodeAndEmitRow(frameBuf, pos, fallbackKind, rowToEmit, reuseStreamRecord, timestamp);
            }
        } else {
            for (int i = 0; i < count; i++) {
                pos = decodeAndEmitRow(frameBuf, pos, fallbackKind, rowToEmit, null, timestamp);
            }
        }
    }

    private int decodeAndEmitRow(
            byte[] buffer,
            int pos,
            RowKind fallbackKind,
            GenericRowData reuseRow,
            StreamRecord<RowData> reuseRecord,
            long timestamp)
            throws IOException {

        final int op = readIntBE(buffer, pos);
        pos += 4;
        pos += 8; // skip __rowId

        final int nFields = resultFieldTypes.size();
        final int nullBytes = (nFields + 7) >>> 3;
        final int nullBitmapPos = pos;
        pos += nullBytes;

        final GenericRowData outRow =
                reuseRow != null && reuseRow.getArity() == nFields
                        ? reuseRow
                        : new GenericRowData(nFields);

        for (int i = 0; i < nFields; i++) {
            if (isNullBitSet(buffer, nullBitmapPos, i)) {
                outRow.setField(i, null);
                continue;
            }

            final WireType wt = resultWireTypes[i];
            pos = decodeFieldValueAndAdvance(buffer, pos, wt, resultReadTypes.get(i), resultFieldTypes.get(i), outRow, i);
        }

        outRow.setRowKind(ExternalRuntimeBinaryCodec.opToRowKind(op, fallbackKind));

        if (reuseRecord != null) {
            reuseRecord.replace(outRow, timestamp);
            output.collect(reuseRecord);
        } else {
            output.collect(new StreamRecord<>(outRow, timestamp));
        }

        return pos;
    }

    private int decodeFieldValueAndAdvance(
            byte[] buf,
            int pos,
            WireType wt,
            LogicalType sourceType,
            LogicalType targetType,
            GenericRowData outRow,
            int fieldIndex)
            throws IOException {
        switch (wt) {
            case BOOL:
                outRow.setField(fieldIndex, buf[pos] != 0);
                return pos + 1;
            case INT32:
                outRow.setField(fieldIndex, readIntBE(buf, pos));
                return pos + 4;
            case INT64:
                outRow.setField(fieldIndex, readLongBE(buf, pos));
                return pos + 8;
            case TIMESTAMP_MILLIS: {
                final long millis = readLongBE(buf, pos);
                outRow.setField(fieldIndex, org.apache.flink.table.data.TimestampData.fromEpochMillis(millis));
                return pos + 8;
            }
            case FLOAT32:
                outRow.setField(fieldIndex, Float.intBitsToFloat(readIntBE(buf, pos)));
                return pos + 4;
            case FLOAT64:
                outRow.setField(fieldIndex, Double.longBitsToDouble(readLongBE(buf, pos)));
                return pos + 8;
            case STRING: {
                final int strLen = readIntBE(buf, pos);
                outRow.setField(fieldIndex, org.apache.flink.table.data.StringData.fromBytes(buf, pos + 4, strLen));
                return pos + 4 + strLen;
            }
            case BYTES: {
                final int bytesLen = readIntBE(buf, pos);
                byte[] bytes = new byte[bytesLen];
                System.arraycopy(buf, pos + 4, bytes, 0, bytesLen);
                outRow.setField(fieldIndex, bytes);
                return pos + 4 + bytesLen;
            }
            case DECIMAL_UNSCALED_I64: {
                final long unscaled = readLongBE(buf, pos);
                if (targetType instanceof org.apache.flink.table.types.logical.DecimalType) {
                    final org.apache.flink.table.types.logical.DecimalType dt =
                            (org.apache.flink.table.types.logical.DecimalType) targetType;
                    outRow.setField(fieldIndex, org.apache.flink.table.data.DecimalData.fromUnscaledLong(
                            unscaled, dt.getPrecision(), dt.getScale()));
                } else {
                    outRow.setField(fieldIndex, unscaled);
                }
                return pos + 8;
            }
            case DECIMAL_UNSCALED_BYTES: {
                final int decLen = readIntBE(buf, pos);
                byte[] decBytes = new byte[decLen];
                System.arraycopy(buf, pos + 4, decBytes, 0, decLen);
                if (targetType instanceof org.apache.flink.table.types.logical.DecimalType) {
                    final org.apache.flink.table.types.logical.DecimalType dt =
                            (org.apache.flink.table.types.logical.DecimalType) targetType;
                    final java.math.BigInteger bi = new java.math.BigInteger(decBytes);
                    final java.math.BigDecimal bd = new java.math.BigDecimal(bi, dt.getScale());
                    outRow.setField(fieldIndex, org.apache.flink.table.data.DecimalData.fromBigDecimal(
                            bd, dt.getPrecision(), dt.getScale()));
                } else {
                    outRow.setField(fieldIndex, decBytes);
                }
                return pos + 4 + decLen;
            }
            default:
                throw new IOException("Unsupported wire type: " + wt);
        }
    }

    private static boolean isNullBitSet(byte[] payload, int bitmapPos, int fieldIndex) {
        final int byteIndex = bitmapPos + (fieldIndex >>> 3);
        final int bit = fieldIndex & 7;
        return (payload[byteIndex] & (1 << bit)) != 0;
    }

    private void emitWithTimestamp(RowData row, StreamRecord<RowData> input) {
        if (input.hasTimestamp()) {
            output.collect(new StreamRecord<>(row, input.getTimestamp()));
        } else {
            output.collect(new StreamRecord<>(row));
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

    private static final class ResponseBlock {
        private final long rowId;
        private final List<RowData> rows;

        private ResponseBlock(long rowId, List<RowData> rows) {
            this.rowId = rowId;
            this.rows = rows;
        }
    }

    private void ensureFrameBuf(int len) {
        if (frameBuf.length >= len) {
            return;
        }
        int n = frameBuf.length;
        while (n < len) {
            n <<= 1;
        }
        frameBuf = new byte[n];
    }

    private static int readIntBE(BufferedInputStream in) throws IOException {
        final int b1 = in.read();
        final int b2 = in.read();
        final int b3 = in.read();
        final int b4 = in.read();
        if ((b1 | b2 | b3 | b4) < 0) {
            throw new java.io.EOFException("EOF while reading int32");
        }
        return (b1 << 24) | (b2 << 16) | (b3 << 8) | (b4);
    }

    private static int readIntBE(byte[] buf, int p) {
        return ((buf[p] & 0xff) << 24)
                | ((buf[p + 1] & 0xff) << 16)
                | ((buf[p + 2] & 0xff) << 8)
                | (buf[p + 3] & 0xff);
    }

    private static long readLongBE(byte[] buf, int p) {
        return ((long) (buf[p] & 0xff) << 56)
                | ((long) (buf[p + 1] & 0xff) << 48)
                | ((long) (buf[p + 2] & 0xff) << 40)
                | ((long) (buf[p + 3] & 0xff) << 32)
                | ((long) (buf[p + 4] & 0xff) << 24)
                | ((long) (buf[p + 5] & 0xff) << 16)
                | ((long) (buf[p + 6] & 0xff) << 8)
                | (buf[p + 7] & 0xff);
    }

    private static void readFully(BufferedInputStream in, byte[] b, int off, int len)
            throws IOException {
        int n = 0;
        while (n < len) {
            final int r = in.read(b, off + n, len - n);
            if (r < 0) {
                throw new java.io.EOFException("Truncated frame");
            }
            n += r;
        }
    }

    @Override
    protected void closeInternal() throws Exception {
        IOException error = null;

        codec = null;
        reuseRow = null;
        reuseStreamRecord = null;
        frameBuf = null;
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


}
