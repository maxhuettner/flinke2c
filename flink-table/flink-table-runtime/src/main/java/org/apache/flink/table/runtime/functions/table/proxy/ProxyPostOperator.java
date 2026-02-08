package org.apache.flink.table.runtime.functions.table.proxy;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.table.runtime.functions.table.proxy.ProxyBinaryCodec.WireType;

import javax.annotation.Nullable;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** POST: receives processed framed rows from external process and merges them into incoming rows. */
@Internal
public final class ProxyPostOperator extends ProxyOperator {

    private static final long serialVersionUID = 1L;

    // POST (binary reader)
    private transient BufferedInputStream in;
    private transient long expectedRowId;
    private transient Map<Long, ResponseBlock> reorderBlockBuffer;
    private transient boolean reuseObjects;
    private transient GenericRowData reuseRow;
    private transient ProxyBinaryCodec headerCodec;
    private transient GenericRowData reuseHeaderRow;
    private transient RowData.FieldGetter countGetter;

    public ProxyPostOperator(String conf, RowType rowType) {
        this(conf, rowType, rowType);
    }

    public ProxyPostOperator(String conf, RowType inputRowType, @Nullable RowType resultRowType) {
        super(conf, inputRowType, resultRowType);
    }

    @Override
    protected Role role() {
        return Role.POST;
    }

    @Override
    protected void openInternal() throws Exception {
        final ProxyTcpConfig.ProxyEndpoint proxy = tcpConfig.getSelectedProxy();
        final int port = proxy.getReceivePort();
        this.socket = connectSocket(proxy.getHost(), port, tcpConfig.getConnectTimeoutMs());

        if (tcpConfig.getReadTimeoutMs() > 0) {
            socket.setSoTimeout(tcpConfig.getReadTimeoutMs());
        }

        this.in = new BufferedInputStream(socket.getInputStream(), tcpConfig.getBufferSize());

        // send preamble
        final BufferedOutputStream postOut =
                new BufferedOutputStream(socket.getOutputStream(), tcpConfig.getBufferSize());
        final String configJson = buildConfigJson();
        writeLengthPrefixedJson(postOut, configJson);

        this.reuseObjects =
                getRuntimeContext().isObjectReuseEnabled() && !tcpConfig.isReorderResponses();

        // codec (reader configured; writer null)
        this.codec =
                new ProxyBinaryCodec(
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
                new ProxyBinaryCodec(
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
            this.reorderBlockBuffer = new HashMap<>();
        } else {
            this.reorderBlockBuffer = null;
        }

        LOG.info(
                "ProxyPostOperator connected to {}:{} (rowType={}, sentConfigBytes={})",
                proxy.getHost(),
                port,
                inputRowType,
                configJson.getBytes(StandardCharsets.UTF_8).length);
    }

    @Override
    protected RowData processRow(RowData inRow) throws Exception {
        throw new UnsupportedOperationException(
                "ProxyPostOperator emits counted responses via processElementInternal.");
    }

    @Override
    protected void processElementInternal(StreamRecord<RowData> element) throws Exception {
        final RowData inRow = element.getValue();
        final RowKind fallbackKind = inRow.getRowKind();
        final ResponseBlock block =
                tcpConfig.isReorderResponses()
                        ? readNextOrderedBlock(fallbackKind)
                        : readNextBlock(fallbackKind);

        if (!tcpConfig.isReorderResponses()) {
            if (block.rowId != expectedRowId) {
                throw new IOException(
                        "ProxyPostOperator expected rowId "
                                + expectedRowId
                                + " but received "
                                + block.rowId);
            }
            expectedRowId++;
        }

        for (int i = 0; i < block.rows.size(); i++) {
            final RowData outRow = mergeProxyRow(inRow, block.rows.get(i));
            output.collect(element.copy(outRow));
        }
    }

    private RowWithId readNextRowWithId(RowKind fallbackKind) throws IOException {
        if (codec == null || in == null) {
            throw new IOException("ProxyPostOperator codec not initialized");
        }

        final ProxyBinaryCodec.RowWithId decoded = codec.readFramedRow(in, fallbackKind, reuseRow);
        return new RowWithId(decoded.rowId, decoded.row);
    }

    private Header readNextHeader(RowKind fallbackKind) throws IOException {
        if (headerCodec == null || in == null || countGetter == null) {
            throw new IOException("ProxyPostOperator header codec not initialized");
        }
        final ProxyBinaryCodec.RowWithId decoded =
                headerCodec.readFramedRow(in, fallbackKind, reuseHeaderRow);
        final Object countValue = countGetter.getFieldOrNull(decoded.row);
        if (!(countValue instanceof Integer)) {
            throw new IOException("ProxyPostOperator invalid count field: " + countValue);
        }
        return new Header(decoded.rowId, (Integer) countValue);
    }

    private ResponseBlock readNextBlock(RowKind fallbackKind) throws IOException {
        final Header header = readNextHeader(fallbackKind);
        if (header.count < 0) {
            throw new IOException("ProxyPostOperator received negative count: " + header.count);
        }

        final List<RowData> rows = new ArrayList<>(header.count);
        for (int i = 0; i < header.count; i++) {
            final RowWithId decoded = readNextRowWithId(fallbackKind);
            if (decoded.rowId != header.rowId) {
                throw new IOException(
                        "ProxyPostOperator expected rowId "
                                + header.rowId
                                + " but received "
                                + decoded.rowId);
            }
            rows.add(decoded.row);
        }
        return new ResponseBlock(header.rowId, rows);
    }

    private ResponseBlock readNextOrderedBlock(RowKind fallbackKind) throws IOException {
        final long targetId = expectedRowId;
        ResponseBlock ready = reorderBlockBuffer.remove(targetId);
        while (ready == null) {
            final ResponseBlock next = readNextBlock(fallbackKind);
            if (next.rowId == targetId) {
                ready = next;
            } else {
                reorderBlockBuffer.put(next.rowId, next);
                if (reorderBlockBuffer.size() > tcpConfig.getReorderMaxBuffer()) {
                    throw new IOException(
                            "ProxyPostOperator reorder buffer exceeded "
                                    + tcpConfig.getReorderMaxBuffer()
                                    + " entries; increase reorderMax or enforce ordered responses.");
                }
            }
        }
        expectedRowId++;
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
        reorderBlockBuffer = null;

        error = suppress(error, closeQuietly(in));
        in = null;

        if (error != null) {
            throw error;
        }
    }

    private final RowData mergeProxyRow(RowData baseRow, RowData proxyRow) {
        if (resultReplacesAllFields) {
            return proxyRow;
        }
        final int fieldCount = inputRowType.getFieldCount();
        final GenericRowData outRow = new GenericRowData(fieldCount);
        outRow.setRowKind(proxyRow.getRowKind());

        for (int i = 0; i < fieldCount; i++) {
            final int resultPos = resultPosByInputIndex[i];
            if (resultPos >= 0) {
                outRow.setField(i, resultFieldGetters[resultPos].getFieldOrNull(proxyRow));
            } else {
                outRow.setField(i, fullRowFieldGetters[i].getFieldOrNull(baseRow));
            }
        }
        return outRow;
    }
}
