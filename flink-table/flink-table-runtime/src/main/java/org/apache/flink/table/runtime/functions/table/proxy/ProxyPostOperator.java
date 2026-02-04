package org.apache.flink.table.runtime.functions.table.proxy;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/** POST: receives processed framed rows from external process and merges them into incoming rows. */
@Internal
public final class ProxyPostOperator extends ProxyOperator {

    private static final long serialVersionUID = 1L;

    // POST (binary reader)
    private transient BufferedInputStream in;
    private transient long expectedRowId;
    private transient Map<Long, RowData> reorderBuffer;

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

        // codec (reader configured; writer null)
        this.codec =
                new ProxyBinaryCodec(
                        tcpConfig.isReorderResponses(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        resultWireTypes,
                        resultReadTypes.toArray(new LogicalType[0]),
                        resultFieldTypes.toArray(new LogicalType[0]));

        this.expectedRowId = 0L;
        this.reorderBuffer = tcpConfig.isReorderResponses() ? new HashMap<>() : null;

        LOG.info(
                "ProxyPostOperator connected to {}:{} (rowType={}, sentConfigBytes={})",
                proxy.getHost(),
                port,
                inputRowType,
                configJson.getBytes(StandardCharsets.UTF_8).length);
    }

    @Override
    protected RowData processRow(RowData inRow) throws Exception {
        final RowData proxyRow;
        final RowKind fallbackKind = inRow.getRowKind();
        if (tcpConfig.isReorderResponses()) {
            proxyRow = readNextOrderedRow(fallbackKind);
        } else {
            proxyRow = readNextRow(fallbackKind);
        }
        return mergeProxyRow(inRow, proxyRow);
    }

    private RowData readNextRow(RowKind fallbackKind) throws IOException {
        return readNextRowWithId(fallbackKind).row;
    }

    private RowWithId readNextRowWithId(RowKind fallbackKind) throws IOException {
        if (codec == null || in == null) {
            throw new IOException("ProxyPostOperator codec not initialized");
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
                if (reorderBuffer.size() > tcpConfig.getReorderMaxBuffer()) {
                    throw new IOException(
                            "ProxyPostOperator reorder buffer exceeded "
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

    @Override
    protected void closeInternal() throws Exception {
        IOException error = null;

        codec = null;

        error = suppress(error, closeQuietly(in));
        in = null;

        if (error != null) {
            throw error;
        }
    }
}