package org.apache.flink.table.runtime.functions.table.proxy;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** PRE: sends input rows (framed binary) to external process, emits placeholders. */
@Internal
public final class ProxyPreOperator extends ProxyOperator {

    private static final long serialVersionUID = 1L;

    // PRE (binary writer + batch state)
    private transient BufferedOutputStream out;
    private transient long nextRowId;

    public ProxyPreOperator(String conf, RowType rowType) {
        super(conf, rowType, null);
    }

    @Override
    protected Role role() {
        return Role.PRE;
    }

    @Override
    protected void openInternal() throws Exception {
        final ProxyTcpConfig.ProxyEndpoint proxy = tcpConfig.getSelectedProxy();
        final int port = proxy.getSendPort();
        this.socket = connectSocket(proxy.getHost(), port, tcpConfig.getConnectTimeoutMs());
        this.out = new BufferedOutputStream(socket.getOutputStream(), tcpConfig.getBufferSize());

        // preamble
        final String configJson = buildConfigJson();
        writeLengthPrefixedJson(out, configJson);

        this.nextRowId = 0L;

        // codec (writer configured; reader null)
        this.codec =
                new ProxyBinaryCodec(
                        true,
                        payloadWireTypes,
                        payloadWriteTypes.toArray(new LogicalType[0]),
                        payloadSourceRoots,
                        payloadSourcePrecision,
                        payloadSourceScale,
                        payloadTimestampPrecision,
                        null,
                        null,
                        null,
                        false);

        LOG.info(
                "ProxyPreOperator connected to {}:{} (rowType={}, sentConfigBytes={})",
                proxy.getHost(),
                port,
                inputRowType,
                configJson.getBytes(StandardCharsets.UTF_8).length);
    }

    @Override
    protected RowData processRow(RowData inRow) throws Exception {
        appendRowToBinary(inRow);
        return createPlaceholderRow(inRow.getRowKind());
    }

    private void appendRowToBinary(RowData row) throws IOException {
        codec.writeFramedRow(out, row, payloadFieldIndicesArray, nextRowId);
        nextRowId++;
    }

    private IOException tryFlushRemaining() {
        try {
            out.flush();
            return null;
        } catch (IOException e) {
            return e;
        }
    }

    @Override
    protected void closeInternal() throws Exception {
        IOException error = null;

        error = suppress(error, tryFlushRemaining());

        error = suppress(error, flushAndClose(out));
        out = null;

        codec = null;

        if (error != null) {
            throw error;
        }
    }

    private final RowData createPlaceholderRow(RowKind kind) {
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
}
