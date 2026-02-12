package org.apache.flink.table.runtime.functions.table.externalruntime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** PRE: sends input rows to the external runtime and emits placeholders. */
@Internal
public final class ExternalRuntimePreOperator extends ExternalRuntimeOperator
        implements ProcessingTimeService.ProcessingTimeCallback {

    private static final long serialVersionUID = 1L;

    private transient List<ExternalRuntimeTcpConfig.ExternalRuntimeEndpoint> endpoints;
    private transient List<BufferedOutputStream> outs;
    private transient List<Socket> sockets;
    private transient List<ByteArrayOutputStream> batchBuffers;
    private transient int[] batchCounts;
    private transient long[] batchFirstBufferedAt;
    private transient long[] batchLastBufferedAt;
    private transient int batchSize;
    private transient int flushIntervalMs;
    private transient int flushIdleMs;
    private transient long timerIntervalMs;
    private transient boolean timerScheduled;
    private transient long nextRowId;

    public ExternalRuntimePreOperator(String conf, RowType rowType) {
        super(conf, rowType, null);
    }

    @Override
    protected Role role() {
        return Role.PRE;
    }

    @Override
    protected void openInternal() throws Exception {
        final int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        final int totalSubtasks = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
        this.endpoints = tcpConfig.selectEndpoints(subtaskIndex, totalSubtasks);
        this.outs = new ArrayList<>(endpoints.size());
        this.sockets = new ArrayList<>(endpoints.size());
        this.batchBuffers = new ArrayList<>(endpoints.size());
        this.batchCounts = new int[endpoints.size()];
        this.batchFirstBufferedAt = new long[endpoints.size()];
        this.batchLastBufferedAt = new long[endpoints.size()];
        for (int i = 0; i < endpoints.size(); i++) {
            batchFirstBufferedAt[i] = -1L;
            batchLastBufferedAt[i] = -1L;
        }
        this.batchSize = Math.max(1, tcpConfig.getBatchSize());
        this.flushIntervalMs = Math.max(0, tcpConfig.getFlushIntervalMs());
        this.flushIdleMs = Math.max(0, tcpConfig.getFlushIdleMs());
        this.timerIntervalMs = computeTimerInterval(flushIntervalMs, flushIdleMs);
        this.timerScheduled = false;
        final String configJson = buildConfigJson();
        for (ExternalRuntimeTcpConfig.ExternalRuntimeEndpoint endpoint : endpoints) {
            final int port = endpoint.getSendPort();
            final Socket sock =
                    connectSocket(endpoint.getHost(), port, tcpConfig.getConnectTimeoutMs());
            sock.setTcpNoDelay(true);
            final BufferedOutputStream out =
                    new BufferedOutputStream(sock.getOutputStream(), tcpConfig.getBufferSize());
            writeLengthPrefixedJson(out, configJson);
            out.flush();
            sockets.add(sock);
            outs.add(out);
            batchBuffers.add(new ByteArrayOutputStream(tcpConfig.getBufferSize()));
        }
        if (!sockets.isEmpty()) {
            this.socket = sockets.get(0);
        }

        this.nextRowId = 0L;

        this.codec =
                new ExternalRuntimeBinaryCodec(
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
                "ExternalRuntimePreOperator connected to {} runtime(s) (rowType={}, sentConfigBytes={})",
                endpoints.size(),
                inputRowType,
                configJson.getBytes(StandardCharsets.UTF_8).length);

        if (timerIntervalMs > 0) {
            scheduleTimer(getProcessingTimeService().getCurrentProcessingTime());
        }
    }

    @Override
    protected RowData processRow(RowData inRow) throws Exception {
        appendRowToBinary(inRow);
        return createPlaceholderRow(inRow.getRowKind());
    }

    private void appendRowToBinary(RowData row) throws IOException {
        if (outs == null || outs.isEmpty() || batchBuffers == null) {
            throw new IOException("ExternalRuntimePreOperator output stream not initialized");
        }
        final int idx = tcpConfig.selectEndpointIndex(nextRowId, outs.size());
        final ByteArrayOutputStream buffer = batchBuffers.get(idx);
        codec.writeFramedRow(buffer, row, payloadFieldIndicesArray, nextRowId);
        final long now = getProcessingTimeService().getCurrentProcessingTime();
        if (batchCounts[idx] == 0) {
            batchFirstBufferedAt[idx] = now;
        }
        batchLastBufferedAt[idx] = now;
        batchCounts[idx]++;
        if (batchCounts[idx] >= batchSize) {
            flushBatch(idx);
        }
        nextRowId++;
        if (!timerScheduled && timerIntervalMs > 0) {
            scheduleTimer(now);
        }
    }

    private void flushBatch(int idx) throws IOException {
        final ByteArrayOutputStream buffer = batchBuffers.get(idx);
        if (buffer == null || buffer.size() == 0) {
            batchCounts[idx] = 0;
            return;
        }
        final BufferedOutputStream out = outs.get(idx);
        buffer.writeTo(out);
        out.flush();
        buffer.reset();
        batchCounts[idx] = 0;
        batchFirstBufferedAt[idx] = -1L;
        batchLastBufferedAt[idx] = -1L;
    }

    private IOException tryFlushRemaining() {
        IOException error = null;
        if (outs == null || batchBuffers == null) {
            return null;
        }
        for (int i = 0; i < outs.size(); i++) {
            try {
                flushBatch(i);
            } catch (IOException e) {
                error = suppress(error, e);
            }
        }
        return error;
    }

    @Override
    public void onProcessingTime(long timestamp) throws Exception {
        if (batchCounts == null || batchBuffers == null) {
            return;
        }
        final long now = timestamp;
        boolean hasPending = false;
        for (int i = 0; i < batchCounts.length; i++) {
            if (batchCounts[i] == 0) {
                continue;
            }
            hasPending = true;
            if (flushIntervalMs > 0) {
                final long firstAt = batchFirstBufferedAt[i];
                if (firstAt >= 0 && now - firstAt >= flushIntervalMs) {
                    flushBatch(i);
                    continue;
                }
            }
            if (flushIdleMs > 0) {
                final long lastAt = batchLastBufferedAt[i];
                if (lastAt >= 0 && now - lastAt >= flushIdleMs) {
                    flushBatch(i);
                }
            }
        }
        if (timerIntervalMs > 0 && hasPending) {
            scheduleTimer(now);
        } else {
            timerScheduled = false;
        }
    }

    private void scheduleTimer(long now) {
        final long next = now + timerIntervalMs;
        timerScheduled = true;
        getProcessingTimeService().registerTimer(next, this);
    }

    private static long computeTimerInterval(int flushIntervalMs, int flushIdleMs) {
        if (flushIntervalMs > 0 && flushIdleMs > 0) {
            return Math.min(flushIntervalMs, flushIdleMs);
        }
        if (flushIntervalMs > 0) {
            return flushIntervalMs;
        }
        if (flushIdleMs > 0) {
            return flushIdleMs;
        }
        return 0;
    }

    @Override
    protected void closeInternal() throws Exception {
        IOException error = null;

        error = suppress(error, tryFlushRemaining());

        if (outs != null) {
            for (BufferedOutputStream out : outs) {
                error = suppress(error, flushAndClose(out));
            }
        }
        outs = null;
        batchBuffers = null;
        batchCounts = null;
        batchFirstBufferedAt = null;
        batchLastBufferedAt = null;
        flushIntervalMs = 0;
        flushIdleMs = 0;
        timerIntervalMs = 0;
        timerScheduled = false;

        codec = null;

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
