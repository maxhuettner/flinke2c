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

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * POST: receives processed rows from the external runtime and merges them into
 * incoming rows.
 */
@Internal
public final class ExternalRuntimePostOperator extends ExternalRuntimeOperator {

    private static final long serialVersionUID = 1L;
    private static final int MAX_FRAME_BYTES = 100_000_000;
    private static final long ROUTING_HINT_RECENCY_ROWS = 65_536L;
    private static final int DYNAMIC_MISSING_ROW_TIMEOUT_MS = 500;

    private transient List<ExternalRuntimeTcpConfig.ExternalRuntimeEndpoint> endpoints;
    private transient List<BufferedInputStream> ins;
    private transient List<BufferedOutputStream> outs;
    private transient List<Socket> sockets;
    private transient List<SocketChannel> channels;
    private transient List<ChannelFrameState> channelFrameStates;
    private transient Selector selector;
    private transient long expectedRowId;
    private transient Long2ObjectOpenHashMap<ResponseBlock> dynamicBlockBuffer;
    private transient boolean dynamicRoutingEnabled;
    private transient long[] endpointReconnectAtNanos;
    private transient String configJson;
    private transient boolean reuseObjects;
    private transient GenericRowData reuseRow;
    private transient byte[] frameBuf;
    private transient int frameDataPos;
    private transient int frameDataCount;
    private transient StreamRecord<RowData> reuseStreamRecord;
    private transient int cachedConnectedCount;
    private transient ArrayDeque<Integer> readyEndpointQueue;
    private transient boolean[] readyEndpointQueued;
    private transient int lastFrameLen;
    private transient int routingWidthHint;
    private transient long[] lastMatchedTargetRowIdByEndpoint;
    private transient boolean threadedReadEnabled;
    private transient ArrayBlockingQueue<ResponseBlock> asyncResponseQueue;
    private transient List<Thread> responseReaderThreads;
    private transient volatile boolean responseReadersRunning;
    private transient AtomicReference<IOException> asyncReadFailure;
    private transient boolean consecutiveMissingSkipMode;
    private transient long lastSkippedMissingRowId;

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
        this.dynamicRoutingEnabled = tcpConfig.isAutoParallelismEnabled();
        this.threadedReadEnabled = dynamicRoutingEnabled;
        this.ins = new ArrayList<>(endpoints.size());
        this.outs = new ArrayList<>(endpoints.size());
        this.sockets = new ArrayList<>(endpoints.size());
        this.channels = dynamicRoutingEnabled && !threadedReadEnabled ? new ArrayList<>(endpoints.size()) : null;
        this.channelFrameStates =
                dynamicRoutingEnabled && !threadedReadEnabled ? new ArrayList<>(endpoints.size()) : null;
        this.selector = dynamicRoutingEnabled && !threadedReadEnabled ? Selector.open() : null;
        this.readyEndpointQueue =
                dynamicRoutingEnabled && !threadedReadEnabled ? new ArrayDeque<>() : null;
        this.readyEndpointQueued =
                dynamicRoutingEnabled && !threadedReadEnabled ? new boolean[endpoints.size()] : null;
        this.lastMatchedTargetRowIdByEndpoint = dynamicRoutingEnabled ? new long[endpoints.size()] : null;
        this.endpointReconnectAtNanos = new long[endpoints.size()];
        this.cachedConnectedCount = 0;
        this.configJson = buildConfigJson();
        this.asyncResponseQueue =
                threadedReadEnabled
                        ? new ArrayBlockingQueue<>(Math.max(1024, tcpConfig.getReorderMaxBuffer() * 2))
                        : null;
        this.responseReaderThreads = threadedReadEnabled ? new ArrayList<>(endpoints.size()) : null;
        this.responseReadersRunning = threadedReadEnabled;
        this.asyncReadFailure = threadedReadEnabled ? new AtomicReference<>() : null;
        this.consecutiveMissingSkipMode = false;
        this.lastSkippedMissingRowId = -1L;

        for (int i = 0; i < endpoints.size(); i++) {
            ins.add(null);
            outs.add(null);
            sockets.add(null);
            if (dynamicRoutingEnabled && !threadedReadEnabled) {
                channels.add(null);
                channelFrameStates.add(new ChannelFrameState());
            }
            endpointReconnectAtNanos[i] = 0L;
            if (dynamicRoutingEnabled) {
                lastMatchedTargetRowIdByEndpoint[i] = -1L;
            }
        }

        for (int i = 0; i < endpoints.size(); i++) {
            try {
                connectEndpoint(i);
            } catch (IOException e) {
                if (!dynamicRoutingEnabled) {
                    throw e;
                }
                endpointReconnectAtNanos[i] = System.nanoTime()
                        + tcpConfig.getFailoverReconnectBackoffMs() * 1_000_000L;
                LOG.warn(
                        "ExternalRuntimePostOperator could not connect to endpoint {}:{} at open; will retry.",
                        endpoints.get(i).getHost(),
                        endpoints.get(i).getReceivePort(),
                        e);
            }
        }
        if (cachedConnectedCount == 0) {
            throw new IOException("ExternalRuntimePostOperator could not connect to any runtime endpoint.");
        }

        this.reuseObjects = getRuntimeContext().isObjectReuseEnabled();

        this.codec = new ExternalRuntimeBinaryCodec(
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
        if (dynamicRoutingEnabled) {
            this.dynamicBlockBuffer = new Long2ObjectOpenHashMap<>();
        } else {
            this.dynamicBlockBuffer = null;
        }
        this.lastFrameLen = 0;
        this.routingWidthHint = dynamicRoutingEnabled ? 1 : 0;
        if (threadedReadEnabled) {
            startResponseReaderThreads();
        }

        LOG.info(
                "ExternalRuntimePostOperator connected to {} runtime(s) (rowType={}, sentConfigBytes={})",
                connectedEndpointCount(),
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
        final long timestamp = element.hasTimestamp() ? element.getTimestamp() : 0L;
        if (dynamicRoutingEnabled) {
            if (threadedReadEnabled) {
                readNextDynamicBlockFromQueue(fallbackKind, expectedRowId, timestamp);
                expectedRowId++;
                return;
            }
            if (channels == null || channels.isEmpty()) {
                throw new IOException("ExternalRuntimePostOperator dynamic channels not initialized");
            }
            readNextDynamicBlockDirect(fallbackKind, expectedRowId, timestamp);
            expectedRowId++;
            return;
        }
        if (ins == null || ins.isEmpty()) {
            throw new IOException("ExternalRuntimePostOperator input stream not initialized");
        }

        final int endpointIndex = tcpConfig.selectEndpointIndex(expectedRowId, ins.size());
        readAndEmitBlock(endpointIndex, fallbackKind, expectedRowId, timestamp);
        expectedRowId++;
    }

    private void readNextDynamicBlockFromQueue(RowKind fallbackKind, long targetRowId, long timestamp)
            throws IOException {
        if (shouldSkipMissingRowImmediately(targetRowId)) {
            if (tryEmitTargetFromBufferedOrReadyQueue(fallbackKind, targetRowId, timestamp)) {
                clearMissingSkipState();
                return;
            }
            markMissingRowSkipped(targetRowId);
            return;
        }
        final long waitStartNanos = System.nanoTime();
        if (tryEmitTargetFromBufferedOrReadyQueue(fallbackKind, targetRowId, timestamp)) {
            clearMissingSkipState();
            return;
        }
        // If newer blocks are already buffered and target rowId is still missing, waiting the full
        // timeout would throttle all faster endpoints via head-of-line blocking.
        if (!dynamicBlockBuffer.isEmpty() && hasDisconnectedEndpoint()) {
            LOG.warn(
                    "ExternalRuntimePostOperator skipping missing rowId {} immediately because newer buffered rows already exist.",
                    targetRowId);
            markMissingRowSkipped(targetRowId);
            return;
        }

        while (true) {
            final IOException failure =
                    asyncReadFailure == null ? null : asyncReadFailure.getAndSet(null);
            if (failure != null) {
                throw failure;
            }
            final ResponseBlock block;
            try {
                block = asyncResponseQueue.poll(10L, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for async response block.", e);
            }
            if (block == null) {
                if (hasMissingRowTimedOut(waitStartNanos)) {
                    LOG.warn(
                            "ExternalRuntimePostOperator skipping missing rowId {} after {} ms timeout.",
                            targetRowId,
                            DYNAMIC_MISSING_ROW_TIMEOUT_MS);
                    markMissingRowSkipped(targetRowId);
                    return;
                }
                continue;
            }
            if (block.rowId == targetRowId) {
                emitBufferedBlock(block, fallbackKind, timestamp);
                clearMissingSkipState();
                return;
            }
            bufferBlock(block, targetRowId);
            // As soon as we observe a newer rowId, skip the missing target immediately instead of
            // waiting the timeout and slowing down healthy/faster runtimes.
            if (block.rowId > targetRowId && hasDisconnectedEndpoint()) {
                LOG.warn(
                        "ExternalRuntimePostOperator skipping missing rowId {} immediately after observing newer rowId {}.",
                        targetRowId,
                        block.rowId);
                markMissingRowSkipped(targetRowId);
                return;
            }
            if (hasDisconnectedEndpoint() && hasMissingRowTimedOut(waitStartNanos)) {
                LOG.warn(
                        "ExternalRuntimePostOperator skipping missing rowId {} after {} ms timeout.",
                        targetRowId,
                        DYNAMIC_MISSING_ROW_TIMEOUT_MS);
                markMissingRowSkipped(targetRowId);
                return;
            }
        }
    }

    private boolean tryEmitTargetFromBufferedOrReadyQueue(
            RowKind fallbackKind, long targetRowId, long timestamp) throws IOException {
        if (!dynamicBlockBuffer.isEmpty()) {
            final ResponseBlock buffered = dynamicBlockBuffer.remove(targetRowId);
            if (buffered != null) {
                emitBufferedBlock(buffered, fallbackKind, timestamp);
                return true;
            }
        }
        if (asyncResponseQueue == null) {
            return false;
        }
        int drained = 0;
        while (drained < 64) {
            final ResponseBlock block = asyncResponseQueue.poll();
            if (block == null) {
                break;
            }
            if (block.rowId == targetRowId) {
                emitBufferedBlock(block, fallbackKind, timestamp);
                return true;
            }
            bufferBlock(block, targetRowId);
            drained++;
        }
        return false;
    }

    private boolean shouldSkipMissingRowImmediately(long targetRowId) {
        if (!hasDisconnectedEndpoint()) {
            return false;
        }
        if (!consecutiveMissingSkipMode) {
            return false;
        }
        return targetRowId == lastSkippedMissingRowId + 1L;
    }

    private void markMissingRowSkipped(long targetRowId) {
        consecutiveMissingSkipMode = true;
        lastSkippedMissingRowId = targetRowId;
    }

    private void clearMissingSkipState() {
        consecutiveMissingSkipMode = false;
        lastSkippedMissingRowId = -1L;
    }

    private boolean hasMissingRowTimedOut(long waitStartNanos) {
        final long elapsedNanos = System.nanoTime() - waitStartNanos;
        return elapsedNanos >= DYNAMIC_MISSING_ROW_TIMEOUT_MS * 1_000_000L;
    }

    private boolean hasDisconnectedEndpoint() {
        if (sockets == null || sockets.isEmpty()) {
            return true;
        }
        for (int i = 0; i < sockets.size(); i++) {
            if (!isEndpointConnected(i)) {
                return true;
            }
        }
        return false;
    }

    private void startResponseReaderThreads() {
        for (int i = 0; i < endpoints.size(); i++) {
            final int endpointIndex = i;
            final Thread reader =
                    new Thread(
                            () -> runResponseReader(endpointIndex),
                            "external-runtime-post-read-" + endpointIndex);
            reader.setDaemon(true);
            responseReaderThreads.add(reader);
            reader.start();
        }
    }

    private void runResponseReader(int endpointIndex) {
        while (responseReadersRunning) {
            try {
                if (!isEndpointConnected(endpointIndex)) {
                    maybeReconnectOneEndpoint(endpointIndex);
                    TimeUnit.MILLISECONDS.sleep(2L);
                    continue;
                }
                final BufferedInputStream in = ins.get(endpointIndex);
                if (in == null) {
                    TimeUnit.MILLISECONDS.sleep(2L);
                    continue;
                }
                final int batchLen = readIntBE(in);
                if (batchLen <= 0 || batchLen > MAX_FRAME_BYTES) {
                    throw new IOException(
                            "ExternalRuntimePostOperator received invalid frame size: " + batchLen);
                }
                final byte[] payload = new byte[batchLen];
                readFully(in, payload, 0, batchLen);
                final long blockRowId = parseBlockRowId(payload);
                while (responseReadersRunning) {
                    if (asyncResponseQueue.offer(
                            new ResponseBlock(blockRowId, payload, endpointIndex),
                            10L,
                            TimeUnit.MILLISECONDS)) {
                        break;
                    }
                }
            } catch (IOException io) {
                if (io instanceof SocketTimeoutException) {
                    continue;
                }
                markEndpointDisconnected(endpointIndex, io);
                if (!dynamicRoutingEnabled && asyncReadFailure != null) {
                    asyncReadFailure.compareAndSet(null, io);
                    return;
                }
            } catch (InterruptedException ie) {
                if (!responseReadersRunning) {
                    return;
                }
            }
        }
    }

    private long parseBlockRowId(byte[] payload) throws IOException {
        if (payload.length < 12) {
            throw new IOException("ExternalRuntimePostOperator received truncated frame");
        }
        return readLongBE(payload, 4);
    }

    private void maybeReconnectOneEndpoint(int endpointIndex) {
        final long now = System.nanoTime();
        if (endpointReconnectAtNanos[endpointIndex] > now) {
            return;
        }
        try {
            connectEndpoint(endpointIndex);
        } catch (IOException e) {
            endpointReconnectAtNanos[endpointIndex] =
                    now + (long) tcpConfig.getFailoverReconnectBackoffMs() * 1_000_000L;
            if (!dynamicRoutingEnabled && asyncReadFailure != null) {
                asyncReadFailure.compareAndSet(null, e);
            }
        }
    }

    private void readNextDynamicBlockDirect(
            RowKind fallbackKind, long targetRowId, long timestamp)
            throws IOException {
        // Check buffer for previously read out-of-order blocks
        if (!dynamicBlockBuffer.isEmpty()) {
            final ResponseBlock buffered = dynamicBlockBuffer.remove(targetRowId);
            if (buffered != null) {
                emitBufferedBlock(buffered, fallbackKind, timestamp);
                observeMatchedTargetEndpoint(buffered.endpointIndex, targetRowId);
                maybeRefreshRoutingWidthHint(targetRowId);
                return;
            }
        }

        if (tryReadHintedEndpoint(fallbackKind, targetRowId, timestamp)) {
            return;
        }

        while (true) {
            final int endpointIndex = pollNextReadyEndpoint();
            if (endpointIndex < 0) {
                if (tryReadHintedEndpoint(fallbackKind, targetRowId, timestamp)) {
                    return;
                }
                continue;
            }
            try {
                final Long blockRowId = tryReadFrameIntoBufferFromChannel(endpointIndex);
                if (blockRowId == null) {
                    continue;
                }
                if (blockRowId == targetRowId) {
                    emitRowsFromBuffer(fallbackKind, timestamp);
                    observeMatchedTargetEndpoint(endpointIndex, targetRowId);
                    maybeRefreshRoutingWidthHint(targetRowId);
                    return;
                }
                bufferBlock(materializeBlockFromBuffer(blockRowId, endpointIndex), targetRowId);
            } catch (IOException io) {
                markEndpointDisconnected(endpointIndex, io);
            }
        }
    }

    private boolean tryReadHintedEndpoint(RowKind fallbackKind, long targetRowId, long timestamp)
            throws IOException {
        if (channels == null || channels.isEmpty()) {
            return false;
        }
        final int width = Math.max(1, Math.min(routingWidthHint, channels.size()));
        final int endpointIndex = Math.floorMod(targetRowId, width);
        if (!isEndpointConnected(endpointIndex)) {
            return false;
        }
        try {
            final Long blockRowId = tryReadFrameIntoBufferFromChannel(endpointIndex);
            if (blockRowId == null) {
                return false;
            }
            if (blockRowId == targetRowId) {
                emitRowsFromBuffer(fallbackKind, timestamp);
                observeMatchedTargetEndpoint(endpointIndex, targetRowId);
                maybeRefreshRoutingWidthHint(targetRowId);
                return true;
            }
            bufferBlock(materializeBlockFromBuffer(blockRowId, endpointIndex), targetRowId);
            return false;
        } catch (IOException io) {
            markEndpointDisconnected(endpointIndex, io);
            return false;
        }
    }

    private Long tryReadFrameIntoBufferFromChannel(int endpointIndex) throws IOException {
        final SocketChannel channel = channels.get(endpointIndex);
        if (channel == null) {
            throw new IOException(
                    "ExternalRuntimePostOperator endpoint "
                            + endpointIndex
                            + " channel not initialized");
        }
        final ChannelFrameState state = channelFrameStates.get(endpointIndex);

        if (!state.readingPayload) {
            while (state.lenBuf.hasRemaining()) {
                final int lenRead = channel.read(state.lenBuf);
                if (lenRead < 0) {
                    throw new IOException("EOF while reading dynamic frame length");
                }
                if (lenRead == 0) {
                    return null;
                }
            }
            state.lenBuf.flip();
            final int batchLen = state.lenBuf.getInt();
            state.lenBuf.clear();
            if (batchLen <= 0 || batchLen > MAX_FRAME_BYTES) {
                throw new IOException(
                        "ExternalRuntimePostOperator received invalid frame size: " + batchLen);
            }
            state.ensurePayloadCapacity(batchLen);
            state.payloadBuf.clear();
            state.payloadBuf.limit(batchLen);
            state.readingPayload = true;
        }

        while (state.payloadBuf.hasRemaining()) {
            final int payloadRead = channel.read(state.payloadBuf);
            if (payloadRead < 0) {
                throw new IOException("EOF while reading dynamic frame payload");
            }
            if (payloadRead == 0) {
                return null;
            }
        }

        state.payloadBuf.flip();
        final int batchLen = state.payloadBuf.remaining();
        lastFrameLen = batchLen;

        // Hot path: avoid copying the whole frame into a separate byte[] on every
        // block.
        // payloadBuf is a heap ByteBuffer with a backing array (see ChannelFrameState),
        // so we can
        // decode directly from that array. This removes one full memcpy per response
        // block, which
        // is a major throughput win when auto-parallelism enables dynamic routing.
        frameBuf = state.payloadBuf.array();

        state.readingPayload = false;

        return parseFrameHeaderFromFrameBuf();
    }

    /**
     * Emits rows directly from frameBuf without allocating ResponseBlock/ArrayList.
     */
    private void emitRowsFromBuffer(RowKind fallbackKind, long timestamp) throws IOException {
        int pos = frameDataPos;
        final int count = frameDataCount;
        final GenericRowData rowToEmit = reuseObjects ? reuseRow : null;
        for (int i = 0; i < count; i++) {
            pos = decodeAndEmitRow(
                    frameBuf, pos, fallbackKind, rowToEmit, reuseStreamRecord, timestamp);
        }
    }

    /**
     * Copies the current frame for out-of-order buffering without decoding rows
     * eagerly.
     */
    private ResponseBlock materializeBlockFromBuffer(long blockRowId, int endpointIndex) {
        final byte[] payload = new byte[lastFrameLen];
        System.arraycopy(frameBuf, 0, payload, 0, lastFrameLen);
        return new ResponseBlock(blockRowId, payload, endpointIndex);
    }

    private void emitBufferedBlock(ResponseBlock block, RowKind fallbackKind, long timestamp)
            throws IOException {
        ensureFrameBuf(block.payload.length);
        System.arraycopy(block.payload, 0, frameBuf, 0, block.payload.length);
        lastFrameLen = block.payload.length;
        final long blockRowId = parseFrameHeaderFromFrameBuf();
        if (blockRowId != block.rowId) {
            throw new IOException(
                    "ExternalRuntimePostOperator buffered block rowId mismatch: expected "
                            + block.rowId
                            + " but parsed "
                            + blockRowId);
        }
        emitRowsFromBuffer(fallbackKind, timestamp);
    }

    private void observeMatchedTargetEndpoint(int endpointIndex, long targetRowId) {
        if (lastMatchedTargetRowIdByEndpoint == null
                || endpointIndex < 0
                || endpointIndex >= lastMatchedTargetRowIdByEndpoint.length) {
            return;
        }
        lastMatchedTargetRowIdByEndpoint[endpointIndex] = targetRowId;
    }

    private void maybeRefreshRoutingWidthHint(long targetRowId) {
        if (lastMatchedTargetRowIdByEndpoint == null || lastMatchedTargetRowIdByEndpoint.length == 0) {
            return;
        }
        final long cutoff = targetRowId - ROUTING_HINT_RECENCY_ROWS;
        int widest = 1;
        for (int i = lastMatchedTargetRowIdByEndpoint.length - 1; i >= 0; i--) {
            if (lastMatchedTargetRowIdByEndpoint[i] >= cutoff) {
                widest = i + 1;
                break;
            }
        }
        routingWidthHint = Math.max(1, widest);
    }

    /** Polls selector for one readable endpoint, returns index or -1 on timeout. */
    private int pollNextReadyEndpoint() throws IOException {
        maybeReconnectEndpoints();
        updateConnectedEndpointCount();

        if (cachedConnectedCount <= 0) {
            maybeReconnectEndpoints();
            if (cachedConnectedCount <= 0) {
                throw new IOException(
                        "ExternalRuntimePostOperator has no connected runtime endpoints.");
            }
        }

        if (selector == null) {
            return -1;
        }

        if (readyEndpointQueue != null) {
            while (!readyEndpointQueue.isEmpty()) {
                final int endpointIndex = readyEndpointQueue.pollFirst();
                if (readyEndpointQueued != null && endpointIndex >= 0 && endpointIndex < readyEndpointQueued.length) {
                    readyEndpointQueued[endpointIndex] = false;
                }
                if (isEndpointConnected(endpointIndex)) {
                    return endpointIndex;
                }
            }
        }

        // First do a non-blocking poll; in steady state this avoids a blocking select()
        // call per row.
        int ready = selector.selectNow();
        if (ready <= 0) {
            // Fall back to a blocking select with the configured timeout.
            ready = selector.select(tcpConfig.getFailoverPollTimeoutMs());
        }
        if (ready <= 0) {
            maybeReconnectEndpoints();
            return -1;
        }

        final Iterator<SelectionKey> it = selector.selectedKeys().iterator();
        while (it.hasNext()) {
            final SelectionKey key = it.next();
            it.remove();
            if (!key.isValid() || !key.isReadable()) {
                continue;
            }
            final int endpointIndex = (Integer) key.attachment();
            if (isEndpointConnected(endpointIndex)) {
                if (readyEndpointQueued == null
                        || endpointIndex < 0
                        || endpointIndex >= readyEndpointQueued.length
                        || !readyEndpointQueued[endpointIndex]) {
                    readyEndpointQueue.addLast(endpointIndex);
                    if (readyEndpointQueued != null
                            && endpointIndex >= 0
                            && endpointIndex < readyEndpointQueued.length) {
                        readyEndpointQueued[endpointIndex] = true;
                    }
                }
            }
        }
        while (readyEndpointQueue != null && !readyEndpointQueue.isEmpty()) {
            final int endpointIndex = readyEndpointQueue.pollFirst();
            if (readyEndpointQueued != null && endpointIndex >= 0 && endpointIndex < readyEndpointQueued.length) {
                readyEndpointQueued[endpointIndex] = false;
            }
            if (isEndpointConnected(endpointIndex)) {
                return endpointIndex;
            }
        }
        maybeReconnectEndpoints();
        return -1;
    }

    private void bufferBlock(ResponseBlock block, long targetRowId) throws IOException {
        if (block.rowId < targetRowId) {
            LOG.debug(
                    "ExternalRuntimePostOperator dropping stale dynamic block rowId={} (targetRowId={}).",
                    block.rowId,
                    targetRowId);
            return;
        }
        dynamicBlockBuffer.put(block.rowId, block);
        if (dynamicBlockBuffer.size() > tcpConfig.getReorderMaxBuffer()) {
            throw new IOException(
                    "ExternalRuntimePostOperator dynamic reorder buffer exceeded "
                            + tcpConfig.getReorderMaxBuffer()
                            + " entries; increase reorderMax or reduce failover lag.");
        }
    }

    private long parseFrameHeaderFromFrameBuf() throws IOException {
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

        this.frameDataPos = pos;
        this.frameDataCount = count;
        return blockRowId;
    }

    private void updateConnectedEndpointCount() {
        int count = 0;
        for (int i = 0; i < endpoints.size(); i++) {
            if (isEndpointConnected(i)) {
                count++;
            }
        }
        cachedConnectedCount = count;
    }

    private int connectedEndpointCount() {
        if (sockets == null) {
            return 0;
        }
        int connected = 0;
        for (int i = 0; i < sockets.size(); i++) {
            if (isEndpointConnected(i)) {
                connected++;
            }
        }
        return connected;
    }

    private boolean isEndpointConnected(int endpointIndex) {
        if (endpointIndex < 0 || endpointIndex >= sockets.size()) {
            return false;
        }
        if (dynamicRoutingEnabled) {
            if (threadedReadEnabled) {
                return sockets.get(endpointIndex) != null
                        && ins.get(endpointIndex) != null
                        && outs.get(endpointIndex) != null;
            }
            return sockets.get(endpointIndex) != null
                    && outs.get(endpointIndex) != null
                    && channels.get(endpointIndex) != null;
        }
        return endpointIndex >= 0
                && endpointIndex < sockets.size()
                && sockets.get(endpointIndex) != null
                && ins.get(endpointIndex) != null
                && outs.get(endpointIndex) != null;
    }

    private void maybeReconnectEndpoints() {
        if (!dynamicRoutingEnabled || threadedReadEnabled) {
            return;
        }
        final long now = System.nanoTime();
        for (int i = 0; i < endpoints.size(); i++) {
            if (isEndpointConnected(i) || endpointReconnectAtNanos[i] > now) {
                continue;
            }
            try {
                connectEndpoint(i);
            } catch (IOException e) {
                endpointReconnectAtNanos[i] = now + (long) tcpConfig.getFailoverReconnectBackoffMs() * 1_000_000L;
                LOG.debug(
                        "ExternalRuntimePostOperator reconnect attempt failed for endpoint {}:{}",
                        endpoints.get(i).getHost(),
                        endpoints.get(i).getReceivePort(),
                        e);
            }
        }
    }

    private void connectEndpoint(int endpointIndex) throws IOException {
        final ExternalRuntimeTcpConfig.ExternalRuntimeEndpoint endpoint = endpoints.get(endpointIndex);

        if (dynamicRoutingEnabled && !threadedReadEnabled) {
            SocketChannel candidateChannel = null;
            Socket candidateSocket = null;
            BufferedOutputStream candidateOut = null;
            try {
                candidateChannel = SocketChannel.open();
                candidateChannel.configureBlocking(true);
                candidateSocket = candidateChannel.socket();
                candidateSocket.setTcpNoDelay(true);
                candidateSocket.connect(
                        new InetSocketAddress(endpoint.getHost(), endpoint.getReceivePort()),
                        tcpConfig.getConnectTimeoutMs());

                candidateOut = new BufferedOutputStream(
                        candidateSocket.getOutputStream(), tcpConfig.getBufferSize());
                writeLengthPrefixedJson(candidateOut, configJson);
                candidateOut.flush();

                candidateChannel.configureBlocking(false);
                selector.wakeup();
                candidateChannel.register(selector, SelectionKey.OP_READ, endpointIndex);

                closeQuietly(channels.get(endpointIndex));
                closeQuietly(outs.get(endpointIndex));
                closeQuietly(sockets.get(endpointIndex));

                channels.set(endpointIndex, candidateChannel);
                ins.set(endpointIndex, null);
                outs.set(endpointIndex, candidateOut);
                sockets.set(endpointIndex, candidateSocket);
                endpointReconnectAtNanos[endpointIndex] = 0L;
                refreshPrimarySocket();
                updateConnectedEndpointCount();
                return;
            } catch (IOException e) {
                closeQuietly(candidateOut);
                closeQuietly(candidateSocket);
                closeQuietly(candidateChannel);
                throw e;
            }
        }

        Socket candidateSocket = null;
        BufferedInputStream candidateIn = null;
        BufferedOutputStream candidateOut = null;
        try {
            candidateSocket = connectSocket(
                    endpoint.getHost(),
                    endpoint.getReceivePort(),
                    tcpConfig.getConnectTimeoutMs());

            final int readTimeoutMs = tcpConfig.getReadTimeoutMs() > 0
                    ? tcpConfig.getReadTimeoutMs()
                    : 0;
            if (readTimeoutMs > 0) {
                candidateSocket.setSoTimeout(readTimeoutMs);
            }

            candidateIn = new BufferedInputStream(candidateSocket.getInputStream(), tcpConfig.getBufferSize());
            candidateOut = new BufferedOutputStream(candidateSocket.getOutputStream(), tcpConfig.getBufferSize());
            writeLengthPrefixedJson(candidateOut, configJson);
            candidateOut.flush();

            closeQuietly(ins.get(endpointIndex));
            closeQuietly(outs.get(endpointIndex));
            closeQuietly(sockets.get(endpointIndex));

            ins.set(endpointIndex, candidateIn);
            outs.set(endpointIndex, candidateOut);
            sockets.set(endpointIndex, candidateSocket);
            endpointReconnectAtNanos[endpointIndex] = 0L;
            refreshPrimarySocket();
            updateConnectedEndpointCount();
        } catch (IOException e) {
            closeQuietly(candidateIn);
            closeQuietly(candidateOut);
            closeQuietly(candidateSocket);
            throw e;
        }
    }

    private void markEndpointDisconnected(int endpointIndex, IOException cause) {
        final ExternalRuntimeTcpConfig.ExternalRuntimeEndpoint endpoint = endpoints.get(endpointIndex);
        if (dynamicRoutingEnabled) {
            if (!threadedReadEnabled) {
                closeQuietly(channels.get(endpointIndex));
                channels.set(endpointIndex, null);
                final ChannelFrameState frameState = channelFrameStates.get(endpointIndex);
                frameState.lenBuf.clear();
                frameState.readingPayload = false;
                if (frameState.payloadBuf != null) {
                    frameState.payloadBuf.clear();
                }
                if (readyEndpointQueue != null) {
                    readyEndpointQueue.removeIf(idx -> idx == endpointIndex);
                }
                if (readyEndpointQueued != null
                        && endpointIndex >= 0
                        && endpointIndex < readyEndpointQueued.length) {
                    readyEndpointQueued[endpointIndex] = false;
                }
            }
        }
        closeQuietly(ins.get(endpointIndex));
        closeQuietly(outs.get(endpointIndex));
        closeQuietly(sockets.get(endpointIndex));
        ins.set(endpointIndex, null);
        outs.set(endpointIndex, null);
        sockets.set(endpointIndex, null);
        endpointReconnectAtNanos[endpointIndex] = System.nanoTime()
                + (long) tcpConfig.getFailoverReconnectBackoffMs() * 1_000_000L;
        refreshPrimarySocket();
        updateConnectedEndpointCount();
        if (isExpectedDisconnect(cause)) {
            LOG.info(
                    "ExternalRuntimePostOperator disconnected endpoint {}:{} ({}); will retry.",
                    endpoint.getHost(),
                    endpoint.getReceivePort(),
                    summarizeDisconnect(cause));
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "ExternalRuntimePostOperator expected disconnect details for endpoint {}:{}.",
                        endpoint.getHost(),
                        endpoint.getReceivePort(),
                        cause);
            }
            return;
        }
        LOG.warn(
                "ExternalRuntimePostOperator disconnected endpoint {}:{}; will retry.",
                endpoint.getHost(),
                endpoint.getReceivePort(),
                cause);
    }

    private boolean isExpectedDisconnect(IOException cause) {
        if (cause == null) {
            return false;
        }
        if (cause instanceof EOFException) {
            return true;
        }
        final String msg = cause.getMessage();
        if (msg == null || msg.isEmpty()) {
            return false;
        }
        return msg.contains("Connection reset") || msg.contains("Broken pipe");
    }

    private String summarizeDisconnect(IOException cause) {
        if (cause == null) {
            return "unknown";
        }
        final String message = cause.getMessage();
        if (message == null || message.isEmpty()) {
            return cause.getClass().getSimpleName();
        }
        return message;
    }

    private void refreshPrimarySocket() {
        socket = null;
        if (sockets == null) {
            return;
        }
        for (Socket candidate : sockets) {
            if (candidate != null) {
                socket = candidate;
                return;
            }
        }
    }

    private void readAndEmitBlock(
            int endpointIndex,
            RowKind fallbackKind,
            long targetRowId,
            long timestamp)
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

        if (reuseObjects && reuseStreamRecord != null) {
            for (int i = 0; i < count; i++) {
                pos = decodeAndEmitRow(frameBuf, pos, fallbackKind, rowToEmit, reuseStreamRecord, timestamp);
            }
        } else {
            for (int i = 0; i < count; i++) {
                pos = decodeAndEmitRow(
                        frameBuf, pos, fallbackKind, rowToEmit, reuseStreamRecord, timestamp);
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

        final GenericRowData outRow = reuseRow != null && reuseRow.getArity() == nFields
                ? reuseRow
                : new GenericRowData(nFields);

        for (int i = 0; i < nFields; i++) {
            if (isNullBitSet(buffer, nullBitmapPos, i)) {
                outRow.setField(i, null);
                continue;
            }

            final WireType wt = resultWireTypes[i];
            pos = decodeFieldValueAndAdvance(buffer, pos, wt, resultReadTypes.get(i), resultFieldTypes.get(i), outRow,
                    i);
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
                    final org.apache.flink.table.types.logical.DecimalType dt = (org.apache.flink.table.types.logical.DecimalType) targetType;
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
                    final org.apache.flink.table.types.logical.DecimalType dt = (org.apache.flink.table.types.logical.DecimalType) targetType;
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

    private static final class ResponseBlock {
        private final long rowId;
        private final byte[] payload;
        private final int endpointIndex;

        private ResponseBlock(long rowId, byte[] payload, int endpointIndex) {
            this.rowId = rowId;
            this.payload = payload;
            this.endpointIndex = endpointIndex;
        }
    }

    private static final class ChannelFrameState {
        private final ByteBuffer lenBuf = ByteBuffer.allocate(4);
        private ByteBuffer payloadBuf = ByteBuffer.allocate(8 * 1024);
        private boolean readingPayload;

        private void ensurePayloadCapacity(int payloadSize) {
            if (payloadBuf.capacity() >= payloadSize) {
                return;
            }
            int cap = payloadBuf.capacity();
            while (cap < payloadSize) {
                cap <<= 1;
            }
            payloadBuf = ByteBuffer.allocate(cap);
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

    private void stopResponseReaderThreads() {
        responseReadersRunning = false;
        if (responseReaderThreads != null) {
            for (int i = 0; i < responseReaderThreads.size(); i++) {
                final Thread reader = responseReaderThreads.get(i);
                if (reader != null) {
                    reader.interrupt();
                }
            }
            for (int i = 0; i < responseReaderThreads.size(); i++) {
                final Thread reader = responseReaderThreads.get(i);
                if (reader == null) {
                    continue;
                }
                try {
                    reader.join(1000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            responseReaderThreads.clear();
        }
    }

    @Override
    protected void closeInternal() throws Exception {
        IOException error = null;

        stopResponseReaderThreads();

        codec = null;
        reuseRow = null;
        reuseStreamRecord = null;
        frameBuf = null;
        dynamicBlockBuffer = null;
        endpointReconnectAtNanos = null;
        configJson = null;
        dynamicRoutingEnabled = false;
        cachedConnectedCount = 0;
        lastFrameLen = 0;
        routingWidthHint = 0;
        threadedReadEnabled = false;
        asyncResponseQueue = null;
        asyncReadFailure = null;
        consecutiveMissingSkipMode = false;
        lastSkippedMissingRowId = -1L;

        if (channels != null) {
            for (SocketChannel channel : channels) {
                error = suppress(error, closeQuietly(channel));
            }
        }
        channels = null;
        channelFrameStates = null;
        responseReaderThreads = null;

        if (selector != null) {
            error = suppress(error, closeQuietly(selector));
        }
        selector = null;
        if (readyEndpointQueue != null) {
            readyEndpointQueue.clear();
        }
        readyEndpointQueue = null;
        readyEndpointQueued = null;
        lastMatchedTargetRowIdByEndpoint = null;

        // Close sockets first to sever connections, preventing
        // BufferedOutputStream.close()
        // from attempting a flush on a broken pipe during shutdown.
        if (sockets != null) {
            for (Socket sock : sockets) {
                error = suppress(error, closeQuietly(sock));
            }
        }
        sockets = null;
        socket = null;

        // Now close streams; flush will fail harmlessly since sockets are already
        // closed.
        if (outs != null) {
            for (BufferedOutputStream out : outs) {
                closeQuietly(out);
            }
        }
        outs = null;

        if (ins != null) {
            for (BufferedInputStream in : ins) {
                closeQuietly(in);
            }
        }
        ins = null;

        if (error != null) {
            throw error;
        }
    }
}
