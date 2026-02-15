package org.apache.flink.table.runtime.functions.table.externalruntime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** PRE: sends input rows to the external runtime and emits placeholders. */
@Internal
public final class ExternalRuntimePreOperator extends ExternalRuntimeOperator
        implements BoundedOneInput {

    private static final long serialVersionUID = 1L;
    private static final long AUTO_SCALE_EVAL_INTERVAL_NANOS = 500_000_000L;
    private static final long AUTO_SCALE_UP_COOLDOWN_NANOS = 2_000_000_000L;
    private static final long AUTO_SCALE_DOWN_COOLDOWN_NANOS = 8_000_000_000L;
    private static final long AUTO_SCALE_IDLE_WINDOW_THRESHOLD = 16;
    private static final long AUTO_BATCH_LINGER_NANOS = 5_000_000L;
    private static final long AUTO_BATCH_FLUSH_CHECK_INTERVAL_NANOS = 1_000_000L;
    private static final int AUTO_SCALE_EVAL_ROW_INTERVAL = 8192;
    private static final int ASYNC_SEND_QUEUE_CAPACITY = 8192;
    private static final long ASYNC_SEND_ENQUEUE_TIMEOUT_MS = 25L;
    private static final long ASYNC_DRAIN_WAIT_SLEEP_NANOS = 1_000_000L;

    private transient List<ExternalRuntimeTcpConfig.ExternalRuntimeEndpoint> endpoints;
    private transient List<EndpointState> endpointStates;
    private transient List<Integer> activeEndpointIndices;
    private transient int batchSize;
    private transient long nextRowId;
    private transient String configJson;
    private transient long autoWindowStartNanos;
    private transient long autoWindowRows;
    private transient long autoRowsSinceLastCheck;
    private transient long nextScaleUpAtNanos;
    private transient long idleWindowStreak;
    private transient long nextAutoBatchFlushCheckAtNanos;
    private transient List<ArrayBlockingQueue<PendingSendBatch>> endpointSendQueues;
    private transient ArrayBlockingQueue<SendFailure> asyncSendFailures;
    private transient AtomicInteger pendingSendBatches;
    private transient List<Thread> sendWorkerThreads;
    private transient volatile boolean sendWorkerRunning;
    private transient int nextFailoverEndpointCursor;

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
        this.endpointStates = new ArrayList<>(endpoints.size());
        this.activeEndpointIndices = new ArrayList<>(endpoints.size());
        this.batchSize = Math.max(1, tcpConfig.getBatchSize());
        this.configJson = buildConfigJson();
        this.endpointSendQueues = new ArrayList<>(endpoints.size());
        this.asyncSendFailures = new ArrayBlockingQueue<>(ASYNC_SEND_QUEUE_CAPACITY);
        this.pendingSendBatches = new AtomicInteger(0);
        this.sendWorkerThreads = new ArrayList<>(endpoints.size());
        this.sendWorkerRunning = true;
        this.nextFailoverEndpointCursor = 0;

        for (ExternalRuntimeTcpConfig.ExternalRuntimeEndpoint endpoint : endpoints) {
            endpointStates.add(new EndpointState(endpoint, tcpConfig.getBufferSize()));
            endpointSendQueues.add(new ArrayBlockingQueue<>(ASYNC_SEND_QUEUE_CAPACITY));
        }

        final int configuredParallelism = tcpConfig.getRuntimeParallelism();
        final int fixedActiveTarget =
                configuredParallelism > 0
                        ? Math.min(configuredParallelism, endpointStates.size())
                        : endpointStates.size();
        final int initialActiveTarget = tcpConfig.isAutoParallelismEnabled() ? 1 : fixedActiveTarget;
        activateEndpoints(initialActiveTarget);
        if (activeEndpointIndices.isEmpty()) {
            throw new IOException("ExternalRuntimePreOperator could not connect to any runtime endpoint.");
        }
        updatePrimarySocket();

        this.nextRowId = 0L;
        this.autoWindowStartNanos = System.nanoTime();
        this.autoWindowRows = 0L;
        this.autoRowsSinceLastCheck = 0L;
        this.nextScaleUpAtNanos = System.nanoTime() + AUTO_SCALE_UP_COOLDOWN_NANOS;
        this.idleWindowStreak = 0L;
        this.nextAutoBatchFlushCheckAtNanos = System.nanoTime() + AUTO_BATCH_FLUSH_CHECK_INTERVAL_NANOS;

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

        for (int i = 0; i < endpointStates.size(); i++) {
            final int endpointIndex = i;
            final ArrayBlockingQueue<PendingSendBatch> queue = endpointSendQueues.get(endpointIndex);
            final Thread worker =
                    new Thread(
                            () -> runSendWorker(endpointIndex, queue),
                            "external-runtime-pre-send-" + endpointIndex);
            worker.setDaemon(true);
            sendWorkerThreads.add(worker);
            worker.start();
        }

        LOG.info(
                "ExternalRuntimePreOperator connected to {} runtime(s) (rowType={}, sentConfigBytes={})",
                activeEndpointIndices.size(),
                inputRowType,
                configJson.getBytes(StandardCharsets.UTF_8).length);
    }

    @Override
    protected RowData processRow(RowData inRow) throws Exception {
        appendRowToBinary(inRow);
        return createPlaceholderRow(inRow.getRowKind());
    }

    @Override
    public void endInput() throws Exception {
        final IOException error = tryFlushRemaining();
        waitForPendingSends();
        drainAsyncSendFailures();
        if (error != null) {
            throw error;
        }
    }

    private void appendRowToBinary(RowData row) throws IOException {
        if (endpointStates == null || endpointStates.isEmpty()) {
            throw new IOException("ExternalRuntimePreOperator output stream not initialized");
        }
        drainAsyncSendFailures();
        if (tcpConfig.isAutoParallelismEnabled()) {
            autoRowsSinceLastCheck++;
            if (autoRowsSinceLastCheck >= AUTO_SCALE_EVAL_ROW_INTERVAL) {
                autoRowsSinceLastCheck = 0L;
                maybeAdjustAutoParallelism();
            }
        }
        final int endpointIndex = selectActiveEndpointIndex(nextRowId);
        final EndpointState endpointState = endpointStates.get(endpointIndex);
        final long nowNanos = System.nanoTime();
        if (endpointState.batchCount == 0) {
            endpointState.firstBufferedAtNanos = nowNanos;
        }
        codec.writeFramedRow(endpointState.batchBuffer, row, payloadFieldIndicesArray, nextRowId);
        endpointState.batchCount++;
        if (endpointState.batchCount >= batchSize) {
            flushBatch(endpointIndex);
        } else if (tcpConfig.isAutoParallelismEnabled()
                && activeEndpointIndices.size() > 1
                && nowNanos >= nextAutoBatchFlushCheckAtNanos) {
            flushStaleActiveBatches(nowNanos);
            nextAutoBatchFlushCheckAtNanos = nowNanos + AUTO_BATCH_FLUSH_CHECK_INTERVAL_NANOS;
        }
        nextRowId++;
        autoWindowRows++;
    }

    private int selectActiveEndpointIndex(long rowId) throws IOException {
        ensureActiveEndpoints();
        final int activeCount = activeEndpointIndices.size();
        final int routeIndex = tcpConfig.selectEndpointIndex(rowId, activeCount);
        return activeEndpointIndices.get(routeIndex);
    }

    private void flushBatch(int endpointIndex) throws IOException {
        flushBatch(endpointIndex, false);
    }

    private void flushBatch(int endpointIndex, boolean forceFlush) throws IOException {
        final EndpointState endpointState = endpointStates.get(endpointIndex);
        final ByteArrayOutputStream buffer = endpointState.batchBuffer;
        if (buffer.size() == 0) {
            endpointState.batchCount = 0;
            endpointState.firstBufferedAtNanos = 0L;
            return;
        }

        final int rowsInBatch = endpointState.batchCount;
        final byte[] payload = buffer.toByteArray();
        buffer.reset();
        endpointState.batchCount = 0;
        endpointState.firstBufferedAtNanos = 0L;

        enqueueSendBatch(endpointIndex, payload, rowsInBatch, forceFlush);
    }


    private void flushStaleActiveBatches(long nowNanos) throws IOException {
        if (activeEndpointIndices == null || activeEndpointIndices.isEmpty()) {
            return;
        }
        for (int i = 0; i < activeEndpointIndices.size(); i++) {
            final int endpointIndex = activeEndpointIndices.get(i);
            final EndpointState endpointState = endpointStates.get(endpointIndex);
            if (endpointState.batchCount <= 0 || endpointState.firstBufferedAtNanos <= 0L) {
                continue;
            }
            if (nowNanos - endpointState.firstBufferedAtNanos >= AUTO_BATCH_LINGER_NANOS) {
                flushBatch(endpointIndex, true);
            }
        }
    }

    private void enqueueSendBatch(int endpointIndex, byte[] payload, int rowsInBatch, boolean forceFlush)
            throws IOException {
        if (!sendWorkerRunning || endpointSendQueues == null) {
            throw new IOException("ExternalRuntimePreOperator async sender is not running.");
        }
        if (endpointIndex < 0 || endpointIndex >= endpointSendQueues.size()) {
            throw new IOException(
                    "ExternalRuntimePreOperator async sender endpoint index out of range: "
                            + endpointIndex);
        }
        drainAsyncSendFailures();
        final PendingSendBatch batch =
                new PendingSendBatch(endpointIndex, payload, rowsInBatch, forceFlush);
        final ArrayBlockingQueue<PendingSendBatch> sendQueue = endpointSendQueues.get(endpointIndex);
        if (sendQueue == null) {
            throw new IOException(
                    "ExternalRuntimePreOperator async sender queue not initialized for endpoint "
                            + endpointIndex);
        }
        try {
            if (sendQueue.offer(batch, ASYNC_SEND_ENQUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                pendingSendBatches.incrementAndGet();
                return;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while enqueueing async send batch.", e);
        }

        final IOException queueSaturated =
                new IOException(
                        "ExternalRuntimePreOperator async send queue saturated for endpoint "
                                + endpointIndex
                                + "; triggering failover.");
        if (tcpConfig.isAutoFailoverEnabled()) {
            handleEndpointFailure(endpointIndex, queueSaturated, payload, rowsInBatch);
            return;
        }
        throw queueSaturated;
    }

    private void runSendWorker(
            int endpointIndex, ArrayBlockingQueue<PendingSendBatch> endpointSendQueue) {
        while (sendWorkerRunning || (endpointSendQueue != null && !endpointSendQueue.isEmpty())) {
            final PendingSendBatch batch;
            try {
                batch = endpointSendQueue.poll(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                if (!sendWorkerRunning) {
                    break;
                }
                continue;
            }
            if (batch == null) {
                continue;
            }
            try {
                final EndpointState endpointState = endpointStates.get(endpointIndex);
                final BufferedOutputStream out = endpointState.out;
                if (out == null) {
                    throw new IOException(
                            "Runtime endpoint output stream not available: "
                                    + endpointState.endpoint.getHost()
                                    + ':'
                                    + endpointState.endpoint.getSendPort());
                }
                out.write(batch.payload);
                // Keep delivery semantics identical to fixed-parallel mode so POST does not
                // wait on rows that are still buffered in PRE after scale-up.
                out.flush();
            } catch (IOException ioe) {
                if (asyncSendFailures != null) {
                    while (!asyncSendFailures.offer(new SendFailure(batch, ioe))) {
                        asyncSendFailures.poll();
                    }
                }
            } finally {
                pendingSendBatches.decrementAndGet();
            }
        }
    }

    private void maybeAdjustAutoParallelism() throws IOException {
        final long now = System.nanoTime();
        final long elapsed = now - autoWindowStartNanos;
        if (elapsed < AUTO_SCALE_EVAL_INTERVAL_NANOS) {
            return;
        }

        final long windowRows = autoWindowRows;
        autoWindowStartNanos = now;
        autoWindowRows = 0L;

        final int activeCount = activeEndpointIndices.size();
        final int maxEndpoints = endpointStates.size();

        if (windowRows == 0) {
            idleWindowStreak++;
            if (idleWindowStreak >= AUTO_SCALE_IDLE_WINDOW_THRESHOLD && activeCount > 1) {
                scaleDownOneEndpoint();
                nextScaleUpAtNanos = now + AUTO_SCALE_DOWN_COOLDOWN_NANOS;
                idleWindowStreak = 0L;
            }
            return;
        }
        idleWindowStreak = 0L;

        if (activeCount < maxEndpoints && now >= nextScaleUpAtNanos) {
            if (tryScaleUpOneEndpoint()) {
                nextScaleUpAtNanos = now + AUTO_SCALE_UP_COOLDOWN_NANOS;
            }
        }
    }

    private boolean tryScaleUpOneEndpoint() throws IOException {
        for (int i = 0; i < endpointStates.size(); i++) {
            if (activeEndpointIndices.contains(i)) {
                continue;
            }
            if (connectEndpoint(i)) {
                // Flush existing active streams before changing routing width to avoid
                // leaving older rowIds stranded in partial batches during scale-up.
                flushActiveBatchesBeforeScaleUp();
                activeEndpointIndices.add(i);
                updatePrimarySocket();
                LOG.info(
                        "ExternalRuntimePreOperator auto-parallelism scaled UP to {} endpoint(s), "
                                + "added endpoint {} ({}:{}), active={}.",
                        activeEndpointIndices.size(),
                        i,
                        endpointStates.get(i).endpoint.getHost(),
                        endpointStates.get(i).endpoint.getSendPort(),
                        activeEndpointIndices);
                return true;
            }
        }
        return false;
    }

    private void flushActiveBatchesBeforeScaleUp() throws IOException {
        if (activeEndpointIndices == null || activeEndpointIndices.isEmpty()) {
            return;
        }

        final List<Integer> snapshot = new ArrayList<>(activeEndpointIndices);
        for (int endpointIndex : snapshot) {
            if (endpointIndex < 0 || endpointIndex >= endpointStates.size()) {
                continue;
            }
            if (!activeEndpointIndices.contains(endpointIndex)) {
                continue;
            }
            final EndpointState state = endpointStates.get(endpointIndex);
            if (state.batchCount > 0 || state.batchBuffer.size() > 0) {
                flushBatch(endpointIndex, true);
            }
        }
    }

    private boolean scaleDownOneEndpoint() {
        if (activeEndpointIndices.size() <= 1) {
            return false;
        }
        final int removePos = activeEndpointIndices.size() - 1;
        final int endpointIndex = activeEndpointIndices.get(removePos);
        final EndpointState endpointState = endpointStates.get(endpointIndex);
        if (endpointState.batchCount > 0 || endpointState.batchBuffer.size() > 0) {
            return false;
        }
        activeEndpointIndices.remove(removePos);
        closeEndpoint(endpointState, false);
        updatePrimarySocket();
        LOG.info(
                "ExternalRuntimePreOperator auto-parallelism scaled DOWN to {} endpoint(s), "
                        + "removed endpoint {} ({}:{}), active={}.",
                activeEndpointIndices.size(),
                endpointIndex,
                endpointState.endpoint.getHost(),
                endpointState.endpoint.getSendPort(),
                activeEndpointIndices);
        return true;
    }

    private void handleEndpointFailure(int failedEndpointIndex, IOException cause) throws IOException {
        handleEndpointFailure(failedEndpointIndex, cause, null, 0);
    }

    private void handleEndpointFailure(
            int failedEndpointIndex, IOException cause, byte[] extraPendingBytes, int extraPendingRows)
            throws IOException {
        if (!tcpConfig.isAutoFailoverEnabled()) {
            throw cause;
        }
        // Pull already queued async batches for the failed endpoint back to operator thread.
        final List<PendingSendBatch> queuedBatches = drainQueuedBatchesForEndpoint(failedEndpointIndex);
        final EndpointState failedState = endpointStates.get(failedEndpointIndex);
        final byte[] bufferedBytes = failedState.batchBuffer.toByteArray();
        int pendingRows = failedState.batchCount + Math.max(0, extraPendingRows);
        int totalPendingBytesLen =
                bufferedBytes.length + (extraPendingBytes == null ? 0 : extraPendingBytes.length);
        for (int i = 0; i < queuedBatches.size(); i++) {
            final PendingSendBatch queued = queuedBatches.get(i);
            pendingRows += queued.rowsInBatch;
            totalPendingBytesLen += queued.payload.length;
        }
        final byte[] pendingBytes = new byte[totalPendingBytesLen];
        int pendingPos = 0;
        if (extraPendingBytes != null && extraPendingBytes.length > 0) {
            System.arraycopy(extraPendingBytes, 0, pendingBytes, pendingPos, extraPendingBytes.length);
            pendingPos += extraPendingBytes.length;
        }
        if (bufferedBytes.length > 0) {
            System.arraycopy(bufferedBytes, 0, pendingBytes, pendingPos, bufferedBytes.length);
            pendingPos += bufferedBytes.length;
        }
        for (int i = 0; i < queuedBatches.size(); i++) {
            final byte[] queuedPayload = queuedBatches.get(i).payload;
            System.arraycopy(queuedPayload, 0, pendingBytes, pendingPos, queuedPayload.length);
            pendingPos += queuedPayload.length;
        }
        failedState.batchBuffer.reset();
        failedState.batchCount = 0;
        failedState.firstBufferedAtNanos = 0L;

        removeActiveEndpoint(failedEndpointIndex);
        closeEndpoint(failedState, true);
        updatePrimarySocket();

        final int failoverIndex = selectFailoverEndpoint();
        if (failoverIndex < 0) {
            throw new IOException(
                    "ExternalRuntimePreOperator failed over endpoint unavailable after failure: "
                            + failedState.endpoint.getHost()
                            + ':'
                            + failedState.endpoint.getSendPort(),
                    cause);
        }

        if (pendingBytes.length > 0) {
            // Replay failed in-flight bytes immediately to avoid leaving old rowIds in a
            // partial buffer, which can block POST waiting for missing responses.
            enqueueSendBatch(failoverIndex, pendingBytes, Math.max(1, pendingRows), true);
        }
    }

    private List<PendingSendBatch> drainQueuedBatchesForEndpoint(int endpointIndex) {
        final List<PendingSendBatch> drained = new ArrayList<>();
        if (endpointSendQueues == null
                || endpointIndex < 0
                || endpointIndex >= endpointSendQueues.size()) {
            return drained;
        }
        final ArrayBlockingQueue<PendingSendBatch> sendQueue = endpointSendQueues.get(endpointIndex);
        if (sendQueue == null) {
            return drained;
        }
        PendingSendBatch batch;
        while ((batch = sendQueue.poll()) != null) {
            pendingSendBatches.decrementAndGet();
            drained.add(batch);
        }
        return drained;
    }

    private void drainAsyncSendFailures() throws IOException {
        if (asyncSendFailures == null) {
            return;
        }
        SendFailure failure;
        while ((failure = asyncSendFailures.poll()) != null) {
            handleEndpointFailure(
                    failure.batch.endpointIndex,
                    new IOException("Asynchronous send failed for runtime endpoint.", failure.cause),
                    failure.batch.payload,
                    failure.batch.rowsInBatch);
        }
    }

    private int selectFailoverEndpoint() throws IOException {
        ensureActiveEndpoints();
        if (activeEndpointIndices.isEmpty()) {
            return -1;
        }
        final int activeSize = activeEndpointIndices.size();
        final int start = Math.floorMod(nextFailoverEndpointCursor, activeSize);
        int bestEndpointIndex = activeEndpointIndices.get(start);
        int bestLoad = estimateEndpointLoad(bestEndpointIndex);
        for (int offset = 1; offset < activeSize; offset++) {
            final int candidateEndpointIndex =
                    activeEndpointIndices.get((start + offset) % activeSize);
            final int candidateLoad = estimateEndpointLoad(candidateEndpointIndex);
            if (candidateLoad < bestLoad) {
                bestEndpointIndex = candidateEndpointIndex;
                bestLoad = candidateLoad;
                if (bestLoad <= 0) {
                    break;
                }
            }
        }
        nextFailoverEndpointCursor = (start + 1) % activeSize;
        return bestEndpointIndex;
    }

    private int estimateEndpointLoad(int endpointIndex) {
        int load = 0;
        if (endpointIndex >= 0 && endpointIndex < endpointStates.size()) {
            load += Math.max(0, endpointStates.get(endpointIndex).batchCount);
        }
        if (endpointSendQueues != null
                && endpointIndex >= 0
                && endpointIndex < endpointSendQueues.size()
                && endpointSendQueues.get(endpointIndex) != null) {
            load += Math.max(0, endpointSendQueues.get(endpointIndex).size());
        }
        return load;
    }

    private void ensureActiveEndpoints() throws IOException {
        if (!activeEndpointIndices.isEmpty()) {
            return;
        }
        if (activateEndpoints(1) == 0) {
            throw new IOException("ExternalRuntimePreOperator has no active runtime endpoints.");
        }
    }

    private int activateEndpoints(int targetActive) throws IOException {
        int activated = 0;
        for (int i = 0; i < endpointStates.size() && activeEndpointIndices.size() < targetActive; i++) {
            if (activeEndpointIndices.contains(i)) {
                continue;
            }
            if (connectEndpoint(i)) {
                activeEndpointIndices.add(i);
                activated++;
            }
        }
        updatePrimarySocket();
        return activated;
    }

    private boolean connectEndpoint(int endpointIndex) throws IOException {
        final EndpointState endpointState = endpointStates.get(endpointIndex);
        if (endpointState.out != null && endpointState.socket != null) {
            return true;
        }
        final long now = System.nanoTime();
        if (endpointState.nextReconnectAtNanos > now) {
            return false;
        }

        Socket candidateSocket = null;
        BufferedOutputStream candidateOut = null;
        try {
            candidateSocket =
                    connectSocket(
                            endpointState.endpoint.getHost(),
                            endpointState.endpoint.getSendPort(),
                            tcpConfig.getConnectTimeoutMs());
            candidateSocket.setTcpNoDelay(true);
            candidateOut =
                    new BufferedOutputStream(candidateSocket.getOutputStream(), tcpConfig.getBufferSize());
            writeLengthPrefixedJson(candidateOut, configJson);
            candidateOut.flush();

            endpointState.socket = candidateSocket;
            endpointState.out = candidateOut;
            endpointState.nextReconnectAtNanos = 0L;
            return true;
        } catch (IOException e) {
            closeQuietly(candidateOut);
            closeQuietly(candidateSocket);
            endpointState.nextReconnectAtNanos =
                    now + (long) tcpConfig.getFailoverReconnectBackoffMs() * 1_000_000L;
            if (!tcpConfig.isAutoFailoverEnabled() && !tcpConfig.isAutoParallelismEnabled()) {
                throw e;
            }
            LOG.warn(
                    "ExternalRuntimePreOperator could not connect to endpoint {}:{}; will retry.",
                    endpointState.endpoint.getHost(),
                    endpointState.endpoint.getSendPort(),
                    e);
            return false;
        }
    }

    private void removeActiveEndpoint(int endpointIndex) {
        for (int i = activeEndpointIndices.size() - 1; i >= 0; i--) {
            if (activeEndpointIndices.get(i) == endpointIndex) {
                activeEndpointIndices.remove(i);
            }
        }
    }

    private void closeEndpoint(EndpointState endpointState, boolean failed) {
        closeQuietly(endpointState.out);
        closeQuietly(endpointState.socket);
        endpointState.out = null;
        endpointState.socket = null;
        endpointState.firstBufferedAtNanos = 0L;
        endpointState.nextReconnectAtNanos =
                failed ? System.nanoTime() + (long) tcpConfig.getFailoverReconnectBackoffMs() * 1_000_000L : 0L;
    }

    private void updatePrimarySocket() {
        this.socket = null;
        if (activeEndpointIndices == null || endpointStates == null) {
            return;
        }
        for (int endpointIndex : activeEndpointIndices) {
            final EndpointState endpointState = endpointStates.get(endpointIndex);
            if (endpointState.socket != null) {
                this.socket = endpointState.socket;
                return;
            }
        }
    }

    private IOException tryFlushRemaining() {
        IOException error = null;
        if (endpointStates == null) {
            return null;
        }
        for (int i = 0; i < endpointStates.size(); i++) {
            try {
                flushBatch(i, true);
            } catch (IOException e) {
                error = suppress(error, e);
            }
        }
        return error;
    }

    private void waitForPendingSends() throws IOException {
        while (pendingSendBatches != null && pendingSendBatches.get() > 0) {
            drainAsyncSendFailures();
            try {
                TimeUnit.NANOSECONDS.sleep(ASYNC_DRAIN_WAIT_SLEEP_NANOS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for async sends to drain.", e);
            }
        }
    }

    private void stopSendWorker() {
        sendWorkerRunning = false;
        if (sendWorkerThreads != null) {
            for (int i = 0; i < sendWorkerThreads.size(); i++) {
                final Thread worker = sendWorkerThreads.get(i);
                if (worker != null) {
                    worker.interrupt();
                }
            }
            for (int i = 0; i < sendWorkerThreads.size(); i++) {
                final Thread worker = sendWorkerThreads.get(i);
                if (worker == null) {
                    continue;
                }
                try {
                    worker.join(1000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            sendWorkerThreads.clear();
        }
        sendWorkerThreads = null;
    }

    @Override
    protected void closeInternal() throws Exception {
        IOException error = null;

        // Best-effort flush during close; ignore errors since remote may have disconnected
        try {
            tryFlushRemaining();
            waitForPendingSends();
            drainAsyncSendFailures();
        } catch (Exception e) {
            LOG.debug("ExternalRuntimePreOperator best-effort flush failed during close.", e);
        }
        stopSendWorker();

        // Close sockets first to sever connections, preventing BufferedOutputStream.close()
        // from attempting a flush on a broken pipe during shutdown.
        if (endpointStates != null) {
            for (EndpointState endpointState : endpointStates) {
                error = suppress(error, closeQuietly(endpointState.socket));
                endpointState.socket = null;
            }
        }

        // Now close output streams; flush will fail harmlessly since sockets are already closed.
        if (endpointStates != null) {
            for (EndpointState endpointState : endpointStates) {
                closeQuietly(endpointState.out);
                endpointState.out = null;
            }
        }

        codec = null;

        if (endpointStates != null) {
            for (EndpointState endpointState : endpointStates) {
                endpointState.batchBuffer.reset();
                endpointState.batchCount = 0;
                endpointState.nextReconnectAtNanos = 0L;
                endpointState.firstBufferedAtNanos = 0L;
            }
        }
        endpointStates = null;
        activeEndpointIndices = null;
        configJson = null;
        autoWindowStartNanos = 0L;
        autoWindowRows = 0L;
        autoRowsSinceLastCheck = 0L;
        nextScaleUpAtNanos = 0L;
        idleWindowStreak = 0L;
        nextAutoBatchFlushCheckAtNanos = 0L;
        endpointSendQueues = null;
        asyncSendFailures = null;
        pendingSendBatches = null;
        sendWorkerRunning = false;
        nextFailoverEndpointCursor = 0;
        socket = null;

        if (error != null) {
            throw error;
        }
    }

    private static final class EndpointState {
        private final ExternalRuntimeTcpConfig.ExternalRuntimeEndpoint endpoint;
        private final ByteArrayOutputStream batchBuffer;
        private Socket socket;
        private BufferedOutputStream out;
        private int batchCount;
        private long nextReconnectAtNanos;
        private long firstBufferedAtNanos;

        private EndpointState(
                ExternalRuntimeTcpConfig.ExternalRuntimeEndpoint endpoint, int initialBufferSize) {
            this.endpoint = endpoint;
            this.batchBuffer = new ByteArrayOutputStream(initialBufferSize);
            this.batchCount = 0;
            this.nextReconnectAtNanos = 0L;
            this.firstBufferedAtNanos = 0L;
        }
    }

    private static final class PendingSendBatch {
        private final int endpointIndex;
        private final byte[] payload;
        private final int rowsInBatch;
        private final boolean forceFlush;

        private PendingSendBatch(
                int endpointIndex, byte[] payload, int rowsInBatch, boolean forceFlush) {
            this.endpointIndex = endpointIndex;
            this.payload = payload;
            this.rowsInBatch = rowsInBatch;
            this.forceFlush = forceFlush;
        }
    }

    private static final class SendFailure {
        private final PendingSendBatch batch;
        private final IOException cause;

        private SendFailure(PendingSendBatch batch, IOException cause) {
            this.batch = batch;
            this.cause = cause;
        }
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
}
