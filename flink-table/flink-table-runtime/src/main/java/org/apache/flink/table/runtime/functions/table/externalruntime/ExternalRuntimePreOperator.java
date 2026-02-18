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
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLongArray;

/** PRE: sends input rows to the external runtime and emits placeholders. */
@Internal
public final class ExternalRuntimePreOperator extends ExternalRuntimeOperator
        implements BoundedOneInput {

    private static final long serialVersionUID = 1L;
    private static final long AUTO_BATCH_LINGER_NANOS = 5_000_000L;
    private static final long AUTO_BATCH_FLUSH_CHECK_INTERVAL_NANOS = 1_000_000L;
    private static final long AUTO_RECONNECT_SWEEP_INTERVAL_NANOS = 250_000_000L;
    private static final int ASYNC_SEND_QUEUE_CAPACITY = 8192;
    private static final long ASYNC_SEND_ENQUEUE_TIMEOUT_MS = 25L;
    private static final long ASYNC_DRAIN_WAIT_SLEEP_NANOS = 1_000_000L;
    private static final long ASYNC_WRITE_TIMEOUT_MS = 1_000L;
    private static final long ASYNC_WRITE_TIMEOUT_NANOS = ASYNC_WRITE_TIMEOUT_MS * 1_000_000L;
    private static final long ASYNC_WRITE_TIMEOUT_SWEEP_NANOS = 50_000_000L;

    private transient List<ExternalRuntimeTcpConfig.ExternalRuntimeEndpoint> endpoints;
    private transient List<EndpointState> endpointStates;
    private transient List<Integer> activeEndpointIndices;
    private transient int batchSize;
    private transient long nextRowId;
    private transient String configJson;
    private transient ByteArrayOutputStream autoBatchBuffer;
    private transient int autoBatchCount;
    private transient long autoBatchFirstBufferedAtNanos;
    private transient long nextAutoBatchFlushCheckAtNanos;
    private transient long nextAutoReconnectSweepAtNanos;
    private transient List<ArrayBlockingQueue<PendingSendBatch>> endpointSendQueues;
    private transient AtomicIntegerArray endpointQueuedBatchCounts;
    private transient ArrayBlockingQueue<PendingSendBatch> sharedAutoSendQueue;
    private transient ArrayBlockingQueue<SendFailure> asyncSendFailures;
    private transient AtomicInteger pendingSendBatches;
    private transient List<Thread> sendWorkerThreads;
    private transient volatile boolean sendWorkerRunning;
    private transient AtomicLongArray endpointWriteStartedAtNanos;
    private transient Thread writeTimeoutWatcherThread;
    private transient volatile boolean writeTimeoutWatcherRunning;
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
        final int configuredBatchSize = Math.max(1, tcpConfig.getBatchSize());
        this.configJson = buildConfigJson();
        final boolean autoParallelismEnabled = tcpConfig.isAutoParallelismEnabled();
        this.batchSize = configuredBatchSize;
        this.endpointSendQueues = autoParallelismEnabled ? null : new ArrayList<>(endpoints.size());
        this.endpointQueuedBatchCounts = autoParallelismEnabled ? null : new AtomicIntegerArray(endpoints.size());
        this.sharedAutoSendQueue =
                autoParallelismEnabled
                        ? new ArrayBlockingQueue<>(ASYNC_SEND_QUEUE_CAPACITY * Math.max(1, endpoints.size()))
                        : null;
        this.asyncSendFailures = new ArrayBlockingQueue<>(ASYNC_SEND_QUEUE_CAPACITY);
        this.pendingSendBatches = new AtomicInteger(0);
        this.sendWorkerThreads = new ArrayList<>(endpoints.size());
        this.sendWorkerRunning = true;
        this.endpointWriteStartedAtNanos = new AtomicLongArray(endpoints.size());
        this.writeTimeoutWatcherThread = null;
        this.writeTimeoutWatcherRunning = false;
        this.nextFailoverEndpointCursor = 0;
        this.autoBatchBuffer = autoParallelismEnabled ? new ByteArrayOutputStream(tcpConfig.getBufferSize()) : null;
        this.autoBatchCount = 0;
        this.autoBatchFirstBufferedAtNanos = 0L;

        for (ExternalRuntimeTcpConfig.ExternalRuntimeEndpoint endpoint : endpoints) {
            endpointStates.add(new EndpointState(endpoint, tcpConfig.getBufferSize()));
            if (endpointSendQueues != null) {
                endpointSendQueues.add(new ArrayBlockingQueue<>(ASYNC_SEND_QUEUE_CAPACITY));
            }
        }

        final int configuredParallelism = tcpConfig.getRuntimeParallelism();
        final int fixedActiveTarget =
                configuredParallelism > 0
                        ? Math.min(configuredParallelism, endpointStates.size())
                        : endpointStates.size();
        final int initialActiveTarget =
                tcpConfig.isAutoParallelismEnabled() ? endpointStates.size() : fixedActiveTarget;
        activateEndpoints(initialActiveTarget);
        if (activeEndpointIndices.isEmpty()) {
            throw new IOException("ExternalRuntimePreOperator could not connect to any runtime endpoint.");
        }
        updatePrimarySocket();

        this.nextRowId = 0L;
        final long nowNanos = System.nanoTime();
        this.nextAutoBatchFlushCheckAtNanos = nowNanos + AUTO_BATCH_FLUSH_CHECK_INTERVAL_NANOS;
        this.nextAutoReconnectSweepAtNanos = nowNanos + AUTO_RECONNECT_SWEEP_INTERVAL_NANOS;

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
            final Thread worker =
                    new Thread(
                            autoParallelismEnabled
                                    ? () -> runAutoSendWorker(endpointIndex)
                                    : () -> runSendWorker(endpointIndex, endpointSendQueues.get(endpointIndex)),
                            "external-runtime-pre-send-" + endpointIndex);
            worker.setDaemon(true);
            sendWorkerThreads.add(worker);
            worker.start();
        }
        if (autoParallelismEnabled) {
            startWriteTimeoutWatcher();
        }

        LOG.info(
                "ExternalRuntimePreOperator connected to {} runtime(s) (rowType={}, sentConfigBytes={}, batchSize={})",
                activeEndpointIndices.size(),
                inputRowType,
                configJson.getBytes(StandardCharsets.UTF_8).length,
                batchSize);
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
        final long nowNanos = System.nanoTime();
        if (tcpConfig.isAutoParallelismEnabled() && nowNanos >= nextAutoReconnectSweepAtNanos) {
            maybeReconnectAutoEndpoints();
            nextAutoReconnectSweepAtNanos = nowNanos + AUTO_RECONNECT_SWEEP_INTERVAL_NANOS;
        }
        if (tcpConfig.isAutoParallelismEnabled()) {
            appendRowToAutoBatch(row, nowNanos);
            return;
        }
        final int endpointIndex = selectActiveEndpointIndex(nextRowId);
        final EndpointState endpointState = endpointStates.get(endpointIndex);
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
    }

    private void appendRowToAutoBatch(RowData row, long nowNanos) throws IOException {
        if (autoBatchBuffer == null) {
            throw new IOException("ExternalRuntimePreOperator auto batch buffer not initialized");
        }
        if (autoBatchCount == 0) {
            autoBatchFirstBufferedAtNanos = nowNanos;
        }
        codec.writeFramedRow(autoBatchBuffer, row, payloadFieldIndicesArray, nextRowId);
        autoBatchCount++;
        if (autoBatchCount >= batchSize) {
            flushAutoBatch(false);
        } else if (nowNanos >= nextAutoBatchFlushCheckAtNanos) {
            if (autoBatchFirstBufferedAtNanos > 0L
                    && nowNanos - autoBatchFirstBufferedAtNanos >= AUTO_BATCH_LINGER_NANOS) {
                flushAutoBatch(true);
            }
            nextAutoBatchFlushCheckAtNanos = nowNanos + AUTO_BATCH_FLUSH_CHECK_INTERVAL_NANOS;
        }
        nextRowId++;
    }

    private int selectActiveEndpointIndex(long rowId) throws IOException {
        ensureActiveEndpoints();
        final int activeCount = activeEndpointIndices.size();
        final int routeIndex = tcpConfig.selectEndpointIndex(rowId, activeCount);
        return activeEndpointIndices.get(routeIndex);
    }

    private void maybeReconnectAutoEndpoints() throws IOException {
        if (!tcpConfig.isAutoParallelismEnabled() || endpointStates == null) {
            return;
        }
        boolean added = false;
        for (int i = 0; i < endpointStates.size(); i++) {
            if (activeEndpointIndices.contains(i)) {
                continue;
            }
            if (connectEndpoint(i)) {
                activeEndpointIndices.add(i);
                added = true;
            }
        }
        if (added) {
            activeEndpointIndices.sort(Integer::compareTo);
            updatePrimarySocket();
        }
    }

    private void flushBatch(int endpointIndex) throws IOException {
        flushBatch(endpointIndex, false);
    }

    private void flushAutoBatch(boolean forceFlush) throws IOException {
        if (autoBatchBuffer == null || autoBatchBuffer.size() == 0 || autoBatchCount <= 0) {
            autoBatchCount = 0;
            autoBatchFirstBufferedAtNanos = 0L;
            return;
        }
        final int rowsInBatch = autoBatchCount;
        final byte[] payload = autoBatchBuffer.toByteArray();
        autoBatchBuffer.reset();
        autoBatchCount = 0;
        autoBatchFirstBufferedAtNanos = 0L;
        enqueueAutoSendBatch(payload, rowsInBatch, forceFlush);
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
                if (endpointQueuedBatchCounts != null
                        && endpointIndex >= 0
                        && endpointIndex < endpointQueuedBatchCounts.length()) {
                    endpointQueuedBatchCounts.incrementAndGet(endpointIndex);
                }
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
        if (tcpConfig.isAutoParallelismEnabled()) {
            handleEndpointFailure(endpointIndex, queueSaturated, payload, rowsInBatch);
            return;
        }
        throw queueSaturated;
    }

    private void enqueueAutoSendBatch(byte[] payload, int rowsInBatch, boolean forceFlush)
            throws IOException {
        if (!sendWorkerRunning || sharedAutoSendQueue == null) {
            throw new IOException("ExternalRuntimePreOperator auto async sender is not running.");
        }
        drainAsyncSendFailures();
        final PendingSendBatch batch = new PendingSendBatch(-1, payload, rowsInBatch, forceFlush);
        try {
            while (sendWorkerRunning) {
                if (sharedAutoSendQueue.offer(batch, ASYNC_SEND_ENQUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                    pendingSendBatches.incrementAndGet();
                    return;
                }
                drainAsyncSendFailures();
            }
            throw new IOException("ExternalRuntimePreOperator auto sender stopped while enqueueing.");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while enqueueing auto async send batch.", e);
        }
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
                markEndpointWriteStarted(endpointIndex);
                try {
                    out.write(batch.payload);
                    // Keep delivery semantics identical to fixed-parallel mode so POST does not
                    // wait on rows that are still buffered in PRE after scale-up.
                    out.flush();
                } finally {
                    clearEndpointWriteStarted(endpointIndex);
                }
            } catch (IOException ioe) {
                if (asyncSendFailures != null) {
                    while (!asyncSendFailures.offer(new SendFailure(batch, endpointIndex, ioe))) {
                        asyncSendFailures.poll();
                    }
                }
            } finally {
                pendingSendBatches.decrementAndGet();
                if (endpointQueuedBatchCounts != null
                        && endpointIndex >= 0
                        && endpointIndex < endpointQueuedBatchCounts.length()) {
                    endpointQueuedBatchCounts.decrementAndGet(endpointIndex);
                }
            }
        }
    }

    private void runAutoSendWorker(int endpointIndex) {
        while (sendWorkerRunning || (sharedAutoSendQueue != null && !sharedAutoSendQueue.isEmpty())) {
            final EndpointState endpointState = endpointStates.get(endpointIndex);
            final BufferedOutputStream out = endpointState.out;
            if (out == null) {
                tryReconnectEndpointForAutoWorker(endpointIndex);
                try {
                    TimeUnit.MILLISECONDS.sleep(2L);
                } catch (InterruptedException e) {
                    if (!sendWorkerRunning) {
                        break;
                    }
                }
                continue;
            }
            final PendingSendBatch batch;
            try {
                batch =
                        sharedAutoSendQueue == null
                                ? null
                                : sharedAutoSendQueue.poll(50L, TimeUnit.MILLISECONDS);
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
                markEndpointWriteStarted(endpointIndex);
                try {
                    out.write(batch.payload);
                    out.flush();
                } finally {
                    clearEndpointWriteStarted(endpointIndex);
                }
            } catch (IOException ioe) {
                if (asyncSendFailures != null) {
                    while (!asyncSendFailures.offer(new SendFailure(batch, endpointIndex, ioe))) {
                        asyncSendFailures.poll();
                    }
                }
            } finally {
                pendingSendBatches.decrementAndGet();
            }
        }
    }

    private void tryReconnectEndpointForAutoWorker(int endpointIndex) {
        try {
            connectEndpoint(endpointIndex);
        } catch (IOException ioe) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "ExternalRuntimePreOperator auto worker failed reconnect for endpoint {}:{}.",
                        endpointStates.get(endpointIndex).endpoint.getHost(),
                        endpointStates.get(endpointIndex).endpoint.getSendPort(),
                        ioe);
            }
        }
    }

    private void handleEndpointFailure(int failedEndpointIndex, IOException cause) throws IOException {
        handleEndpointFailure(failedEndpointIndex, cause, null, 0);
    }

    private void handleEndpointFailure(
            int failedEndpointIndex, IOException cause, byte[] extraPendingBytes, int extraPendingRows)
            throws IOException {
        if (!tcpConfig.isAutoParallelismEnabled()) {
            throw cause;
        }
        if (tcpConfig.isAutoParallelismEnabled()) {
            final EndpointState failedState = endpointStates.get(failedEndpointIndex);
            removeActiveEndpoint(failedEndpointIndex);
            closeEndpoint(failedState, true);
            updatePrimarySocket();
            if (extraPendingBytes != null && extraPendingBytes.length > 0) {
                enqueueAutoSendBatch(extraPendingBytes, Math.max(1, extraPendingRows), true);
            }
            return;
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
            if (endpointQueuedBatchCounts != null
                    && endpointIndex >= 0
                    && endpointIndex < endpointQueuedBatchCounts.length()) {
                endpointQueuedBatchCounts.decrementAndGet(endpointIndex);
            }
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
                    failure.failedEndpointIndex,
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
        if (endpointQueuedBatchCounts != null
                && endpointIndex >= 0
                && endpointIndex < endpointQueuedBatchCounts.length()) {
            load += Math.max(0, endpointQueuedBatchCounts.get(endpointIndex));
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
            if (!tcpConfig.isAutoParallelismEnabled()) {
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
        if (tcpConfig.isAutoParallelismEnabled()) {
            try {
                flushAutoBatch(true);
            } catch (IOException e) {
                error = suppress(error, e);
            }
            return error;
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

    private void startWriteTimeoutWatcher() {
        writeTimeoutWatcherRunning = true;
        writeTimeoutWatcherThread =
                new Thread(this::runWriteTimeoutWatcher, "external-runtime-pre-send-timeout");
        writeTimeoutWatcherThread.setDaemon(true);
        writeTimeoutWatcherThread.start();
    }

    private void stopWriteTimeoutWatcher() {
        writeTimeoutWatcherRunning = false;
        if (writeTimeoutWatcherThread != null) {
            writeTimeoutWatcherThread.interrupt();
            try {
                writeTimeoutWatcherThread.join(1000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        writeTimeoutWatcherThread = null;
    }

    private void runWriteTimeoutWatcher() {
        while (writeTimeoutWatcherRunning) {
            try {
                if (!sendWorkerRunning
                        || endpointStates == null
                        || endpointWriteStartedAtNanos == null) {
                    TimeUnit.MILLISECONDS.sleep(10L);
                    continue;
                }
                final long nowNanos = System.nanoTime();
                final int limit = Math.min(endpointStates.size(), endpointWriteStartedAtNanos.length());
                for (int endpointIndex = 0; endpointIndex < limit; endpointIndex++) {
                    final long writeStartedAtNanos = endpointWriteStartedAtNanos.get(endpointIndex);
                    if (writeStartedAtNanos <= 0L) {
                        continue;
                    }
                    if (nowNanos - writeStartedAtNanos < ASYNC_WRITE_TIMEOUT_NANOS) {
                        continue;
                    }
                    if (!endpointWriteStartedAtNanos.compareAndSet(
                            endpointIndex, writeStartedAtNanos, 0L)) {
                        continue;
                    }
                    final EndpointState endpointState = endpointStates.get(endpointIndex);
                    if (endpointState.socket == null && endpointState.out == null) {
                        continue;
                    }
                    LOG.warn(
                            "ExternalRuntimePreOperator send to endpoint {}:{} exceeded {} ms; closing socket to trigger failover/reconnect.",
                            endpointState.endpoint.getHost(),
                            endpointState.endpoint.getSendPort(),
                            ASYNC_WRITE_TIMEOUT_MS);
                    removeActiveEndpoint(endpointIndex);
                    closeEndpoint(endpointState, true);
                    updatePrimarySocket();
                }
                TimeUnit.NANOSECONDS.sleep(ASYNC_WRITE_TIMEOUT_SWEEP_NANOS);
            } catch (InterruptedException e) {
                if (!writeTimeoutWatcherRunning) {
                    return;
                }
            } catch (Throwable t) {
                LOG.warn("ExternalRuntimePreOperator write-timeout watcher failed; continuing.", t);
            }
        }
    }

    private void markEndpointWriteStarted(int endpointIndex) {
        if (endpointWriteStartedAtNanos == null
                || endpointIndex < 0
                || endpointIndex >= endpointWriteStartedAtNanos.length()) {
            return;
        }
        endpointWriteStartedAtNanos.set(endpointIndex, System.nanoTime());
    }

    private void clearEndpointWriteStarted(int endpointIndex) {
        if (endpointWriteStartedAtNanos == null
                || endpointIndex < 0
                || endpointIndex >= endpointWriteStartedAtNanos.length()) {
            return;
        }
        endpointWriteStartedAtNanos.set(endpointIndex, 0L);
    }

    @Override
    protected void closeInternal() throws Exception {
        IOException error = null;

        stopWriteTimeoutWatcher();

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
        autoBatchBuffer = null;
        autoBatchCount = 0;
        autoBatchFirstBufferedAtNanos = 0L;
        nextAutoBatchFlushCheckAtNanos = 0L;
        nextAutoReconnectSweepAtNanos = 0L;
        endpointSendQueues = null;
        endpointQueuedBatchCounts = null;
        sharedAutoSendQueue = null;
        asyncSendFailures = null;
        pendingSendBatches = null;
        sendWorkerRunning = false;
        endpointWriteStartedAtNanos = null;
        writeTimeoutWatcherRunning = false;
        writeTimeoutWatcherThread = null;
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
        private final int failedEndpointIndex;
        private final IOException cause;

        private SendFailure(PendingSendBatch batch, int failedEndpointIndex, IOException cause) {
            this.batch = batch;
            this.failedEndpointIndex = failedEndpointIndex;
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
