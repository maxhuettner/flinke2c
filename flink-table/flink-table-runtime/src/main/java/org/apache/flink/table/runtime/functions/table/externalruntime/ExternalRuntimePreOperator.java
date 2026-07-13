package org.apache.flink.table.runtime.functions.table.externalruntime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
    private static final long ACK_RETRY_INTERVAL_NANOS = 100_000_000L;
    private static final long ACK_RETRY_SWEEP_SLEEP_NANOS = 10_000_000L;
    private static final int ACK_DRAIN_EVERY_ROWS = 64;
    private static final long PENDING_ACK_NO_PROGRESS_TIMEOUT_MS = 30_000L;
    private static final int ACK_FRAME_LEN = 12;
    private static final int ACK_OP = -1;

    private transient List<ExternalRuntimeTcpConfig.ExternalRuntimeEndpoint> endpoints;
    private transient List<EndpointState> endpointStates;
    private transient List<Integer> activeEndpointIndices;
    private transient int batchSize;
    private transient long nextRowId;
    private transient String configJson;
    private transient ByteArrayOutputStream autoBatchBuffer;
    private transient int autoBatchCount;
    private transient long autoBatchFirstRowId;
    private transient long autoBatchFirstBufferedAtNanos;
    private transient long nextAutoBatchFlushCheckAtNanos;
    private transient long nextAutoReconnectSweepAtNanos;
    private transient List<ArrayBlockingQueue<PendingSendBatch>> endpointSendQueues;
    private transient AtomicIntegerArray endpointQueuedBatchCounts;
    private transient ArrayBlockingQueue<PendingSendBatch> sharedAutoSendQueue;
    private transient ArrayBlockingQueue<PendingSendBatch> sharedAutoReplayQueue;
    private transient ArrayBlockingQueue<SendFailure> asyncSendFailures;
    private transient AtomicInteger pendingSendBatches;
    private transient List<Thread> sendWorkerThreads;
    private transient volatile boolean sendWorkerRunning;
    private transient AtomicLongArray endpointWriteStartedAtNanos;
    private transient Thread writeTimeoutWatcherThread;
    private transient volatile boolean writeTimeoutWatcherRunning;
    private transient int nextFailoverEndpointCursor;
    private transient ConcurrentMap<Long, PendingBatch> pendingBatches;
    private transient ArrayBlockingQueue<Long> ackedRowWatermarks;
    private transient List<Thread> ackReaderThreads;
    private transient volatile boolean ackReadersRunning;
    private transient Thread resendPendingBatchesThread;
    private transient volatile boolean resendPendingBatchesRunning;
    private transient int ackDrainCountdown;
    private transient long highestAckedRowId;
    private transient ListState<PendingBatchState> pendingBatchesState;
    private transient ListState<Long> nextRowIdState;

    public ExternalRuntimePreOperator(String conf, RowType rowType) {
        super(conf, rowType, null);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        pendingBatchesState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "external-runtime-pre-pending-batches",
                                        TypeInformation.of(new TypeHint<PendingBatchState>() {})));
        nextRowIdState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "external-runtime-pre-next-row-id",
                                        TypeInformation.of(Long.class)));

        this.pendingBatches = new ConcurrentHashMap<>();
        this.nextRowId = 0L;

        if (context.isRestored()) {
            for (Long restoredNextRowId : nextRowIdState.get()) {
                if (restoredNextRowId != null) {
                    this.nextRowId = restoredNextRowId;
                }
            }
            for (PendingBatchState restoredPendingBatch : pendingBatchesState.get()) {
                if (restoredPendingBatch == null || restoredPendingBatch.payload == null) {
                    continue;
                }
                pendingBatches.put(
                        restoredPendingBatch.firstRowId,
                        new PendingBatch(
                                restoredPendingBatch.firstRowId,
                                restoredPendingBatch.firstRowId
                                        + Math.max(1, restoredPendingBatch.rowCount)
                                        - 1L,
                                restoredPendingBatch.rowCount,
                                restoredPendingBatch.payload,
                                0L));
            }
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        drainAckedRowWatermarks();

        final List<PendingBatchState> pendingSnapshot =
                new ArrayList<>(pendingBatches == null ? 0 : pendingBatches.size() + 1);
        if (pendingBatches != null) {
            for (PendingBatch pendingBatch : pendingBatches.values()) {
                pendingSnapshot.add(
                        new PendingBatchState(
                                pendingBatch.firstRowId,
                                pendingBatch.rowCount,
                                pendingBatch.payload));
            }
        }
        if (sharedAutoSendQueue != null && autoBatchCount > 0 && autoBatchBuffer != null) {
            pendingSnapshot.add(
                    new PendingBatchState(
                            autoBatchFirstRowId,
                            autoBatchCount,
                            autoBatchBuffer.toByteArray()));
        }
        pendingBatchesState.update(pendingSnapshot);
        nextRowIdState.update(java.util.Collections.singletonList(nextRowId));
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
        this.sharedAutoReplayQueue =
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
        this.autoBatchFirstRowId = 0L;
        this.autoBatchFirstBufferedAtNanos = 0L;
        this.ackedRowWatermarks =
                autoParallelismEnabled
                        ? new ArrayBlockingQueue<>(ASYNC_SEND_QUEUE_CAPACITY * Math.max(1, endpoints.size()))
                        : null;
        this.ackReaderThreads = autoParallelismEnabled ? new ArrayList<>(endpoints.size()) : null;
        this.ackReadersRunning = autoParallelismEnabled;
        this.resendPendingBatchesThread = null;
        this.resendPendingBatchesRunning = false;
        this.ackDrainCountdown = ACK_DRAIN_EVERY_ROWS;
        this.highestAckedRowId = -1L;
        if (autoParallelismEnabled && pendingBatches == null) {
            this.pendingBatches = new ConcurrentHashMap<>();
        } else if (!autoParallelismEnabled) {
            this.pendingBatches = null;
        }

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
        final int initialActiveTarget = autoParallelismEnabled ? endpointStates.size() : fixedActiveTarget;
        activateEndpoints(initialActiveTarget);
        if (activeEndpointIndices.isEmpty()) {
            throw new IOException("ExternalRuntimePreOperator could not connect to any runtime endpoint.");
        }
        updatePrimarySocket();

        final long nowNanos = System.nanoTime();
        this.nextAutoBatchFlushCheckAtNanos = nowNanos + AUTO_BATCH_FLUSH_CHECK_INTERVAL_NANOS;
        this.nextAutoReconnectSweepAtNanos = nowNanos + AUTO_RECONNECT_SWEEP_INTERVAL_NANOS;

        this.codec = new ExternalRuntimeBinaryCodec(
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

        if (!autoParallelismEnabled) {
            this.nextRowId = 0L;
        }

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
            for (int i = 0; i < endpointStates.size(); i++) {
                final int endpointIndex = i;
                final Thread reader =
                        new Thread(
                                () -> runAckReader(endpointIndex),
                                "external-runtime-pre-ack-" + endpointIndex);
                reader.setDaemon(true);
                ackReaderThreads.add(reader);
                reader.start();
            }
            startWriteTimeoutWatcher();
            startPendingBatchResender();
            resendPendingBatchesAfterRestore();
        }

        LOG.info(
                "ExternalRuntimePreOperator connected to {} runtime(s) (rowType={}, sentConfigBytes={}, batchSize={}, restoredPendingBatches={})",
                activeEndpointIndices.size(),
                inputRowType,
                configJson.getBytes(StandardCharsets.UTF_8).length,
                batchSize,
                pendingBatches == null ? 0 : pendingBatches.size());
    }

    @Override
    protected RowData processRow(RowData inRow) throws Exception {
        if (tcpConfig.isAutoParallelismEnabled()) {
            if (--ackDrainCountdown <= 0) {
                drainAckedRowWatermarks();
                ackDrainCountdown = ACK_DRAIN_EVERY_ROWS;
            }
            appendRowToBinary(inRow);
            return createPlaceholderRow(inRow.getRowKind());
        }
        appendRowToBinary(inRow);
        return createPlaceholderRow(inRow.getRowKind());
    }

    @Override
    public void endInput() throws Exception {
        final IOException error = tryFlushRemaining();
        if (tcpConfig.isAutoParallelismEnabled()) {
            waitForPendingBatchAcknowledgements();
        }
        waitForPendingSends();
        drainAsyncSendFailures();
        if (error != null) {
            throw error;
        }
    }

    private void drainAckedRowWatermarks() {
        if (ackedRowWatermarks == null || pendingBatches == null) {
            return;
        }
        long newHighestAckedRowId = highestAckedRowId;
        Long ackedRowWatermark;
        while ((ackedRowWatermark = ackedRowWatermarks.poll()) != null) {
            if (ackedRowWatermark > newHighestAckedRowId) {
                newHighestAckedRowId = ackedRowWatermark;
            }
        }
        if (newHighestAckedRowId <= highestAckedRowId) {
            return;
        }
        highestAckedRowId = newHighestAckedRowId;
        for (Map.Entry<Long, PendingBatch> entry : pendingBatches.entrySet()) {
            final PendingBatch pendingBatch = entry.getValue();
            if (pendingBatch == null || pendingBatch.lastRowId > highestAckedRowId) {
                continue;
            }
            pendingBatches.remove(entry.getKey(), pendingBatch);
        }
    }

    private void resendPendingBatchesAfterRestore() throws IOException {
        if (pendingBatches == null || pendingBatches.isEmpty()) {
            return;
        }
        final long nowNanos = System.nanoTime();
        for (PendingBatch pendingBatch : pendingBatches.values()) {
            pendingBatch.lastSendNanos = nowNanos;
            enqueueAutoReplayBatch(pendingBatch.payload, pendingBatch.rowCount, true);
        }
    }

    private void startPendingBatchResender() {
        resendPendingBatchesRunning = true;
        resendPendingBatchesThread =
                new Thread(this::runPendingBatchResender, "external-runtime-pre-resend");
        resendPendingBatchesThread.setDaemon(true);
        resendPendingBatchesThread.start();
    }

    private void stopPendingBatchResender() {
        resendPendingBatchesRunning = false;
        if (resendPendingBatchesThread != null) {
            resendPendingBatchesThread.interrupt();
            try {
                resendPendingBatchesThread.join(1000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        resendPendingBatchesThread = null;
    }

    private void runPendingBatchResender() {
        while (resendPendingBatchesRunning) {
            try {
                drainAckedRowWatermarks();
                final long now = System.nanoTime();
                if (pendingBatches != null) {
                    for (PendingBatch pendingBatch : pendingBatches.values()) {
                        if (now - pendingBatch.lastSendNanos < ACK_RETRY_INTERVAL_NANOS) {
                            continue;
                        }
                        pendingBatch.lastSendNanos = now;
                        enqueueAutoReplayBatch(pendingBatch.payload, pendingBatch.rowCount, true);
                    }
                }
                TimeUnit.NANOSECONDS.sleep(ACK_RETRY_SWEEP_SLEEP_NANOS);
            } catch (IOException e) {
                if (sendWorkerRunning) {
                    LOG.warn(
                            "ExternalRuntimePreOperator pending-batch resend sweep hit IO error; continuing.",
                            e);
                }
            } catch (InterruptedException e) {
                if (!resendPendingBatchesRunning) {
                    return;
                }
            } catch (Throwable t) {
                LOG.warn("ExternalRuntimePreOperator pending-batch resend sweep failed; continuing.", t);
            }
        }
    }

    private void runAckReader(int endpointIndex) {
        while (ackReadersRunning) {
            try {
                final EndpointState endpointState = endpointStates.get(endpointIndex);
                final BufferedInputStream in = endpointState.in;
                if (in == null) {
                    tryReconnectEndpointForAutoWorker(endpointIndex);
                    TimeUnit.MILLISECONDS.sleep(2L);
                    continue;
                }
                final long rowId = readAckRowId(in);
                while (ackReadersRunning) {
                    if (ackedRowWatermarks.offer(rowId, 10L, TimeUnit.MILLISECONDS)) {
                        break;
                    }
                }
            } catch (InterruptedException e) {
                if (!ackReadersRunning) {
                    return;
                }
            } catch (IOException e) {
                if (!ackReadersRunning || endpointStates == null || endpointIndex >= endpointStates.size()) {
                    return;
                }
                final EndpointState endpointState = endpointStates.get(endpointIndex);
                removeActiveEndpoint(endpointIndex);
                closeEndpoint(endpointState, true);
                updatePrimarySocket();
            }
        }
    }

    private void waitForPendingBatchAcknowledgements() throws IOException {
        long lastPendingCount = pendingBatches == null ? 0L : pendingBatches.size();
        long lastProgressNanos = System.nanoTime();
        while (hasUnacknowledgedPendingBatches()) {
            drainAckedRowWatermarks();
            drainAsyncSendFailures();
            final long pendingCount = pendingBatches == null ? 0L : pendingBatches.size();
            final long now = System.nanoTime();
            if (pendingCount < lastPendingCount) {
                lastPendingCount = pendingCount;
                lastProgressNanos = now;
            } else if (pendingCount > 0
                    && now - lastProgressNanos >= pendingAckNoProgressTimeoutNanos()) {
                throw new IOException(
                        "Timed out waiting for pending external runtime batch acknowledgements; "
                                + pendingCount
                                + " batch(es) still outstanding.");
            }
            try {
                TimeUnit.NANOSECONDS.sleep(ASYNC_DRAIN_WAIT_SLEEP_NANOS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for pending batch acknowledgements.", e);
            }
        }
    }

    private boolean hasUnacknowledgedPendingBatches() {
        return pendingBatches != null && !pendingBatches.isEmpty();
    }

    private long pendingAckNoProgressTimeoutNanos() {
        final long timeoutMs =
                Math.max(
                        PENDING_ACK_NO_PROGRESS_TIMEOUT_MS,
                        (long) tcpConfig.getFailoverReconnectBackoffMs() * 10L);
        return timeoutMs * 1_000_000L;
    }

    private long readAckRowId(BufferedInputStream in) throws IOException {
        final int frameLen = readIntBE(in);
        if (frameLen != ACK_FRAME_LEN) {
            throw new IOException("ExternalRuntimePreOperator received invalid ACK frame length: " + frameLen);
        }
        final byte[] ackFrame = new byte[ACK_FRAME_LEN];
        readFully(in, ackFrame, 0, ACK_FRAME_LEN);
        final int op = readIntBE(ackFrame, 0);
        if (op != ACK_OP) {
            throw new IOException("ExternalRuntimePreOperator expected ACK frame but received op=" + op);
        }
        return readLongBE(ackFrame, 4);
    }

    private static void readFully(BufferedInputStream in, byte[] target, int offset, int length)
            throws IOException {
        int read = 0;
        while (read < length) {
            final int bytesRead = in.read(target, offset + read, length - read);
            if (bytesRead < 0) {
                throw new IOException("EOF while reading PRE acknowledgement frame");
            }
            read += bytesRead;
        }
    }

    private static int readIntBE(BufferedInputStream in) throws IOException {
        final int b1 = in.read();
        final int b2 = in.read();
        final int b3 = in.read();
        final int b4 = in.read();
        if ((b1 | b2 | b3 | b4) < 0) {
            throw new IOException("EOF while reading PRE acknowledgement length");
        }
        return (b1 << 24) | (b2 << 16) | (b3 << 8) | b4;
    }

    private static int readIntBE(byte[] buf, int pos) {
        return ((buf[pos] & 0xff) << 24)
                | ((buf[pos + 1] & 0xff) << 16)
                | ((buf[pos + 2] & 0xff) << 8)
                | (buf[pos + 3] & 0xff);
    }

    private static long readLongBE(byte[] buf, int pos) {
        return ((long) (buf[pos] & 0xff) << 56)
                | ((long) (buf[pos + 1] & 0xff) << 48)
                | ((long) (buf[pos + 2] & 0xff) << 40)
                | ((long) (buf[pos + 3] & 0xff) << 32)
                | ((long) (buf[pos + 4] & 0xff) << 24)
                | ((long) (buf[pos + 5] & 0xff) << 16)
                | ((long) (buf[pos + 6] & 0xff) << 8)
                | (buf[pos + 7] & 0xff);
    }

    private void appendRowToBinary(RowData row) throws IOException {
        if (endpointStates == null || endpointStates.isEmpty()) {
            throw new IOException("ExternalRuntimePreOperator output stream not initialized");
        }
        drainAsyncSendFailures();
        final long nowNanos = System.nanoTime();
        if (nowNanos >= nextAutoReconnectSweepAtNanos) {
            maybeReconnectAutoEndpoints();
            nextAutoReconnectSweepAtNanos = nowNanos + AUTO_RECONNECT_SWEEP_INTERVAL_NANOS;
        }
        if (sharedAutoSendQueue != null) {
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
        } else if (sharedAutoSendQueue != null
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
            autoBatchFirstRowId = nextRowId;
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
            autoBatchFirstRowId = 0L;
            autoBatchFirstBufferedAtNanos = 0L;
            return;
        }
        final long batchFirstRowId = autoBatchFirstRowId;
        final int rowsInBatch = autoBatchCount;
        final byte[] payload = autoBatchBuffer.toByteArray();
        autoBatchBuffer.reset();
        autoBatchCount = 0;
        autoBatchFirstRowId = 0L;
        autoBatchFirstBufferedAtNanos = 0L;
        if (pendingBatches != null) {
            pendingBatches.put(
                    batchFirstRowId,
                    new PendingBatch(
                            batchFirstRowId,
                            batchFirstRowId + rowsInBatch - 1L,
                            rowsInBatch,
                            payload,
                            System.nanoTime()));
        }
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
        final PendingSendBatch batch = new PendingSendBatch(endpointIndex, payload, rowsInBatch, forceFlush);
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

        final IOException queueSaturated = new IOException(
                "ExternalRuntimePreOperator async send queue saturated for endpoint "
                        + endpointIndex
                        + "; triggering failover.");
        handleEndpointFailure(endpointIndex, queueSaturated, payload, rowsInBatch);
    }

    private void enqueueAutoSendBatch(byte[] payload, int rowsInBatch, boolean forceFlush)
            throws IOException {
        enqueueAutoBatch(sharedAutoSendQueue, payload, rowsInBatch, forceFlush, false);
    }

    private void enqueueAutoReplayBatch(byte[] payload, int rowsInBatch, boolean forceFlush)
            throws IOException {
        enqueueAutoBatch(sharedAutoReplayQueue, payload, rowsInBatch, forceFlush, true);
    }

    private void enqueueAutoBatch(
            ArrayBlockingQueue<PendingSendBatch> queue,
            byte[] payload,
            int rowsInBatch,
            boolean forceFlush,
            boolean replay)
            throws IOException {
        if (!sendWorkerRunning || queue == null) {
            throw new IOException("ExternalRuntimePreOperator auto async sender is not running.");
        }
        drainAsyncSendFailures();
        final PendingSendBatch batch = new PendingSendBatch(-1, payload, rowsInBatch, forceFlush);
        try {
            while (sendWorkerRunning) {
                if (queue.offer(batch, ASYNC_SEND_ENQUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                    pendingSendBatches.incrementAndGet();
                    return;
                }
                drainAsyncSendFailures();
            }
            throw new IOException(
                    replay
                            ? "ExternalRuntimePreOperator auto replay sender stopped while enqueueing."
                            : "ExternalRuntimePreOperator auto sender stopped while enqueueing.");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(
                    replay
                            ? "Interrupted while enqueueing auto replay batch."
                            : "Interrupted while enqueueing auto async send batch.",
                    e);
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
        while (sendWorkerRunning
                || (sharedAutoReplayQueue != null && !sharedAutoReplayQueue.isEmpty())
                || (sharedAutoSendQueue != null && !sharedAutoSendQueue.isEmpty())) {
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
                PendingSendBatch nextBatch =
                        sharedAutoReplayQueue == null ? null : sharedAutoReplayQueue.poll();
                if (nextBatch == null) {
                    nextBatch =
                            sharedAutoSendQueue == null
                                    ? null
                                    : sharedAutoSendQueue.poll(50L, TimeUnit.MILLISECONDS);
                }
                batch = nextBatch;
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
        final EndpointState failedState = endpointStates.get(failedEndpointIndex);
        removeActiveEndpoint(failedEndpointIndex);
        closeEndpoint(failedState, true);
        updatePrimarySocket();
        if (extraPendingBytes != null && extraPendingBytes.length > 0) {
            enqueueAutoReplayBatch(extraPendingBytes, Math.max(1, extraPendingRows), true);
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
            final int candidateEndpointIndex = activeEndpointIndices.get((start + offset) % activeSize);
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
        if (endpointState.out != null
                && endpointState.socket != null
                && (!tcpConfig.isAutoParallelismEnabled() || endpointState.in != null)) {
            return true;
        }
        final long now = System.nanoTime();
        if (endpointState.nextReconnectAtNanos > now) {
            return false;
        }

        Socket candidateSocket = null;
        BufferedInputStream candidateIn = null;
        BufferedOutputStream candidateOut = null;
        try {
            candidateSocket = connectSocket(
                    endpointState.endpoint.getHost(),
                    endpointState.endpoint.getSendPort(),
                    tcpConfig.getConnectTimeoutMs());
            candidateSocket.setTcpNoDelay(true);
            if (tcpConfig.isAutoParallelismEnabled()) {
                candidateIn =
                        new BufferedInputStream(candidateSocket.getInputStream(), tcpConfig.getBufferSize());
            }
            candidateOut = new BufferedOutputStream(candidateSocket.getOutputStream(), tcpConfig.getBufferSize());
            writeLengthPrefixedJson(candidateOut, configJson);
            candidateOut.flush();

            endpointState.socket = candidateSocket;
            endpointState.in = candidateIn;
            endpointState.out = candidateOut;
            endpointState.nextReconnectAtNanos = 0L;
            return true;
        } catch (IOException e) {
            closeQuietly(candidateIn);
            closeQuietly(candidateOut);
            closeQuietly(candidateSocket);
            endpointState.nextReconnectAtNanos = now + (long) tcpConfig.getFailoverReconnectBackoffMs() * 1_000_000L;
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
        closeQuietly(endpointState.in);
        closeQuietly(endpointState.out);
        closeQuietly(endpointState.socket);
        endpointState.in = null;
        endpointState.out = null;
        endpointState.socket = null;
        endpointState.firstBufferedAtNanos = 0L;
        endpointState.nextReconnectAtNanos = failed
                ? System.nanoTime() + (long) tcpConfig.getFailoverReconnectBackoffMs() * 1_000_000L
                : 0L;
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
        if (sharedAutoSendQueue != null) {
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

    private void stopAckReaders() {
        ackReadersRunning = false;
        if (ackReaderThreads != null) {
            for (Thread reader : ackReaderThreads) {
                if (reader != null) {
                    reader.interrupt();
                }
            }
            for (Thread reader : ackReaderThreads) {
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
            ackReaderThreads.clear();
        }
        ackReaderThreads = null;
    }

    private void startWriteTimeoutWatcher() {
        writeTimeoutWatcherRunning = true;
        writeTimeoutWatcherThread = new Thread(this::runWriteTimeoutWatcher, "external-runtime-pre-send-timeout");
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

        stopPendingBatchResender();
        stopWriteTimeoutWatcher();

        // Best-effort flush during close; ignore errors since remote may have
        // disconnected
        try {
            waitForPendingBatchAcknowledgements();
            waitForPendingSends();
            drainAckedRowWatermarks();
            drainAsyncSendFailures();
        } catch (Exception e) {
            LOG.debug("ExternalRuntimePreOperator best-effort flush failed during close.", e);
        }
        stopAckReaders();
        stopSendWorker();

        // Close sockets first to sever connections, preventing
        // BufferedOutputStream.close()
        // from attempting a flush on a broken pipe during shutdown.
        if (endpointStates != null) {
            for (EndpointState endpointState : endpointStates) {
                error = suppress(error, closeQuietly(endpointState.socket));
                endpointState.socket = null;
            }
        }

        // Now close output streams; flush will fail harmlessly since sockets are
        // already closed.
        if (endpointStates != null) {
            for (EndpointState endpointState : endpointStates) {
                closeQuietly(endpointState.in);
                endpointState.in = null;
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
        autoBatchFirstRowId = 0L;
        autoBatchFirstBufferedAtNanos = 0L;
        nextAutoBatchFlushCheckAtNanos = 0L;
        nextAutoReconnectSweepAtNanos = 0L;
        endpointSendQueues = null;
        endpointQueuedBatchCounts = null;
        sharedAutoSendQueue = null;
        sharedAutoReplayQueue = null;
        ackedRowWatermarks = null;
        asyncSendFailures = null;
        pendingSendBatches = null;
        sendWorkerRunning = false;
        ackReadersRunning = false;
        ackReaderThreads = null;
        highestAckedRowId = -1L;
        endpointWriteStartedAtNanos = null;
        writeTimeoutWatcherRunning = false;
        writeTimeoutWatcherThread = null;
        resendPendingBatchesRunning = false;
        resendPendingBatchesThread = null;
        pendingBatches = null;
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
        private BufferedInputStream in;
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

    public static final class PendingBatchState implements Serializable {
        public long firstRowId;
        public int rowCount;
        public byte[] payload;

        public PendingBatchState() {}

        private PendingBatchState(long firstRowId, int rowCount, byte[] payload) {
            this.firstRowId = firstRowId;
            this.rowCount = rowCount;
            this.payload = payload;
        }
    }

    private static final class PendingBatch {
        private final long firstRowId;
        private final long lastRowId;
        private final int rowCount;
        private final byte[] payload;
        private volatile long lastSendNanos;

        private PendingBatch(
                long firstRowId,
                long lastRowId,
                int rowCount,
                byte[] payload,
                long lastSendNanos) {
            this.firstRowId = firstRowId;
            this.lastRowId = lastRowId;
            this.rowCount = rowCount;
            this.payload = payload;
            this.lastSendNanos = lastSendNanos;
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
