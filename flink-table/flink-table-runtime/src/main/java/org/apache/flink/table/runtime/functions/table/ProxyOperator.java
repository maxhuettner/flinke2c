/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.functions.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.types.RowKind;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.Flushable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A proxy operator that forwards records through an external TCP process.
 *
 * <p>The PRE side sends input rows, the POST side receives processed rows and emits them
 * downstream.
 *
 * <p>PRE and POST send one JSON config preamble (length-prefixed) before the Arrow IPC stream
 * begins (PRE before sending, POST before reading).
 */
@Internal
public class ProxyOperator extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(ProxyOperator.class);
    private static final long serialVersionUID = 1L;

    public enum Side {
        PRE,
        POST
    }

    private final String conf;
    private final Side side;
    private final RowType rowType;

    private transient ProxyTcpConfig tcpConfig;
    private transient List<LogicalType> fieldTypes;

    // shared socket
    private transient Socket socket;

    // PRE (Arrow writer + batch state)
    private transient BufferedOutputStream out;
    private transient BufferAllocator allocator;
    private transient VectorSchemaRoot writeRoot;
    private transient ArrowStreamWriter writer;
    private transient BigIntVector writeRowIdVector;

    private transient int batchMaxRows;
    private transient int batchRowIndex;
    private transient List<FieldVector> writeVectors; // includes op column at 0
    private transient long nextRowId;

    // POST (Arrow reader + current batch cursor)
    private transient BufferedInputStream in;
    private transient ArrowStreamReader reader;
    private transient VectorSchemaRoot readRoot;
    private transient int readBatchRowCount;
    private transient int readBatchRowIndex;
    private transient List<FieldVector> readVectors;
    private transient BigIntVector readRowIdVector;
    private transient long expectedRowId;
    private transient Map<Long, RowData> reorderBuffer;

    public ProxyOperator(String conf, Side side, RowType rowType) {
        this.conf = conf == null ? "" : conf;
        this.side = side == null ? Side.PRE : side;
        this.rowType = Objects.requireNonNull(rowType, "rowType");
    }

    @Override
    public void open() throws Exception {
        super.open();

        this.tcpConfig = ProxyTcpConfig.from(conf);
        this.fieldTypes =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .collect(Collectors.toList());

        if (side == Side.PRE) {
            openPre();
        } else {
            openPost();
        }
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        if (side == Side.PRE) {
            appendRowToArrowBatch(element.getValue());
            // keep original behavior: forward input downstream unchanged
            output.collect(element);
        } else {
            final RowData outRow;
            if (tcpConfig.reorderResponses) {
                outRow = readNextOrderedRow(element.getValue().getRowKind());
            } else {
                outRow = readNextRow(element.getValue().getRowKind());
            }
            output.collect(element.replace(outRow));
        }
    }

    @Override
    public void close() throws Exception {
        IOException error = null;

        try {
            if (side == Side.PRE) {
                closePre();
            } else {
                closePost();
            }
        } catch (IOException e) {
            error = e;
        }

        error = suppress(error, closeQuietly(socket));
        socket = null;

        super.close();

        if (error != null) {
            throw error;
        }
    }

    // ------------------------------------------------------------------------
    // PRE
    // ------------------------------------------------------------------------

    private void openPre() throws Exception {
        final ProxyTcpConfig.ProxyEndpoint proxy = tcpConfig.selectedProxy;
        final int port = proxy.sendPort;
        this.socket = connectSocket(proxy.host, port, tcpConfig.connectTimeoutMs);
        this.out = new BufferedOutputStream(socket.getOutputStream(), tcpConfig.bufferSize);

        // 1) PREAMBLE: send one config JSON message BEFORE Arrow starts
        final String configJson = buildConfigJson(side);
        writeLengthPrefixedJson(out, configJson);

        // 2) Arrow stream starts immediately after preamble bytes
        this.batchMaxRows = tcpConfig.batchMaxRows;
        this.batchRowIndex = 0;
        this.nextRowId = 0L;

        this.allocator = new RootAllocator(Long.MAX_VALUE);

        final Schema arrowSchema = toArrowSchema(rowType, tcpConfig.reorderResponses);
        this.writeRoot = VectorSchemaRoot.create(arrowSchema, allocator);
        this.writeVectors = writeRoot.getFieldVectors();
        this.writeRowIdVector =
                tcpConfig.reorderResponses ? (BigIntVector) writeVectors.get(1) : null;

        this.writer = new ArrowStreamWriter(writeRoot, /* DictionaryProvider */ null, out);
        this.writer.start(); // writes schema header

        LOG.info(
                "ProxyOperator {} connected to {}:{} (rowType={}, batchMaxRows={}, sentConfigBytes={})",
                side,
                proxy.host,
                port,
                rowType,
                batchMaxRows,
                configJson.getBytes(StandardCharsets.UTF_8).length);
    }

    private void appendRowToArrowBatch(RowData row) throws IOException {
        if (batchRowIndex >= batchMaxRows) {
            flushArrowBatch();
        }

        // column 0: op
        ((IntVector) writeVectors.get(0)).setSafe(batchRowIndex, rowKindToOp(row.getRowKind()));

        final int payloadOffset;
        if (tcpConfig.reorderResponses) {
            writeRowIdVector.setSafe(batchRowIndex, nextRowId++);
            payloadOffset = 2;
        } else {
            payloadOffset = 1;
        }

        // columns payloadOffset..N: fields
        for (int i = 0; i < fieldTypes.size(); i++) {
            final int col = i + payloadOffset;
            final LogicalType type = fieldTypes.get(i);
            final FieldVector v = writeVectors.get(col);

            if (row.isNullAt(i)) {
                v.setNull(batchRowIndex);
                continue;
            }

            writeFieldValue(v, type, row, i, batchRowIndex);
        }

        batchRowIndex++;

        // optional: flush per-row if you really want minimum latency (not recommended generally)
        if (tcpConfig.flushOnWrite) {
            flushArrowBatch();
        } else if (tcpConfig.flushEvery > 0 && batchRowIndex >= tcpConfig.flushEvery) {
            flushArrowBatch();
        }
    }

    private void flushArrowBatch() throws IOException {
        if (batchRowIndex <= 0) {
            return;
        }

        writeRoot.setRowCount(batchRowIndex);
        writer.writeBatch();
        out.flush();

        // reset vectors for next batch (cheap; keeps allocated buffers)
        for (FieldVector v : writeVectors) {
            v.setValueCount(0);
        }
        batchRowIndex = 0;
    }

    private void closePre() throws IOException {
        IOException error = null;

        // flush any remaining rows as a final smaller batch
        error = suppress(error, tryFlushRemaining());

        // end stream marker
        error = suppress(error, closeArrowWriter());

        error = suppress(error, flushAndClose(out));
        out = null;

        error = suppress(error, closeQuietly(writeRoot));
        writeRoot = null;

        error = suppress(error, closeQuietly(allocator));
        allocator = null;

        if (error != null) {
            throw error;
        }
    }

    private IOException tryFlushRemaining() {
        try {
            flushArrowBatch();
            return null;
        } catch (IOException e) {
            return e;
        }
    }

    private IOException closeArrowWriter() {
        if (writer == null) {
            return null;
        }
        try {
            writer.end();
            writer.close();
            return null;
        } catch (IOException e) {
            return e;
        } finally {
            writer = null;
            writeVectors = null;
        }
    }

    // ------------------------------------------------------------------------
    // POST
    // ------------------------------------------------------------------------

    private void openPost() throws IOException {
        final ProxyTcpConfig.ProxyEndpoint proxy = tcpConfig.selectedProxy;
        final int port = proxy.receivePort;
        this.socket = connectSocket(proxy.host, port, tcpConfig.connectTimeoutMs);

        if (tcpConfig.readTimeoutMs > 0) {
            socket.setSoTimeout(tcpConfig.readTimeoutMs);
        }

        this.in = new BufferedInputStream(socket.getInputStream(), tcpConfig.bufferSize);

        // POST registers itself with the proxy before Arrow stream starts
        final BufferedOutputStream postOut =
                new BufferedOutputStream(socket.getOutputStream(), tcpConfig.bufferSize);
        final String configJson = buildConfigJson(side);
        writeLengthPrefixedJson(postOut, configJson);

        // Arrow stream begins immediately after the preamble
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.reader = new ArrowStreamReader(in, allocator);
        this.readRoot = reader.getVectorSchemaRoot();
        this.readVectors = readRoot.getFieldVectors();
        this.readBatchRowCount = 0;
        this.readBatchRowIndex = 0;
        this.readRowIdVector =
                tcpConfig.reorderResponses ? (BigIntVector) readVectors.get(1) : null;
        this.expectedRowId = 0L;
        this.reorderBuffer = tcpConfig.reorderResponses ? new HashMap<>() : null;

        // prime first batch
        loadNextBatchOrEof();

        LOG.info(
                "ProxyOperator {} connected to {}:{} (rowType={}, sentConfigBytes={})",
                side,
                proxy.host,
                port,
                rowType,
                configJson.getBytes(StandardCharsets.UTF_8).length);
    }

    private RowData readNextRow(RowKind fallbackKind) throws IOException {
        if (reader == null) {
            throw new IOException("ProxyOperator reader not initialized");
        }

        while (readBatchRowIndex >= readBatchRowCount) {
            if (!loadNextBatchOrEof()) {
                throw new EOFException("ProxyOperator reached end of Arrow stream");
            }
        }

        final int fieldCount = fieldTypes.size();
        final GenericRowData outRow = new GenericRowData(fieldCount);

        // op is column 0
        final int op = ((IntVector) readVectors.get(0)).get(readBatchRowIndex);
        outRow.setRowKind(opToRowKind(op, fallbackKind));

        final int payloadOffset = tcpConfig.reorderResponses ? 2 : 1;
        for (int i = 0; i < fieldCount; i++) {
            final int col = i + payloadOffset;
            final FieldVector v = readVectors.get(col);
            if (v.isNull(readBatchRowIndex)) {
                outRow.setField(i, null);
            } else {
                outRow.setField(i, readFieldValue(v, fieldTypes.get(i), readBatchRowIndex));
            }
        }

        readBatchRowIndex++;
        return outRow;
    }

    private RowWithId readNextRowWithId(RowKind fallbackKind) throws IOException {
        if (reader == null) {
            throw new IOException("ProxyOperator reader not initialized");
        }

        while (readBatchRowIndex >= readBatchRowCount) {
            if (!loadNextBatchOrEof()) {
                throw new EOFException("ProxyOperator reached end of Arrow stream");
            }
        }

        final long rowId = readRowIdVector.get(readBatchRowIndex);
        final RowData row = readNextRow(fallbackKind);
        return new RowWithId(rowId, row);
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
                if (reorderBuffer.size() > tcpConfig.reorderMaxBuffer) {
                    throw new IOException(
                            "ProxyOperator reorder buffer exceeded "
                                    + tcpConfig.reorderMaxBuffer
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

    private boolean loadNextBatchOrEof() throws IOException {
        final boolean ok = reader.loadNextBatch();
        if (!ok) {
            return false;
        }
        this.readBatchRowCount = readRoot.getRowCount();
        this.readBatchRowIndex = 0;
        return true;
    }

    private void closePost() throws IOException {
        IOException error = null;

        error = suppress(error, closeQuietly(reader));
        reader = null;

        error = suppress(error, closeQuietly(readRoot));
        readRoot = null;

        error = suppress(error, closeQuietly(in));
        in = null;

        error = suppress(error, closeQuietly(allocator));
        allocator = null;

        readVectors = null;

        if (error != null) {
            throw error;
        }
    }

    // ------------------------------------------------------------------------
    // PRE config JSON preamble
    // ------------------------------------------------------------------------

    private String buildConfigJson(Side side) {
        // Provided snippet (may need import/package adjustment depending on Flink version)
        final StreamingRuntimeContext ctx = (StreamingRuntimeContext) getRuntimeContext();

        final TaskInfo taskInfo = getRuntimeContext().getTaskInfo();
        final int subtask = taskInfo.getIndexOfThisSubtask();
        final int parallelism = taskInfo.getNumberOfParallelSubtasks();
        String tmHost = "unknown";
        String jobId = "unknown";

        try {
            // These types/methods exist in many Flink versions; adjust if your version differs.
            final Object tm = ctx.getTaskManagerRuntimeInfo();
            tmHost = (String) tm.getClass().getMethod("getTaskManagerExternalAddress").invoke(tm);

            final Object jobInfo = ctx.getJobInfo();
            final Object jid = jobInfo.getClass().getMethod("getJobId").invoke(jobInfo);
            jobId = String.valueOf(jid);
        } catch (Exception e) {
            // Fall back to what we can reliably get without version-specific classes
            LOG.warn("Failed to extract full runtime info for config preamble; using fallbacks", e);
        }

        // minimal JSON (no dependency). Escape strings.
        final StringBuilder sb = new StringBuilder(128);
        sb.append('{')
                .append("\"role\":\"").append(side == Side.PRE ? "pre" : "post").append("\",")
                .append("\"tmHost\":\"").append(jsonEscape(tmHost)).append("\",")
                .append("\"subtask\":").append(subtask).append(',')
                .append("\"parallelism\":").append(parallelism).append(',')
                .append("\"jobId\":\"").append(jsonEscape(jobId)).append('"');
        if (tcpConfig.calcFieldIndex != null) {
            sb.append(",\"calcFieldIndex\":").append(tcpConfig.calcFieldIndex);
        }
        if (tcpConfig.calcFieldName != null && !tcpConfig.calcFieldName.isEmpty()) {
            sb.append(",\"calcFieldName\":\"")
                    .append(jsonEscape(tcpConfig.calcFieldName))
                    .append('"');
        }
        if (tcpConfig.reorderResponses) {
            sb.append(",\"reorderResponses\":true");
        }
        sb.append('}');
        return sb.toString();
    }

    private static void writeLengthPrefixedJson(OutputStream out, String json) throws IOException {
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        final DataOutputStream dos = new DataOutputStream(out);
        dos.writeInt(bytes.length); // big-endian length
        dos.write(bytes);
        dos.flush(); // ensure preamble is out before Arrow schema starts
    }

    private static String jsonEscape(String s) {
        if (s == null) {
            return "";
        }
        // small escape set is enough here
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    // ------------------------------------------------------------------------
    // Arrow schema + value mapping
    // ------------------------------------------------------------------------

    private static Schema toArrowSchema(RowType rowType, boolean includeRowId) {
        final List<Field> fields = new ArrayList<>();

        // column 0: op
        fields.add(new Field("__op", FieldType.notNullable(new ArrowType.Int(32, true)), null));

        if (includeRowId) {
            // column 1: row id (for reordering)
            fields.add(new Field("__rid", FieldType.notNullable(new ArrowType.Int(64, true)), null));
        }

        // columns (1/2)..N: row fields in order
        for (RowType.RowField f : rowType.getFields()) {
            final String name = f.getName();
            final LogicalType t = f.getType();
            fields.add(new Field(name, FieldType.nullable(toArrowType(t)), /* children */ null));
        }
        return new Schema(fields);
    }

    private static ArrowType toArrowType(LogicalType t) {
        switch (t.getTypeRoot()) {
            case BOOLEAN:
                return ArrowType.Bool.INSTANCE;
            case INTEGER:
                return new ArrowType.Int(32, true);
            case BIGINT:
                return new ArrowType.Int(64, true);
            case FLOAT:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case DOUBLE:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case CHAR:
            case VARCHAR:
                return ArrowType.Utf8.INSTANCE;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // represent as millis since epoch
                return new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, null);
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return new ArrowType.Int(32, true);
            case BINARY:
            case VARBINARY:
                return ArrowType.Binary.INSTANCE;
            case DECIMAL:
                final DecimalType dt = (DecimalType) t;
                return new ArrowType.Decimal(dt.getPrecision(), dt.getScale(), 128);
            default:
                // default to Utf8 to avoid hard failures; you can tighten this later
                return ArrowType.Utf8.INSTANCE;
        }
    }

    private static void writeFieldValue(FieldVector v, LogicalType type, RowData row, int pos, int idx) {
        switch (type.getTypeRoot()) {
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                ((IntVector) v).setSafe(idx, row.getInt(pos));
                return;
            case BIGINT:
                ((BigIntVector) v).setSafe(idx, row.getLong(pos));
                return;
            case FLOAT:
                ((Float4Vector) v).setSafe(idx, row.getFloat(pos));
                return;
            case DOUBLE:
                ((Float8Vector) v).setSafe(idx, row.getDouble(pos));
                return;
            case CHAR:
            case VARCHAR: {
                final byte[] bytes = row.getString(pos).toString().getBytes(StandardCharsets.UTF_8);
                ((VarCharVector) v).setSafe(idx, bytes, 0, bytes.length);
                return;
            }
            case TIMESTAMP_WITHOUT_TIME_ZONE: {
                final long ms = row.getTimestamp(pos, ((TimestampType) type).getPrecision()).getMillisecond();
                ((TimeStampMilliVector) v).setSafe(idx, ms);
                return;
            }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
                final long ms =
                        row.getTimestamp(pos, ((LocalZonedTimestampType) type).getPrecision())
                                .getMillisecond();
                ((TimeStampMilliVector) v).setSafe(idx, ms);
                return;
            }
            case DECIMAL: {
                final DecimalType dt = (DecimalType) type;
                final DecimalData dec = row.getDecimal(pos, dt.getPrecision(), dt.getScale());
                ((org.apache.arrow.vector.DecimalVector) v).setSafe(idx, dec.toBigDecimal());
                return;
            }
            case BINARY:
            case VARBINARY: {
                final byte[] bytes = row.getBinary(pos);
                ((org.apache.arrow.vector.VarBinaryVector) v).setSafe(idx, bytes);
                return;
            }
            default:
                // unsupported => null (already handled before calling)
                v.setNull(idx);
        }
    }

    private static Object readFieldValue(FieldVector v, LogicalType type, int idx) {
        switch (type.getTypeRoot()) {
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return ((IntVector) v).get(idx);
            case BIGINT:
                return ((BigIntVector) v).get(idx);
            case FLOAT:
                return ((Float4Vector) v).get(idx);
            case DOUBLE:
                return ((Float8Vector) v).get(idx);
            case CHAR:
            case VARCHAR: {
                // getObject returns Text/byte[] depending on version; toString is fine here
                final Object o = v.getObject(idx);
                return o == null ? null : StringData.fromString(o.toString());
            }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TimestampData.fromEpochMillis(((TimeStampMilliVector) v).get(idx));
            case DECIMAL: {
                final DecimalType dt = (DecimalType) type;
                final BigDecimal bd = ((org.apache.arrow.vector.DecimalVector) v).getObject(idx);
                return DecimalData.fromBigDecimal(bd, dt.getPrecision(), dt.getScale());
            }
            case BINARY:
            case VARBINARY:
                return ((org.apache.arrow.vector.VarBinaryVector) v).get(idx);
            default:
                return null;
        }
    }

    // ------------------------------------------------------------------------
    // RowKind <-> op
    // ------------------------------------------------------------------------

    private static int rowKindToOp(RowKind kind) {
        switch (kind) {
            case INSERT:
                return 0;
            case UPDATE_AFTER:
                return 1;
            case UPDATE_BEFORE:
                return 2;
            case DELETE:
                return 3;
            default:
                return 127;
        }
    }

    private static RowKind opToRowKind(int op, RowKind fallback) {
        switch (op) {
            case 0:
                return RowKind.INSERT;
            case 1:
                return RowKind.UPDATE_AFTER;
            case 2:
                return RowKind.UPDATE_BEFORE;
            case 3:
                return RowKind.DELETE;
            default:
                return fallback;
        }
    }

    // ------------------------------------------------------------------------
    // IO helpers
    // ------------------------------------------------------------------------

    private static Socket connectSocket(String host, int port, int connectTimeoutMs)
            throws IOException {
        final Socket socket = new Socket();
        socket.setTcpNoDelay(true);
        socket.connect(new InetSocketAddress(host, port), connectTimeoutMs);
        return socket;
    }

    private static IOException closeQuietly(AutoCloseable c) {
        if (c == null) {
            return null;
        }
        try {
            c.close();
            return null;
        } catch(IOException e) {
            return e;
        } catch (Exception e) {
            return new IOException(e);
        }
    }

    private static IOException flushAndClose(Object o) {
        if (o == null) {
            return null;
        }
        IOException err = null;
        try {
            if (o instanceof Flushable) {
                ((Flushable) o).flush();
            }
        } catch (IOException e) {
            err = e;
        }
        if (o instanceof AutoCloseable) {
            err = suppress(err, closeQuietly((AutoCloseable) o));
        }
        return err;
    }

    private static IOException suppress(IOException existing, IOException next) {
        if (existing == null) {
            return next;
        }
        if (next != null) {
            existing.addSuppressed(next);
        }
        return existing;
    }

    // ------------------------------------------------------------------------
    // Config
    // ------------------------------------------------------------------------

    private static final class ProxyTcpConfig {
        private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
        private static final int DEFAULT_CONNECT_TIMEOUT_MS = 10_000;
        private static final int DEFAULT_MAX_FRAME_SIZE = 64 * 1024 * 1024;

        // Arrow batching defaults (you can tune)
        private static final int DEFAULT_BATCH_MAX_ROWS = 8192;
        private static final int DEFAULT_REORDER_MAX_BUFFER = 10000;

        private final List<ProxyEndpoint> proxies;
        private final ProxyEndpoint selectedProxy;
        private final int bufferSize;
        private final int connectTimeoutMs;
        private final int readTimeoutMs;
        private final int maxFrameSize;
        private final Integer calcFieldIndex;
        private final String calcFieldName;

        // re-used knobs (interpreted for Arrow)
        private final boolean flushOnWrite; // if true, flush after each append (low latency, low throughput)
        private final int flushEvery;       // if >0, flush once batch has this many rows (<= batchMaxRows)
        private final int batchMaxRows;
        private final boolean reorderResponses;
        private final int reorderMaxBuffer;

        private ProxyTcpConfig(
                int bufferSize,
                int connectTimeoutMs,
                int readTimeoutMs,
                int maxFrameSize,
                boolean flushOnWrite,
                int flushEvery,
                int batchMaxRows,
                Integer calcFieldIndex,
                String calcFieldName,
                boolean reorderResponses,
                int reorderMaxBuffer,
                List<ProxyEndpoint> proxies,
                ProxyEndpoint selectedProxy) {
            this.bufferSize = bufferSize;
            this.connectTimeoutMs = connectTimeoutMs;
            this.readTimeoutMs = readTimeoutMs;
            this.maxFrameSize = maxFrameSize;
            this.flushOnWrite = flushOnWrite;
            this.flushEvery = flushEvery;
            this.batchMaxRows = batchMaxRows;
            this.calcFieldIndex = calcFieldIndex;
            this.calcFieldName = calcFieldName;
            this.reorderResponses = reorderResponses;
            this.reorderMaxBuffer = reorderMaxBuffer;
            this.proxies = proxies;
            this.selectedProxy = selectedProxy;
        }

        static ProxyTcpConfig from(String conf) {
            final Map<String, String> map = parse(conf);

            final int bufferSize = parseInt(map.get("buffersize"), DEFAULT_BUFFER_SIZE);
            final int connectTimeoutMs =
                    parseInt(map.get("connecttimeoutms"), DEFAULT_CONNECT_TIMEOUT_MS);
            final int readTimeoutMs = parseInt(map.get("readtimeoutms"), 0);
            final int maxFrameSize = parseInt(map.get("maxframesize"), DEFAULT_MAX_FRAME_SIZE);

            final boolean flushOnWrite = parseBoolean(map.get("flush"), false);
            final int flushEvery = parseInt(map.get("flushevery"), 0);
            final int batchMaxRows = parseInt(map.get("batchmaxrows"), DEFAULT_BATCH_MAX_ROWS);
            final Integer calcFieldIndex = parseInt(map.get("calcfieldindex"));
            final String calcFieldName = map.get("calcfieldname");
            final boolean reorderResponses =
                    parseBoolean(firstNonNull(map, "reorder", "correlate"), false);
            final int reorderMaxBuffer =
                    parseInt(map.get("reordermax"), DEFAULT_REORDER_MAX_BUFFER);

            final List<ProxyEndpoint> proxies = parseProxies(map.get("proxies"));
            final ProxyEndpoint selectedProxy = proxies.get(0);

            return new ProxyTcpConfig(
                    bufferSize,
                    connectTimeoutMs,
                    readTimeoutMs,
                    maxFrameSize,
                    flushOnWrite,
                    flushEvery,
                    batchMaxRows,
                    calcFieldIndex,
                    calcFieldName,
                    reorderResponses,
                    reorderMaxBuffer,
                    proxies,
                    selectedProxy);
        }

        private static List<ProxyEndpoint> parseProxies(String value) {
            if (value == null || value.trim().isEmpty()) {
                throw new IllegalArgumentException(
                        "ProxyOperator requires proxies=<host:port> or proxies=<host:send:recv> in conf.");
            }
            final List<ProxyEndpoint> proxies = new ArrayList<>();
            final String[] entries = value.split(",");
            for (String entry : entries) {
                final String trimmed = entry.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                final String[] parts = trimmed.split(":");
                if (parts.length == 1) {
                    throw new IllegalArgumentException(
                            "Invalid proxies entry: " + trimmed + " (expected host:port or host:send:recv)");
                } else if (parts.length == 2) {
                    final String h = parts[0].trim();
                    final int port = Integer.parseInt(parts[1].trim());
                    proxies.add(new ProxyEndpoint(h, port, port));
                } else if (parts.length == 3) {
                    final String h = parts[0].trim();
                    final int send = Integer.parseInt(parts[1].trim());
                    final int recv = Integer.parseInt(parts[2].trim());
                    proxies.add(new ProxyEndpoint(h, send, recv));
                } else {
                    throw new IllegalArgumentException(
                            "Invalid proxies entry: " + trimmed + " (expected host:port or host:send:recv)");
                }
            }
            if (proxies.isEmpty()) {
                throw new IllegalArgumentException(
                        "ProxyOperator requires at least one proxy entry in proxies=...");
            }
            return proxies;
        }

        private static final class ProxyEndpoint {
            private final String host;
            private final int sendPort;
            private final int receivePort;

            private ProxyEndpoint(String host, int sendPort, int receivePort) {
                this.host = host;
                this.sendPort = sendPort;
                this.receivePort = receivePort;
            }
        }

        private static Map<String, String> parse(String conf) {
            final Map<String, String> map = new HashMap<>();
            if (conf == null || conf.isEmpty()) {
                return map;
            }
            final String[] parts = conf.split(";");
            for (String part : parts) {
                final String trimmed = part.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                final int idx = trimmed.indexOf('=');
                if (idx <= 0 || idx == trimmed.length() - 1) {
                    continue;
                }
                final String key = trimmed.substring(0, idx).trim().toLowerCase(Locale.ROOT);
                final String value = trimmed.substring(idx + 1).trim();
                if (!key.isEmpty()) {
                    map.put(key, value);
                }
            }
            return map;
        }

        private static String firstNonNull(Map<String, String> map, String... keys) {
            for (String key : keys) {
                final String value = map.get(key);
                if (value != null && !value.isEmpty()) {
                    return value;
                }
            }
            return null;
        }

        private static Integer parseInt(String value) {
            if (value == null || value.isEmpty()) {
                return null;
            }
            return Integer.parseInt(value);
        }

        private static int parseInt(String value, int defaultValue) {
            if (value == null || value.isEmpty()) {
                return defaultValue;
            }
            return Integer.parseInt(value);
        }

        private static boolean parseBoolean(String value, boolean defaultValue) {
            if (value == null || value.isEmpty()) {
                return defaultValue;
            }
            return Boolean.parseBoolean(value);
        }
    }
}
