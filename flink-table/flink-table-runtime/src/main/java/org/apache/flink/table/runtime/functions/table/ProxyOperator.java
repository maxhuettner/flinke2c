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
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
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

import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
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
    private transient Socket socket;
    private transient ServerSocket serverSocket;
    private transient BufferedOutputStream out;
    private transient MessagePacker packer;
    private transient BufferedInputStream in;
    private transient MessageUnpacker unpacker;
    private transient int writesSinceFlush;

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
            final int port = tcpConfig.sendPort;
            this.socket = connectSocket(tcpConfig.host, port, tcpConfig.connectTimeoutMs);
            this.out =
                    new BufferedOutputStream(socket.getOutputStream(), tcpConfig.bufferSize);
            this.packer = MessagePack.newDefaultPacker(out);
            this.writesSinceFlush = 0;
            LOG.info(
                    "ProxyOperator {} connected to {}:{} (rowType={})",
                    side,
                    tcpConfig.host,
                    port,
                    rowType);
        } else {
            final int port = tcpConfig.receivePort;
            this.serverSocket = new ServerSocket();
            this.serverSocket.setReuseAddress(true);
            this.serverSocket.bind(new InetSocketAddress(port));
            LOG.info("ProxyOperator {} listening on 0.0.0.0:{} ...", side, port);
            this.socket = this.serverSocket.accept();
            this.socket.setTcpNoDelay(true);
            if (tcpConfig.readTimeoutMs > 0) {
                socket.setSoTimeout(tcpConfig.readTimeoutMs);
            }
            this.in =
                    new BufferedInputStream(socket.getInputStream(), tcpConfig.bufferSize);
            this.unpacker = MessagePack.newDefaultUnpacker(in);
            LOG.info(
                    "ProxyOperator {} connected to {}:{} (rowType={})",
                    side,
                    tcpConfig.host,
                    port,
                    rowType);
        }
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        if (side == Side.PRE) {
            writeRow(element.getValue());
            output.collect(element);
        } else {
            final RowData outRow = readRow(element.getValue().getRowKind());
            output.collect(element.replace(outRow));
        }
    }

    @Override
    public void close() throws Exception {
        IOException error = null;
        if (packer != null) {
            try {
                packer.flush();
                packer.close();
            } catch (IOException e) {
                error = e;
            } finally {
                packer = null;
                out = null;
            }
        }
        if (out != null) {
            try {
                out.flush();
                out.close();
            } catch (IOException e) {
                error = e;
            } finally {
                out = null;
            }
        }
        if (unpacker != null) {
            try {
                unpacker.close();
            } catch (IOException e) {
                error = e;
            } finally {
                unpacker = null;
                in = null;
            }
        }
        if (in != null) {
            try {
                in.close();
            } catch (IOException e) {
                error = e;
            } finally {
                in = null;
            }
        }
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
                error = e;
            } finally {
                socket = null;
            }
        }
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                error = e;
            } finally {
                serverSocket = null;
            }
        }
        super.close();
        if (error != null) {
            throw error;
        }
    }

    private void writeRow(RowData row) throws IOException {
        packer.packArrayHeader(fieldTypes.size() + 1);
        packer.packInt(rowKindToOp(row.getRowKind()));
        for (int i = 0; i < fieldTypes.size(); i++) {
            final LogicalType type = fieldTypes.get(i);
            if (row.isNullAt(i)) {
                packer.packNil();
            } else {
                packValue(packer, row, i, type);
            }
        }
        if (tcpConfig.flushOnWrite) {
            packer.flush();
        } else if (tcpConfig.flushEvery > 0) {
            writesSinceFlush++;
            if (writesSinceFlush >= tcpConfig.flushEvery) {
                packer.flush();
                writesSinceFlush = 0;
            }
        }
    }

    private RowData readRow(RowKind fallbackKind) throws IOException {
        if (unpacker == null) {
            throw new IOException("ProxyOperator unpacker not initialized");
        }
        try {
            return decodeRow(unpacker, fallbackKind);
        } catch (EOFException e) {
            throw new EOFException("ProxyOperator reached end of stream");
        }
    }

    private RowData decodeRow(MessageUnpacker unpacker, RowKind fallbackKind) throws IOException {
        final int size = unpacker.unpackArrayHeader();
        final int fieldCount = fieldTypes.size();
        final RowKind rowKind;
        if (size == fieldCount + 1) {
            rowKind = opToRowKind(unpacker.unpackInt(), fallbackKind);
        } else if (size == fieldCount) {
            rowKind = fallbackKind;
        } else {
            throw new IOException(
                    "ProxyOperator unexpected record size: "
                            + size
                            + " (fields="
                            + fieldCount
                            + ")");
        }
        final GenericRowData row = new GenericRowData(fieldCount);
        row.setRowKind(rowKind);
        for (int i = 0; i < fieldCount; i++) {
            if (unpacker.tryUnpackNil()) {
                row.setField(i, null);
            } else {
                row.setField(i, unpackValue(unpacker, fieldTypes.get(i)));
            }
        }
        return row;
    }

    private static void packValue(MessagePacker packer, RowData row, int pos, LogicalType type)
            throws IOException {
        final LogicalTypeRoot root = type.getTypeRoot();
        switch (root) {
            case BOOLEAN:
                packer.packBoolean(row.getBoolean(pos));
                break;
            case INTEGER:
                packer.packInt(row.getInt(pos));
                break;
            case BIGINT:
                packer.packLong(row.getLong(pos));
                break;
            case FLOAT:
                packer.packFloat(row.getFloat(pos));
                break;
            case DOUBLE:
                packer.packDouble(row.getDouble(pos));
                break;
            case CHAR:
            case VARCHAR:
                packer.packString(row.getString(pos).toString());
                break;
            case DECIMAL:
                final DecimalType decimalType = (DecimalType) type;
                final DecimalData decimal =
                        row.getDecimal(pos, decimalType.getPrecision(), decimalType.getScale());
                packer.packString(decimal.toBigDecimal().toPlainString());
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                packer.packLong(
                        row.getTimestamp(pos, ((TimestampType) type).getPrecision())
                                .getMillisecond());
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                packer.packLong(
                        row.getTimestamp(pos, ((LocalZonedTimestampType) type).getPrecision())
                                .getMillisecond());
                break;
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                packer.packInt(row.getInt(pos));
                break;
            case BINARY:
            case VARBINARY:
                final byte[] bytes = row.getBinary(pos);
                packer.packBinaryHeader(bytes.length);
                packer.writePayload(bytes);
                break;
            default:
                packer.packNil();
        }
    }

    private static Object unpackValue(MessageUnpacker unpacker, LogicalType type) throws IOException {
        final LogicalTypeRoot root = type.getTypeRoot();
        switch (root) {
            case BOOLEAN:
                return unpacker.unpackBoolean();
            case INTEGER:
                return unpacker.unpackInt();
            case BIGINT:
                return unpacker.unpackLong();
            case FLOAT:
                return unpacker.unpackFloat();
            case DOUBLE:
                return unpacker.unpackDouble();
            case CHAR:
            case VARCHAR:
                return StringData.fromString(unpacker.unpackString());
            case DECIMAL:
                final DecimalType decimalType = (DecimalType) type;
                final String asString = unpacker.unpackString();
                final BigDecimal bigDecimal = new BigDecimal(asString);
                return DecimalData.fromBigDecimal(
                        bigDecimal, decimalType.getPrecision(), decimalType.getScale());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TimestampData.fromEpochMillis(unpacker.unpackLong());
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return unpacker.unpackInt();
            case BINARY:
            case VARBINARY:
                final int len = unpacker.unpackBinaryHeader();
                final byte[] bytes = new byte[len];
                unpacker.readPayload(bytes);
                return bytes;
            default:
                unpacker.skipValue();
                return null;
        }
    }

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

    private static Socket connectSocket(String host, int port, int connectTimeoutMs)
            throws IOException {
        final Socket socket = new Socket();
        socket.setTcpNoDelay(true);
        socket.connect(new InetSocketAddress(host, port), connectTimeoutMs);
        return socket;
    }

    private static final class ProxyTcpConfig {
        private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
        private static final int DEFAULT_CONNECT_TIMEOUT_MS = 10_000;
        private static final int DEFAULT_MAX_FRAME_SIZE = 64 * 1024 * 1024;

        private final String host;
        private final int sendPort;
        private final int receivePort;
        private final int bufferSize;
        private final int connectTimeoutMs;
        private final int readTimeoutMs;
        private final int maxFrameSize;
        private final boolean flushOnWrite;
        private final int flushEvery;

        private ProxyTcpConfig(
                String host,
                int sendPort,
                int receivePort,
                int bufferSize,
                int connectTimeoutMs,
                int readTimeoutMs,
                int maxFrameSize,
                boolean flushOnWrite,
                int flushEvery) {
            this.host = host;
            this.sendPort = sendPort;
            this.receivePort = receivePort;
            this.bufferSize = bufferSize;
            this.connectTimeoutMs = connectTimeoutMs;
            this.readTimeoutMs = readTimeoutMs;
            this.maxFrameSize = maxFrameSize;
            this.flushOnWrite = flushOnWrite;
            this.flushEvery = flushEvery;
        }

        static ProxyTcpConfig from(String conf) {
            final Map<String, String> map = parse(conf);
            final String host = map.getOrDefault("host", "localhost");
            final Integer port = parseInt(map.get("port"));
            Integer sendPort = parseInt(firstNonNull(map, "sendport", "outport"));
            Integer receivePort = parseInt(firstNonNull(map, "recvport", "receiveport", "inport"));
            if (sendPort == null) {
                sendPort = port;
            }
            if (receivePort == null) {
                receivePort = port;
            }
            if (sendPort == null || receivePort == null) {
                throw new IllegalArgumentException(
                        "ProxyOperator requires port or sendPort/receivePort in conf: " + conf);
            }
            final int bufferSize = parseInt(map.get("buffersize"), DEFAULT_BUFFER_SIZE);
            final int connectTimeoutMs =
                    parseInt(map.get("connecttimeoutms"), DEFAULT_CONNECT_TIMEOUT_MS);
            final int readTimeoutMs = parseInt(map.get("readtimeoutms"), 0);
            final int maxFrameSize = parseInt(map.get("maxframesize"), DEFAULT_MAX_FRAME_SIZE);
            final boolean flushOnWrite = parseBoolean(map.get("flush"), true);
            final int flushEvery = parseInt(map.get("flushevery"), 0);
            return new ProxyTcpConfig(
                    host,
                    sendPort,
                    receivePort,
                    bufferSize,
                    connectTimeoutMs,
                    readTimeoutMs,
                    maxFrameSize,
                    flushOnWrite,
                    flushEvery);
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
