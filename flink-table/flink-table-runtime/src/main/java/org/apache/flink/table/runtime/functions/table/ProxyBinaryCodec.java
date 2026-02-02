package org.apache.flink.table.runtime.functions.table;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

/**
 * Custom binary row codec implementing:
 * [int32_be frameLen][payload]
 * payload := int32 __op, [int64 __rowId if enabled], nullBitmap, values...
 */
final class ProxyBinaryCodec {

    enum WireType {
        INT32,
        INT64,
        FLOAT32,
        FLOAT64,
        BOOL,
        STRING,                 // [int32_be len][utf8]
        BYTES,                  // [int32_be len][raw]
        TIMESTAMP_MILLIS,       // int64 epoch millis
        DECIMAL_UNSCALED_I64,   // int64 unscaled (precision <= 18)
        DECIMAL_UNSCALED_BYTES  // [int32_be len][two's complement bytes] (precision > 18)
    }

    private final boolean includeRowId;
    private final int maxFrameSize;

    // PRE write schema
    private final WireType[] writeWireTypes;
    private final LogicalType[] writeTargetTypes;
    private final LogicalTypeRoot[] writeSourceRoots;
    private final int[] writeTargetPrecision;
    private final int[] writeTargetScale;
    private final int[] writeSourcePrecision;
    private final int[] writeSourceScale;
    private final int[] writeTimestampPrecision;

    // POST read schema
    private final WireType[] readWireTypes;
    private final LogicalType[] readSourceTypes;
    private final LogicalType[] readTargetTypes;

    // reusable buffers
    private final GrowableBuffer outBuf = new GrowableBuffer(8 * 1024);
    private byte[] frameReadBuf = new byte[8 * 1024];
    private Object[] readFieldBuffer;

    private static final long[] POW10 = initPow10();

    ProxyBinaryCodec(
            boolean includeRowId,
            int maxFrameSize,
            @Nullable WireType[] writeWireTypes,
            @Nullable LogicalType[] writeTargetTypes,
            @Nullable LogicalTypeRoot[] writeSourceRoots,
            @Nullable int[] writeTargetPrecision,
            @Nullable int[] writeTargetScale,
            @Nullable int[] writeSourcePrecision,
            @Nullable int[] writeSourceScale,
            @Nullable int[] writeTimestampPrecision,
            @Nullable WireType[] readWireTypes,
            @Nullable LogicalType[] readSourceTypes,
            @Nullable LogicalType[] readTargetTypes) {

        this.includeRowId = includeRowId;
        this.maxFrameSize = maxFrameSize;

        this.writeWireTypes = writeWireTypes;
        this.writeTargetTypes = writeTargetTypes;
        this.writeSourceRoots = writeSourceRoots;
        this.writeTargetPrecision = writeTargetPrecision;
        this.writeTargetScale = writeTargetScale;
        this.writeSourcePrecision = writeSourcePrecision;
        this.writeSourceScale = writeSourceScale;
        this.writeTimestampPrecision = writeTimestampPrecision;

        this.readWireTypes = readWireTypes;
        this.readSourceTypes = readSourceTypes;
        this.readTargetTypes = readTargetTypes;

        if (readWireTypes != null) {
            this.readFieldBuffer = new Object[readWireTypes.length];
        }
    }

    // ---------------------------------------------------------------------
    // PRE: write one framed row
    // ---------------------------------------------------------------------

    void writeFramedRow(
            OutputStream out,
            RowData row,
            int[] payloadFieldIndices,
            long rowId) throws IOException {

        if (writeWireTypes == null) {
            throw new IOException("ProxyBinaryCodec not configured for writing");
        }

        final int nFields = writeWireTypes.length;
        final int nullBytes = (nFields + 7) >>> 3;

        outBuf.reset();

        // __op
        outBuf.putIntBE(rowKindToOp(row.getRowKind()));

        // optional __rowId
        if (includeRowId) {
            outBuf.putLongBE(rowId);
        }

        // null bitmap placeholder (filled later)
        final int nullBitmapPos = outBuf.position();
        outBuf.ensureCapacity(nullBytes);
        for (int i = 0; i < nullBytes; i++) {
            outBuf.putByte((byte) 0);
        }

        // values
        for (int i = 0; i < nFields; i++) {
            final int sourceIndex = payloadFieldIndices[i];
            if (row.isNullAt(sourceIndex)) {
                setNullBit(outBuf.buf(), nullBitmapPos, i);
                continue;
            }
            writeValue(i, row, sourceIndex);
        }

        final int payloadLen = outBuf.position();
        if (payloadLen > maxFrameSize) {
            throw new IOException("Row payload exceeds maxFrameSize: " + payloadLen + " > " + maxFrameSize);
        }

        // frame: [len][payload]
        writeIntBE(out, payloadLen);
        out.write(outBuf.buf(), 0, payloadLen);
    }

    private void writeValue(int fieldPos, RowData row, int sourceIndex) throws IOException {
        final WireType wt = writeWireTypes[fieldPos];
        final LogicalType targetType = writeTargetTypes[fieldPos];
        final LogicalTypeRoot sourceRoot = writeSourceRoots[fieldPos];

        switch (wt) {
            case BOOL:
                outBuf.putByte((byte) (row.getBoolean(sourceIndex) ? 1 : 0));
                return;
            case INT32:
                // Flink may store smaller ints; treat them as int32 on wire
                switch (sourceRoot) {
                    case TINYINT:
                        outBuf.putIntBE(row.getByte(sourceIndex));
                        return;
                    case SMALLINT:
                        outBuf.putIntBE(row.getShort(sourceIndex));
                        return;
                    default:
                        outBuf.putIntBE(row.getInt(sourceIndex));
                        return;
                }
            case INT64:
                outBuf.putLongBE(row.getLong(sourceIndex));
                return;
            case FLOAT32:
                outBuf.putIntBE(Float.floatToIntBits(row.getFloat(sourceIndex)));
                return;
            case FLOAT64:
                outBuf.putLongBE(Double.doubleToLongBits(row.getDouble(sourceIndex)));
                return;
            case STRING: {
                final StringData sd = row.getString(sourceIndex);
                final byte[] bytes = sd.toBytes();
                outBuf.putIntBE(bytes.length);
                outBuf.putBytes(bytes);
                return;
            }
            case BYTES: {
                final byte[] bytes = row.getBinary(sourceIndex);
                outBuf.putIntBE(bytes.length);
                outBuf.putBytes(bytes);
                return;
            }
            case TIMESTAMP_MILLIS: {
                final int precision = writeTimestampPrecision[fieldPos];
                final long ms = row.getTimestamp(sourceIndex, precision).getMillisecond();
                outBuf.putLongBE(ms);
                return;
            }
            case DECIMAL_UNSCALED_I64: {
                final DecimalType dt = (DecimalType) targetType;
                final int srcPrecision = writeSourcePrecision[fieldPos];
                final int srcScale = writeSourceScale[fieldPos];
                final long unscaled = toUnscaledLong(row, sourceIndex, sourceRoot, dt.getPrecision(), dt.getScale(),
                        srcPrecision, srcScale);
                outBuf.putLongBE(unscaled);
                return;
            }
            case DECIMAL_UNSCALED_BYTES: {
                final DecimalType dt = (DecimalType) targetType;
                final int srcPrecision = writeSourcePrecision[fieldPos];
                final int srcScale = writeSourceScale[fieldPos];
                if (sourceRoot == LogicalTypeRoot.DECIMAL && srcScale == dt.getScale()) {
                    final DecimalData dec = row.getDecimal(sourceIndex, srcPrecision, srcScale);
                    final byte[] bytes = dec.toUnscaledBytes();
                    outBuf.putIntBE(bytes.length);
                    outBuf.putBytes(bytes);
                } else {
                    final BigInteger unscaled = toUnscaledBigInt(row, sourceIndex, sourceRoot, dt.getPrecision(), dt.getScale(),
                            srcPrecision, srcScale);
                    final byte[] bytes = unscaled.toByteArray(); // two's complement big-endian
                    outBuf.putIntBE(bytes.length);
                    outBuf.putBytes(bytes);
                }
                return;
            }
            default:
                throw new IOException("Unsupported wire type: " + wt);
        }
    }

    // ---------------------------------------------------------------------
    // POST: read one framed row -> fields (with casts to target types)
    // ---------------------------------------------------------------------

    RowWithId readFramedRow(InputStream in, RowKind fallbackKind) throws IOException {
        if (readWireTypes == null) {
            throw new IOException("ProxyBinaryCodec not configured for reading");
        }

        final int frameLen = readIntBE(in);
        if (frameLen < 0 || frameLen > maxFrameSize) {
            throw new IOException("Invalid frame length: " + frameLen);
        }

        ensureReadBuf(frameLen);
        readFully(in, frameReadBuf, 0, frameLen);

        int p = 0;

        final int op = readIntBE(frameReadBuf, p);
        p += 4;

        final long rowId;
        if (includeRowId) {
            rowId = readLongBE(frameReadBuf, p);
            p += 8;
        } else {
            rowId = -1L;
        }

        final int nFields = readWireTypes.length;
        final int nullBytes = (nFields + 7) >>> 3;

        if (p + nullBytes > frameLen) {
            throw new IOException("Truncated payload: missing nullBitmap");
        }

        final int nullBitmapPos = p;
        p += nullBytes;

        final Object[] outFields = readFieldBuffer;
        for (int i = 0; i < nFields; i++) {
            if (isNullBitSet(frameReadBuf, nullBitmapPos, i)) {
                outFields[i] = null;
                continue;
            }
            p = readValueInto(i, frameReadBuf, p, frameLen, outFields);
        }

        final RowKind kind = opToRowKind(op, fallbackKind);
        return new RowWithId(rowId, kind, outFields);
    }

    private int readValueInto(int i, byte[] buf, int p, int limit, Object[] outFields) throws IOException {
        final WireType wt = readWireTypes[i];

        switch (wt) {
            case BOOL:
                if (p + 1 > limit) throw new IOException("Truncated BOOL");
                outFields[i] = castIfNeeded(buf[p] != 0, readSourceTypes[i], readTargetTypes[i]);
                return p + 1;
            case INT32:
                if (p + 4 > limit) throw new IOException("Truncated INT32");
                outFields[i] = castIfNeeded(readIntBE(buf, p), readSourceTypes[i], readTargetTypes[i]);
                return p + 4;
            case INT64:
            case TIMESTAMP_MILLIS:
                if (p + 8 > limit) throw new IOException("Truncated INT64");
                outFields[i] = castIfNeeded(readLongBE(buf, p), readSourceTypes[i], readTargetTypes[i]);
                return p + 8;
            case FLOAT32:
                if (p + 4 > limit) throw new IOException("Truncated FLOAT32");
                outFields[i] = castIfNeeded(Float.intBitsToFloat(readIntBE(buf, p)), readSourceTypes[i], readTargetTypes[i]);
                return p + 4;
            case FLOAT64:
                if (p + 8 > limit) throw new IOException("Truncated FLOAT64");
                outFields[i] = castIfNeeded(Double.longBitsToDouble(readLongBE(buf, p)), readSourceTypes[i], readTargetTypes[i]);
                return p + 8;
            case STRING: {
                if (p + 4 > limit) throw new IOException("Truncated STRING len");
                final int len = readIntBE(buf, p);
                p += 4;
                if (len < 0 || p + len > limit) throw new IOException("Invalid STRING len: " + len);
                outFields[i] = castIfNeeded(StringData.fromBytes(buf, p, len), readSourceTypes[i], readTargetTypes[i]);
                return p + len;
            }
            case BYTES: {
                if (p + 4 > limit) throw new IOException("Truncated BYTES len");
                final int len = readIntBE(buf, p);
                p += 4;
                if (len < 0 || p + len > limit) throw new IOException("Invalid BYTES len: " + len);
                final byte[] out = new byte[len];
                System.arraycopy(buf, p, out, 0, len);
                outFields[i] = castIfNeeded(out, readSourceTypes[i], readTargetTypes[i]);
                return p + len;
            }
            case DECIMAL_UNSCALED_I64: {
                if (p + 8 > limit) throw new IOException("Truncated DECIMAL_UNSCALED_I64");
                final long unscaled = readLongBE(buf, p);
                outFields[i] = castIfNeeded(unscaled, readSourceTypes[i], readTargetTypes[i]);
                return p + 8;
            }
            case DECIMAL_UNSCALED_BYTES: {
                if (p + 4 > limit) throw new IOException("Truncated DECIMAL_UNSCALED_BYTES len");
                final int len = readIntBE(buf, p);
                p += 4;
                if (len < 0 || p + len > limit) throw new IOException("Invalid DECIMAL bytes len: " + len);
                final byte[] bi = new byte[len];
                System.arraycopy(buf, p, bi, 0, len);
                outFields[i] = castIfNeeded(bi, readSourceTypes[i], readTargetTypes[i]);
                return p + len;
            }
            default:
                throw new IOException("Unsupported read wire type: " + wt);
        }
    }

    private static Object castIfNeeded(Object value, LogicalType sourceType, LogicalType targetType) {
        if (value == null || targetType == null) {
            return value;
        }
        if (sourceType == null) {
            // if no source, interpret based on target (mostly for decimal/timestamp)
            return materializeFromWire(value, targetType);
        }

        final LogicalTypeRoot sr = sourceType.getTypeRoot();
        final LogicalTypeRoot tr = targetType.getTypeRoot();

        // same family => just normalize decimals if needed
        if (sr == tr
                || (isStringRoot(sr) && isStringRoot(tr))
                || (isTimestampRoot(sr) && isTimestampRoot(tr))) {

            if (tr == LogicalTypeRoot.DECIMAL) {
                final DecimalType dt = (DecimalType) targetType;
                final DecimalData d = (DecimalData) materializeFromWire(value, targetType);
                return DecimalDataUtils.castFrom(d, dt.getPrecision(), dt.getScale());
            }
            return materializeFromWire(value, targetType);
        }

        // general cast
        return ProxyOperator.castValue(materializeFromWire(value, sourceType), sourceType, targetType);
    }

    private static Object materializeFromWire(Object value, LogicalType type) {
        final LogicalTypeRoot root = type.getTypeRoot();

        switch (root) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // value is long epoch millis
                return TimestampData.fromEpochMillis((Long) value);
            case DECIMAL: {
                final DecimalType dt = (DecimalType) type;
                if (value instanceof DecimalData) {
                    return value;
                }
                if (value instanceof Long) {
                    return DecimalData.fromUnscaledLong((Long) value, dt.getPrecision(), dt.getScale());
                }
                if (value instanceof byte[]) {
                    final BigInteger bi = new BigInteger((byte[]) value);
                    final BigDecimal bd = new BigDecimal(bi, dt.getScale());
                    final DecimalData dd = DecimalData.fromBigDecimal(bd, dt.getPrecision(), dt.getScale());
                    if (dd == null) {
                        throw new TableException("Failed to deserialize DECIMAL(" + dt.getPrecision() + "," + dt.getScale() + ")");
                    }
                    return dd;
                }
                if (value instanceof StringData) {
                    return DecimalDataUtils.castFrom(value.toString(), dt.getPrecision(), dt.getScale());
                }
                break;
            }
            default:
                break;
        }
        return value;
    }

    // ---------------------------------------------------------------------
    // Helpers: mapping + numeric conversions
    // ---------------------------------------------------------------------

    static WireType wireTypeFor(LogicalType type) {
        if (type == null) {
            return WireType.INT32; // unreachable; caller should not use null
        }
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return WireType.BOOL;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return WireType.INT32;
            case BIGINT:
                return WireType.INT64;
            case FLOAT:
                return WireType.FLOAT32;
            case DOUBLE:
                return WireType.FLOAT64;
            case CHAR:
            case VARCHAR:
                return WireType.STRING;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return WireType.TIMESTAMP_MILLIS;
            case DECIMAL: {
                final DecimalType dt = (DecimalType) type;
                return dt.getPrecision() <= 18 ? WireType.DECIMAL_UNSCALED_I64 : WireType.DECIMAL_UNSCALED_BYTES;
            }
            case BINARY:
            case VARBINARY:
                return WireType.BYTES;
            default:
                throw new TableException("Unsupported logical type for proxy wire format: " + type.asSerializableString());
        }
    }

    static boolean isStringRoot(LogicalTypeRoot root) {
        return root == LogicalTypeRoot.CHAR || root == LogicalTypeRoot.VARCHAR;
    }

    static boolean isTimestampRoot(LogicalTypeRoot root) {
        return root == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
                || root == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
    }

    static int rowKindToOp(RowKind kind) {
        switch (kind) {
            case INSERT: return 0;
            case UPDATE_AFTER: return 1;
            case UPDATE_BEFORE: return 2;
            case DELETE: return 3;
            default: return 127;
        }
    }

    static RowKind opToRowKind(int op, RowKind fallback) {
        switch (op) {
            case 0: return RowKind.INSERT;
            case 1: return RowKind.UPDATE_AFTER;
            case 2: return RowKind.UPDATE_BEFORE;
            case 3: return RowKind.DELETE;
            default: return fallback;
        }
    }

    private long toUnscaledLong(
            RowData row,
            int pos,
            LogicalTypeRoot sourceRoot,
            int targetPrecision,
            int targetScale,
            int sourcePrecision,
            int sourceScale) {

        // fast path: source is DECIMAL already
        if (sourceRoot == LogicalTypeRoot.DECIMAL) {
            final DecimalData dec = row.getDecimal(pos, sourcePrecision, sourceScale);
            if (sourceScale == targetScale) {
                return dec.toUnscaledLong();
            }
            try {
                final long unscaled = dec.toUnscaledLong();
                return rescaleLong(unscaled, targetScale - sourceScale);
            } catch (ArithmeticException e) {
                final BigDecimal bd = dec.toBigDecimal();
                final BigDecimal scaled =
                        bd.scale() == targetScale
                                ? bd
                                : bd.setScale(targetScale, RoundingMode.HALF_UP);
                return scaled.unscaledValue().longValueExact();
            }
        }

        // numeric -> decimal
        switch (sourceRoot) {
            case BIGINT:
                return rescaleLongExact(row.getLong(pos), targetScale);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return rescaleLongExact(row.getInt(pos), targetScale);
            case SMALLINT:
                return rescaleLongExact(row.getShort(pos), targetScale);
            case TINYINT:
                return rescaleLongExact(row.getByte(pos), targetScale);
            case FLOAT:
                return bigDecimalToUnscaledLong(BigDecimal.valueOf(row.getFloat(pos)), targetScale);
            case DOUBLE:
                return bigDecimalToUnscaledLong(BigDecimal.valueOf(row.getDouble(pos)), targetScale);
            default:
                throw new TableException("Cannot cast " + sourceRoot + " to DECIMAL(" + targetPrecision + "," + targetScale + ")");
        }
    }

    private BigInteger toUnscaledBigInt(
            RowData row,
            int pos,
            LogicalTypeRoot sourceRoot,
            int targetPrecision,
            int targetScale,
            int sourcePrecision,
            int sourceScale) {

        if (sourceRoot == LogicalTypeRoot.DECIMAL) {
            final DecimalData dec = row.getDecimal(pos, sourcePrecision, sourceScale);
            if (sourceScale == targetScale) {
                return new BigInteger(dec.toUnscaledBytes());
            }
            final BigInteger unscaled = new BigInteger(dec.toUnscaledBytes());
            return rescaleBigInt(unscaled, targetScale - sourceScale);
        }

        switch (sourceRoot) {
            case BIGINT:
                return rescaleBigInt(BigInteger.valueOf(row.getLong(pos)), targetScale);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return rescaleBigInt(BigInteger.valueOf(row.getInt(pos)), targetScale);
            case SMALLINT:
                return rescaleBigInt(BigInteger.valueOf(row.getShort(pos)), targetScale);
            case TINYINT:
                return rescaleBigInt(BigInteger.valueOf(row.getByte(pos)), targetScale);
            case FLOAT:
                return bigDecimalToUnscaledBigInt(BigDecimal.valueOf(row.getFloat(pos)), targetScale);
            case DOUBLE:
                return bigDecimalToUnscaledBigInt(BigDecimal.valueOf(row.getDouble(pos)), targetScale);
            default:
                throw new TableException("Cannot cast " + sourceRoot + " to DECIMAL(" + targetPrecision + "," + targetScale + ")");
        }
    }

    // ---------------------------------------------------------------------
    // Framing + bitmap + endian helpers
    // ---------------------------------------------------------------------

    static final class RowWithId {
        final long rowId;
        final RowKind kind;
        final Object[] fields;

        RowWithId(long rowId, RowKind kind, Object[] fields) {
            this.rowId = rowId;
            this.kind = kind;
            this.fields = fields;
        }
    }

    private void ensureReadBuf(int len) {
        if (frameReadBuf.length >= len) return;
        int n = frameReadBuf.length;
        while (n < len) n <<= 1;
        frameReadBuf = new byte[n];
    }

    private static void setNullBit(byte[] bitmapHolder, int bitmapPos, int fieldIndex) {
        final int byteIndex = bitmapPos + (fieldIndex >>> 3);
        final int bit = fieldIndex & 7;
        bitmapHolder[byteIndex] |= (byte) (1 << bit);
    }

    private static boolean isNullBitSet(byte[] payload, int bitmapPos, int fieldIndex) {
        final int byteIndex = bitmapPos + (fieldIndex >>> 3);
        final int bit = fieldIndex & 7;
        return (payload[byteIndex] & (1 << bit)) != 0;
    }

    private static void readFully(InputStream in, byte[] b, int off, int len) throws IOException {
        int n = 0;
        while (n < len) {
            final int r = in.read(b, off + n, len - n);
            if (r < 0) {
                throw new EOFException("Truncated frame");
            }
            n += r;
        }
    }

    private static int readIntBE(InputStream in) throws IOException {
        final int b1 = in.read();
        final int b2 = in.read();
        final int b3 = in.read();
        final int b4 = in.read();
        if ((b1 | b2 | b3 | b4) < 0) {
            throw new EOFException("EOF while reading int32");
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
                | ((long) (buf[p + 7] & 0xff));
    }

    private static void writeIntBE(OutputStream out, int v) throws IOException {
        out.write((v >>> 24) & 0xff);
        out.write((v >>> 16) & 0xff);
        out.write((v >>> 8) & 0xff);
        out.write(v & 0xff);
    }

    private static long[] initPow10() {
        final long[] pow = new long[19];
        pow[0] = 1L;
        for (int i = 1; i < pow.length; i++) {
            pow[i] = pow[i - 1] * 10L;
        }
        return pow;
    }

    private static long rescaleLongExact(long value, int targetScale) {
        if (targetScale == 0) {
            return value;
        }
        if (targetScale > 0 && targetScale < POW10.length) {
            return Math.multiplyExact(value, POW10[targetScale]);
        }
        return bigDecimalToUnscaledLong(BigDecimal.valueOf(value), targetScale);
    }

    private static long rescaleLong(long unscaled, int scaleDiff) {
        if (scaleDiff == 0) {
            return unscaled;
        }
        if (scaleDiff > 0) {
            if (scaleDiff >= POW10.length) {
                throw new ArithmeticException("scaleDiff too large for long: " + scaleDiff);
            }
            return Math.multiplyExact(unscaled, POW10[scaleDiff]);
        }
        final int diff = -scaleDiff;
        if (diff >= POW10.length) {
            throw new ArithmeticException("scaleDiff too large for long: " + scaleDiff);
        }
        final long divisor = POW10[diff];
        final long abs = Math.abs(unscaled);
        long quotient = abs / divisor;
        final long remainder = abs - quotient * divisor;
        final long half = divisor >>> 1;
        final boolean increment =
                remainder > half || (remainder == half && (divisor & 1L) == 0);
        if (increment) {
            quotient++;
        }
        return unscaled < 0 ? -quotient : quotient;
    }

    private static BigInteger rescaleBigInt(BigInteger unscaled, int scaleDiff) {
        if (scaleDiff == 0) {
            return unscaled;
        }
        if (scaleDiff > 0) {
            return unscaled.multiply(BigInteger.TEN.pow(scaleDiff));
        }
        final int diff = -scaleDiff;
        final BigInteger divisor = BigInteger.TEN.pow(diff);
        final BigInteger[] qr = unscaled.divideAndRemainder(divisor);
        if (qr[1].signum() == 0) {
            return qr[0];
        }
        final BigInteger twiceRem = qr[1].abs().shiftLeft(1);
        if (twiceRem.compareTo(divisor) >= 0) {
            return qr[0].add(BigInteger.valueOf(unscaled.signum()));
        }
        return qr[0];
    }

    private static long bigDecimalToUnscaledLong(BigDecimal bd, int scale) {
        final BigDecimal scaled = bd.scale() == scale ? bd : bd.setScale(scale, RoundingMode.HALF_UP);
        return scaled.unscaledValue().longValueExact();
    }

    private static BigInteger bigDecimalToUnscaledBigInt(BigDecimal bd, int scale) {
        final BigDecimal scaled = bd.scale() == scale ? bd : bd.setScale(scale, RoundingMode.HALF_UP);
        return scaled.unscaledValue();
    }

    // ---------------------------------------------------------------------
    // Small reusable output buffer
    // ---------------------------------------------------------------------

    private static final class GrowableBuffer {
        private byte[] buf;
        private int pos;

        GrowableBuffer(int initial) {
            this.buf = new byte[Math.max(256, initial)];
            this.pos = 0;
        }

        void reset() {
            pos = 0;
        }

        int position() {
            return pos;
        }

        byte[] buf() {
            return buf;
        }

        void ensureCapacity(int additional) {
            final int need = pos + additional;
            if (need <= buf.length) return;
            int n = buf.length;
            while (n < need) n <<= 1;
            final byte[] nb = new byte[n];
            System.arraycopy(buf, 0, nb, 0, pos);
            buf = nb;
        }

        void putByte(byte v) {
            ensureCapacity(1);
            buf[pos++] = v;
        }

        void putBytes(byte[] bytes) {
            ensureCapacity(bytes.length);
            System.arraycopy(bytes, 0, buf, pos, bytes.length);
            pos += bytes.length;
        }

        void putIntBE(int v) {
            ensureCapacity(4);
            buf[pos++] = (byte) (v >>> 24);
            buf[pos++] = (byte) (v >>> 16);
            buf[pos++] = (byte) (v >>> 8);
            buf[pos++] = (byte) (v);
        }

        void putLongBE(long v) {
            ensureCapacity(8);
            buf[pos++] = (byte) (v >>> 56);
            buf[pos++] = (byte) (v >>> 48);
            buf[pos++] = (byte) (v >>> 40);
            buf[pos++] = (byte) (v >>> 32);
            buf[pos++] = (byte) (v >>> 24);
            buf[pos++] = (byte) (v >>> 16);
            buf[pos++] = (byte) (v >>> 8);
            buf[pos++] = (byte) (v);
        }
    }
}
