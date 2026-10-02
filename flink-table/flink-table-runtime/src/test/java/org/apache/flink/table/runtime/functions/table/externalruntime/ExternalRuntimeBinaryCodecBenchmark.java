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

package org.apache.flink.table.runtime.functions.table.externalruntime;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;

/**
 * Manual decode-side micro-benchmark for {@link ExternalRuntimeBinaryCodec}, deliberately not a
 * JUnit test: it runs for tens of seconds and its output is meant to be read, not asserted on.
 * Not picked up by surefire (no {@code @Test} methods, and its name doesn't match surefire's
 * default Test-prefix/-suffix or IT-suffix include patterns) - run it directly, e.g.:
 *
 * <pre>{@code
 * mvn -q -pl flink-table/flink-table-runtime -am test-compile
 * java -cp flink-table/flink-table-runtime/target/test-classes:flink-table/flink-table-runtime/target/classes:$(mvn -q -pl flink-table/flink-table-runtime dependency:build-classpath -Dmdep.outputFile=/dev/stdout) \
 *     org.apache.flink.table.runtime.functions.table.externalruntime.ExternalRuntimeBinaryCodecBenchmark
 * }</pre>
 *
 * <p>Mirrors {@code ImputationGpuFunction}'s real decode configuration exactly: 7 NexMark-bid
 * fields, price wired as {@code DECIMAL(18,3)} (the compact-decimal-eligible wire type) but
 * decoded into a {@code DECIMAL(23,3)} result field (the UDF's fixed output contract), which is
 * what forces every row's price field through {@code DecimalDataUtils.castFrom}'s
 * BigDecimal-materializing path - see {@code ExternalRuntimeBinaryCodec#castIfNeeded}.
 *
 * <p>Runs decode four times with different {@code wireIndexToTargetField} maps, using the
 * existing {@link ExternalRuntimeBinaryCodec#readFramedRowInto} skip-field mechanism (a target
 * index of -1 parses past a field without materializing or casting it) to isolate cost by
 * subtraction, entirely with production code paths - no reflection, no core Flink edits:
 *
 * <ul>
 *   <li>all 7 fields (matches production exactly)
 *   <li>all fields except price - the delta from the baseline is price's decode+cast cost
 *   <li>only the 3 STRING fields (channel, url, extra) - isolates allocation-heavy STRING decode
 *   <li>every field skipped - pure frame-walking/dispatch overhead, the floor nothing can beat
 * </ul>
 */
public final class ExternalRuntimeBinaryCodecBenchmark {

    private static final int FIELDS = 7;
    private static final int SLOT_STRIDE = 1024; // generous fixed slot size; largest field is a short URL string

    private static final DecimalType WIRE_PRICE_TYPE = new DecimalType(18, 3);
    private static final DecimalType RESULT_PRICE_TYPE = new DecimalType(23, 3);

    private static final ExternalRuntimeBinaryCodec.WireType[] WIRES = {
        ExternalRuntimeBinaryCodec.WireType.DECIMAL_UNSCALED_I64,
        ExternalRuntimeBinaryCodec.WireType.INT64,
        ExternalRuntimeBinaryCodec.WireType.INT64,
        ExternalRuntimeBinaryCodec.WireType.STRING,
        ExternalRuntimeBinaryCodec.WireType.STRING,
        ExternalRuntimeBinaryCodec.WireType.TIMESTAMP_MILLIS,
        ExternalRuntimeBinaryCodec.WireType.STRING,
    };

    // field order: price, auction, bidder, channel, url, dateTime, extra - matches
    // ImputationGpuFunction.fields = {2, 0, 1, 3, 4, 5, 6} applied to the bids row.
    private static final LogicalType[] READ_SOURCES = {
        WIRE_PRICE_TYPE, new BigIntType(), new BigIntType(),
        new VarCharType(64), new VarCharType(256), new TimestampType(3), new VarCharType(64),
    };
    private static final LogicalType[] READ_TARGETS = {
        RESULT_PRICE_TYPE, new BigIntType(), new BigIntType(),
        new VarCharType(64), new VarCharType(256), new TimestampType(3), new VarCharType(64),
    };

    public static void main(String[] args) throws Exception {
        final int batchSize = 1024;
        final int warmupBatches = args.length > 0 ? Integer.parseInt(args[0]) : 500;
        final int measuredBatches = args.length > 1 ? Integer.parseInt(args[1]) : 4000;

        System.out.printf(
                "warmup=%,d rows, measured=%,d rows per variant%n%n",
                (long) warmupBatches * batchSize, (long) measuredBatches * batchSize);

        final ByteBuffer arena = encodeFixture(batchSize);
        final int[] offsets = new int[batchSize];
        for (int i = 0; i < batchSize; i++) {
            offsets[i] = i * SLOT_STRIDE;
        }

        final ExternalRuntimeBinaryCodec decodeCodec =
                new ExternalRuntimeBinaryCodec(
                        true, null, null, null, null, null, null,
                        WIRES, READ_SOURCES, READ_TARGETS, false);

        run("all 7 fields (matches production)",
                decodeCodec, arena, offsets, new int[] {0, 1, 2, 3, 4, 5, 6},
                batchSize, warmupBatches, measuredBatches);
        run("all except price (0)",
                decodeCodec, arena, offsets, new int[] {-1, 1, 2, 3, 4, 5, 6},
                batchSize, warmupBatches, measuredBatches);
        run("STRING fields only (channel, url, extra)",
                decodeCodec, arena, offsets, new int[] {-1, -1, -1, 3, 4, -1, 6},
                batchSize, warmupBatches, measuredBatches);
        run("nothing (frame walk / dispatch floor)",
                decodeCodec, arena, offsets, new int[] {-1, -1, -1, -1, -1, -1, -1},
                batchSize, warmupBatches, measuredBatches);
    }

    private static void run(
            String label,
            ExternalRuntimeBinaryCodec decodeCodec,
            ByteBuffer arena,
            int[] offsets,
            int[] wireIndexToTargetField,
            int batchSize,
            int warmupBatches,
            int measuredBatches)
            throws Exception {
        for (int b = 0; b < warmupBatches; b++) {
            decodeBatch(decodeCodec, arena, offsets, wireIndexToTargetField, batchSize);
        }
        final long start = System.nanoTime();
        for (int b = 0; b < measuredBatches; b++) {
            decodeBatch(decodeCodec, arena, offsets, wireIndexToTargetField, batchSize);
        }
        final long elapsedNanos = System.nanoTime() - start;
        final long rows = (long) measuredBatches * batchSize;
        final double usPerRow = elapsedNanos / 1000.0 / rows;
        final double usPerBatch = elapsedNanos / 1000.0 / measuredBatches;
        System.out.printf(
                "%-45s %8.4f us/row   %8.1f us/%d-row-batch   %s%n",
                label, usPerRow, usPerBatch, batchSize, Arrays.toString(wireIndexToTargetField));
    }

    private static void decodeBatch(
            ExternalRuntimeBinaryCodec decodeCodec,
            ByteBuffer arena,
            int[] offsets,
            int[] wireIndexToTargetField,
            int batchSize)
            throws Exception {
        for (int i = 0; i < batchSize; i++) {
            // Fresh row per decode, matching ImputationGpuFunction.decode(), which allocates a
            // new GenericRowData per completed row rather than reusing one (reuseObjects=false).
            final GenericRowData out = new GenericRowData(FIELDS);
            decodeCodec.readFramedRowInto(arena, offsets[i], out, wireIndexToTargetField);
        }
    }

    /** Encodes {@code batchSize} distinct, realistic rows once into a direct-buffer arena so encode cost never pollutes decode measurements. */
    private static ByteBuffer encodeFixture(int batchSize) throws Exception {
        final LogicalTypeRoot[] writeRoots = new LogicalTypeRoot[FIELDS];
        final int[] writePrecision = new int[FIELDS];
        final int[] writeScale = new int[FIELDS];
        final int[] writeTsPrecision = new int[FIELDS];
        for (int i = 0; i < FIELDS; i++) {
            writeRoots[i] = READ_SOURCES[i].getTypeRoot();
            if (READ_SOURCES[i] instanceof DecimalType) {
                writePrecision[i] = ((DecimalType) READ_SOURCES[i]).getPrecision();
                writeScale[i] = ((DecimalType) READ_SOURCES[i]).getScale();
            }
            if (READ_SOURCES[i] instanceof TimestampType) {
                writeTsPrecision[i] = ((TimestampType) READ_SOURCES[i]).getPrecision();
            }
        }
        final ExternalRuntimeBinaryCodec encodeCodec =
                new ExternalRuntimeBinaryCodec(
                        true, WIRES, READ_SOURCES, writeRoots, writePrecision, writeScale,
                        writeTsPrecision, null, null, null, false);

        final ByteBuffer arena =
                ByteBuffer.allocateDirect(batchSize * SLOT_STRIDE).order(ByteOrder.nativeOrder());
        final Random rnd = new Random(42);
        final int[] identityFields = {0, 1, 2, 3, 4, 5, 6};
        for (int i = 0; i < batchSize; i++) {
            final GenericRowData row = new GenericRowData(FIELDS);
            final long unscaledPrice = 100_000L + rnd.nextInt(900_000); // 100.000-999.999
            row.setField(0, DecimalData.fromUnscaledLong(unscaledPrice, 18, 3));
            row.setField(1, 1000L + rnd.nextInt(100_000)); // auction
            row.setField(2, 1L + rnd.nextInt(10_000)); // bidder
            row.setField(3, StringData.fromString("channel-" + rnd.nextInt(8)));
            row.setField(4, StringData.fromString("https://example.test/auction/" + rnd.nextInt(1_000_000)));
            row.setField(5, TimestampData.fromEpochMillis(System.currentTimeMillis()));
            row.setField(6, StringData.fromString(rnd.nextBoolean() ? "" : "extra-" + rnd.nextInt(100)));
            encodeCodec.encodeFramedRow(row, identityFields, i, arena, i * SLOT_STRIDE, SLOT_STRIDE);
        }
        return arena;
    }
}
