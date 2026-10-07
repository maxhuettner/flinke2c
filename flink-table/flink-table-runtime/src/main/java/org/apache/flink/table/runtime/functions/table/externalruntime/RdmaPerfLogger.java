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

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Per-stage wall-clock CSV logger for the RDMA PRE/POST operators. Enabled with {@code
 * rdmaperf=true} in the operator conf, writes {@code /tmp/rdma_perf_pre.csv} /
 * {@code /tmp/rdma_perf_post.csv}. Only called from the task thread.
 *
 * <p>PRE and POST use separate files since their stage columns differ and the header is only
 * written once per file.
 */
final class RdmaPerfLogger {
    static final Path PRE_CSV_PATH = Paths.get("/tmp/rdma_perf_pre.csv");
    static final Path POST_CSV_PATH = Paths.get("/tmp/rdma_perf_post.csv");
    private static final long DEFAULT_FLUSH_SECONDS = 30L;

    private final Path csvPath;
    private final String label;
    private final String[] stages;
    private final Map<String, Integer> stageIndex;
    private final AtomicLongArray totalNanos;
    private final AtomicLongArray counts;
    private final AtomicLong totalRows = new AtomicLong();
    private final AtomicLong totalBatches = new AtomicLong();
    private final long flushMillis;
    private final long openNanos = System.nanoTime();
    private volatile boolean running;
    private Thread flusher;

    /** Returns a logger if {@code rdmaperf=true} is set in {@code conf}, else null. */
    static RdmaPerfLogger maybeCreate(String conf, Path csvPath, String label, String... stages) {
        Map<String, String> values = parseConf(conf);
        if (!"true".equalsIgnoreCase(values.get("rdmaperf"))) {
            return null;
        }
        long flushSeconds = longValue(values, "rdmaperfflushseconds", DEFAULT_FLUSH_SECONDS);
        return new RdmaPerfLogger(csvPath, label, flushSeconds, stages);
    }

    private RdmaPerfLogger(Path csvPath, String label, long flushSeconds, String... stages) {
        this.csvPath = csvPath;
        this.label = label;
        this.stages = stages.clone();
        this.stageIndex = new HashMap<>();
        for (int i = 0; i < this.stages.length; i++) {
            stageIndex.put(this.stages[i], i);
        }
        this.totalNanos = new AtomicLongArray(this.stages.length);
        this.counts = new AtomicLongArray(this.stages.length);
        this.flushMillis = 1000L * flushSeconds;
    }

    /** Records one occurrence of {@code stage} taking {@code elapsedNanos}. */
    void record(String stage, long elapsedNanos) {
        int index = stageIndex.get(stage);
        totalNanos.addAndGet(index, elapsedNanos);
        counts.incrementAndGet(index);
    }

    void addRow() {
        totalRows.incrementAndGet();
    }

    void addBatch() {
        totalBatches.incrementAndGet();
    }

    /** Starts the periodic flusher, so a running or killed job still leaves data in the CSV. */
    void start() {
        if (flushMillis <= 0L) {
            return;
        }
        running = true;
        flusher = new Thread(this::runFlusher, "rdma-perf-flush-" + label);
        flusher.setDaemon(true);
        flusher.start();
    }

    private void runFlusher() {
        try {
            while (running) {
                Thread.sleep(flushMillis);
                if (running) {
                    writeCsv("periodic");
                }
            }
        } catch (InterruptedException expected) {
            // close() is stopping this thread; the authoritative final row is written there.
        }
    }

    /** Stops the flusher and writes the authoritative final row. */
    void close() {
        running = false;
        if (flusher != null) {
            flusher.interrupt();
            try {
                flusher.join(5000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            flusher = null;
        }
        writeCsv("final");
    }

    private void writeCsv(String phase) {
        try {
            boolean writeHeader = !Files.exists(csvPath);
            long wallMillis = (System.nanoTime() - openNanos) / 1_000_000L;
            StringBuilder header = new StringBuilder("timestamp,label,wallMillis,rows,batches");
            StringBuilder row = new StringBuilder();
            row.append(Instant.now()).append(',').append(csvSafe(label + "-" + phase)).append(',').append(wallMillis)
                    .append(',').append(totalRows.get()).append(',').append(totalBatches.get());
            for (int i = 0; i < stages.length; i++) {
                long nanos = totalNanos.get(i);
                long count = counts.get(i);
                double totalMillisStage = nanos / 1_000_000.0;
                double avgMicros = count == 0 ? 0.0 : (nanos / 1000.0) / count;
                header.append(',').append(stages[i]).append("TotalMillis,")
                        .append(stages[i]).append("AvgMicros,")
                        .append(stages[i]).append("Count");
                row.append(',').append(String.format("%.3f", totalMillisStage))
                        .append(',').append(String.format("%.3f", avgMicros))
                        .append(',').append(count);
            }
            if (csvPath.getParent() != null) {
                Files.createDirectories(csvPath.getParent());
            }
            try (Writer writer = Files.newBufferedWriter(csvPath, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                if (writeHeader) {
                    writer.write(header.toString());
                    writer.write('\n');
                }
                writer.write(row.toString());
                writer.write('\n');
            }
        } catch (IOException | RuntimeException ignored) {
            // Diagnostics only; never fail the job over a logging problem.
        }
    }

    private static String csvSafe(String value) {
        if (value == null) {
            return "";
        }
        return value.indexOf(',') < 0 && value.indexOf('"') < 0 && value.indexOf('\n') < 0
                ? value
                : '"' + value.replace("\"", "\"\"") + '"';
    }

    private static Map<String, String> parseConf(String conf) {
        Map<String, String> values = new HashMap<>();
        if (conf == null || conf.trim().isEmpty()) {
            return values;
        }
        for (String item : conf.split(";")) {
            int equals = item.indexOf('=');
            if (equals > 0 && equals < item.length() - 1) {
                values.put(
                        item.substring(0, equals).trim().toLowerCase(Locale.ROOT),
                        item.substring(equals + 1).trim());
            }
        }
        return values;
    }

    private static long longValue(Map<String, String> values, String key, long fallback) {
        String value = values.get(key);
        return value == null || value.isEmpty() ? fallback : Long.parseLong(value);
    }
}
