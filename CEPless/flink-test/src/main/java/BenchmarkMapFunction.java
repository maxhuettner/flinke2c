import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Records throughput and latency metrics into CSV files without relying on removed Flink Kafka connectors.
 */
public class BenchmarkMapFunction extends RichMapFunction<String, String> {
    private static final long serialVersionUID = 1L;

    private final int recordDelay;
    private final String filename;

    private transient ScheduledExecutorService scheduler;
    private transient String lastMeasurement;
    private transient long lastTimestamp;
    private transient int count;
    private transient int window;
    private transient boolean firstWrite;

    public BenchmarkMapFunction(int recordDelay, String filename) {
        this.recordDelay = recordDelay;
        this.filename = Objects.requireNonNull(filename, "filename");
    }

    @Override
    public void open(Configuration parameters) {
        this.lastMeasurement = null;
        this.lastTimestamp = System.nanoTime();
        this.count = -1;
        this.window = 0;
        this.firstWrite = true;
    }

    @Override
    public String map(String value) throws Exception {
        if (value == null) {
            System.out.println("no value");
            return "";
        }

        ensureScheduler();

        synchronized (this) {
            if (count < 0) {
                count = 1;
            } else {
                count += 1;
            }
            lastMeasurement = value;
            lastTimestamp = System.nanoTime();
        }
        return value;
    }

    private void ensureScheduler() {
        if (scheduler != null) {
            return;
        }

        final ClassLoader userClassLoader = Thread.currentThread().getContextClassLoader();
        ThreadFactory threadFactory = runnable -> {
            Thread thread = new Thread(runnable, "benchmark-map-scheduler");
            thread.setDaemon(true);
            thread.setContextClassLoader(userClassLoader);
            return thread;
        };

        scheduler = Executors.newSingleThreadScheduledExecutor(threadFactory);
        scheduler.scheduleAtFixedRate(this::emitMetricsSafely, recordDelay, 1000, TimeUnit.MILLISECONDS);
    }

    private void emitMetricsSafely() {
        try {
            emitMetrics();
        } catch (Exception e) {
            System.out.println("EXCEPTION!");
            e.printStackTrace();
        }
    }

    private void emitMetrics() throws IOException {
        String measurement;
        long timestamp;
        int throughput;
        int currentWindow;

        synchronized (this) {
            if (lastMeasurement == null) {
                return;
            }
            measurement = lastMeasurement;
            timestamp = lastTimestamp;
            throughput = count;
            count = -1;
            currentWindow = window;
            window++;
        }

        String[] values = measurement.split(",");
        if (values.length < 2) {
            return;
        }

        long endToEndLatency = timestamp - Long.parseLong(values[values.length - 2]);
        long processingLatency = timestamp - Long.parseLong(values[values.length - 1]);

        writeThroughputToCSV(currentWindow, throughput, endToEndLatency, processingLatency);
    }

    private void writeThroughputToCSV(int currentWindow, int throughput, long endToEndLatency, long processingLatency) throws IOException {
        File file = new File(filename);
        boolean needHeader = firstWrite;
        if (!file.exists() || file.length() == 0) {
            needHeader = true;
        }

        if (needHeader) {
            System.out.println("Clearing benchmark data...");
            firstWrite = false;
            try (PrintWriter headerWriter = new PrintWriter(new BufferedWriter(new FileWriter(filename, false)))) {
                headerWriter.write("engine\twindow\tthroughput\tendToEndLatencyNs\tprocessingLatencyNs\n");
            }
        }

        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(filename, true)))) {
            writer.write("Flink\t" + currentWindow + "\t" + throughput + "\t" + endToEndLatency + "\t" + processingLatency + "\n");
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }
}
