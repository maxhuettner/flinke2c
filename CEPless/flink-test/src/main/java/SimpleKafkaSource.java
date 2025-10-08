import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Minimal Kafka source to bridge the missing FlinkKafkaConsumer in Flink 1.18.
 */
public class SimpleKafkaSource extends RichParallelSourceFunction<String> {
    private static final long serialVersionUID = 1L;

    private final String topic;
    private final Properties baseProperties;

    private transient KafkaConsumer<String, String> consumer;
    private volatile boolean running = true;

    public SimpleKafkaSource(String topic, Properties properties) {
        this.topic = Objects.requireNonNull(topic, "topic");
        this.baseProperties = new Properties();
        if (properties != null) {
            this.baseProperties.putAll(properties);
        }
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        Properties kafkaProps = new Properties();
        kafkaProps.putAll(baseProperties);
        kafkaProps.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProps.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProps.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaProps.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, "cepless-flink-test-" + UUID.randomUUID());

        consumer = new KafkaConsumer<>(kafkaProps);
        consumer.subscribe(Collections.singletonList(topic));

        try {
            final Object checkpointLock = ctx.getCheckpointLock();
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(250));
                if (records.isEmpty()) {
                    continue;
                }
                for (ConsumerRecord<String, String> record : records) {
                    synchronized (checkpointLock) {
                        ctx.collect(record.value());
                    }
                }
            }
        } catch (WakeupException e) {
            if (running) {
                throw e;
            }
        } finally {
            shutdownConsumer();
        }
    }

    @Override
    public void cancel() {
        running = false;
        if (consumer != null) {
            consumer.wakeup();
        }
    }

    private void shutdownConsumer() {
        KafkaConsumer<String, String> localConsumer = this.consumer;
        if (localConsumer == null) {
            return;
        }

        Thread closer = new Thread(() -> {
            try {
                localConsumer.close(Duration.ofSeconds(5));
            } catch (WakeupException ignored) {
                // already stopping, nothing else to do
            } catch (RuntimeException ignored) {
                // ignore close failures caused by cancellation interrupts
            }
        }, "simple-kafka-source-close");

        closer.setDaemon(true);
        closer.start();
        try {
            closer.join(TimeUnit.SECONDS.toMillis(5));
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } finally {
            consumer = null;
        }
    }
}
