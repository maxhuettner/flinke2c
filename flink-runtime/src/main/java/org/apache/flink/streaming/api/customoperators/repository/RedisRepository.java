package org.apache.flink.streaming.api.customoperators.repository;

import io.lettuce.core.RedisClient;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.flink.streaming.api.customoperators.OperatorEventReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/** List-based (RPUSH/LRANGE/LTRIM) event repository backed by Redis, batching sends and polling with backoff. */
public class RedisRepository implements EventRepository {

	static {
		// Disable Lettuce JFR integration to avoid reflective constructor issues on non-JFR JDKs
		System.setProperty("io.lettuce.core.jfr", "false");
	}

	private static final Logger LOG = LoggerFactory.getLogger(RedisRepository.class);

	transient private OperatorEventReceiver eventManager;

	transient private RedisClient redisReceiverClient;
	transient private RedisCommands<String, String> redisReceiverCommands;

	transient private RedisClient redisSenderClient;
	transient private RedisAsyncCommands<String, String> redisSenderCommands;

	transient private List<String> buffer;

	transient int outBatchSize = 100;
	transient int inBatchSize = 100;
	transient int backoffinc = 1;
	transient String addr;
	transient int flushInterval;

	transient Timer scheduler0;

	public RedisRepository(OperatorEventReceiver eventHandler, String host, int port) {
		this.eventManager = eventHandler;

		this.redisReceiverClient = RedisClient.create("redis://" + host);
		StatefulRedisConnection<String, String> redisReceiverConnection = this.redisReceiverClient.connect();
		this.redisReceiverCommands = redisReceiverConnection.sync();

		this.redisSenderClient = RedisClient.create("redis://" + host);
		StatefulRedisConnection<String, String> redisConnection = this.redisSenderClient.connect();
		this.redisSenderCommands = redisConnection.async();
		this.redisSenderCommands.setAutoFlushCommands(false);

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			try {
				redisReceiverClient.shutdown();
			} catch (Exception ignored) { }
			try {
				redisSenderClient.shutdown();
			} catch (Exception ignored) { }
		}));

		this.buffer = Collections.synchronizedList(new ArrayList<>());

		outBatchSize = Integer.parseInt(System.getenv("OUT_BATCH_SIZE"));
		inBatchSize = Integer.parseInt(System.getenv("IN_BATCH_SIZE"));
		flushInterval = Integer.parseInt(System.getenv("FLUSH_INTERVAL"));
		backoffinc = Integer.parseInt(System.getenv("BACK_OFF"));
		scheduler0 = new Timer();
		scheduler0.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				List<String> internalBuffer = new ArrayList<>(buffer);
				buffer.clear();
				int size = internalBuffer.size();
				int batches = 0;
				List<String> batch = new ArrayList<>();
				for (int i = 0; i < size; i++) {
					if (batch.size() > outBatchSize) {
						batches++;
						redisSenderCommands.rpush(addr, batch.toArray(new String[batch.size()]));
						batch.clear();
					}
					batch.add(internalBuffer.get(i));
				}
				if (batch.size() > 0) {
					batches++;
					redisSenderCommands.rpush(addr, batch.toArray(new String[batch.size()]));
				}
				LOG.debug("Starting flush with {} batches", batches);
				redisSenderCommands.flushCommands();
			}
		}, 0, flushInterval);
	}

	@Override
	public void listen(String addr) {
		Thread t = new Thread() {
			transient int backoff = 0;
			public void run() {
				LOG.info("Receive thread started");
				boolean lastListEmpty = false;
				while (true) {
					try {
						if (lastListEmpty) {
							backoff = backoff + backoffinc;
							TimeUnit.NANOSECONDS.sleep(backoff);
						}

						redisReceiverCommands.multi();
						redisReceiverCommands.lrange(addr, 0, inBatchSize - 1);
						redisReceiverCommands.ltrim(addr, inBatchSize, -1);
						TransactionResult result = redisReceiverCommands.exec();
						List<String> list = result.get(0);

						lastListEmpty = (list.size() == 0);

						if (!lastListEmpty) {
							backoff = 0;
						}

						list.forEach(item -> {
							eventManager.receivedEvent(item);
						});
					} catch (Exception e) {
						LOG.error("Receive thread exception", e);
					}
				}
			}
		};
		t.setDaemon(true);
		t.start();
	}

	@Override
	public void send(String addr, String item) {
		this.addr = addr;
		this.buffer.add(item);
	}
}
