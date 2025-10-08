package org.apache.flink.streaming.api.customoperators.repository;

public interface EventRepository extends AutoCloseable {
	void listen(String addr);
	void send(String addr, String item);

	@Override
	default void close() throws Exception {
		// no-op by default
	}
}
