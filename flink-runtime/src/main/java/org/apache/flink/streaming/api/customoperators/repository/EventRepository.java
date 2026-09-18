package org.apache.flink.streaming.api.customoperators.repository;

/** Transport used to exchange events with a deployed CEPless user-defined operator. */
public interface EventRepository {
	void listen(String addr);
	void send(String addr, String item);
}
