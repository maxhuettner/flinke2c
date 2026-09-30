package org.apache.flink.streaming.api.customoperators;

/** Callback for events coming back from a CEPless user-defined operator. */
public interface OperatorEventReceiver {
	void receivedEvent(String event);
}
