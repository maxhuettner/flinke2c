package org.apache.flink.streaming.api.customoperators;

/** Callback invoked once the CEPless node manager has deployed the requested operator. */
public interface CustomOperatorDeployed {
	void notifyOperatorDeployed(String requestIdentifier, CustomOperatorAddress address);
}
