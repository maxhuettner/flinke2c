package org.apache.flink.streaming.api.customoperators;

import java.io.Serializable;

/** Address (input/output channel identifiers) of a CEPless operator deployed by the node manager. */
public class CustomOperatorAddress implements Serializable {
	String addrIn;
	String addrOut;

	CustomOperatorAddress(String addrIn, String addrOut) {
		this.addrIn = addrIn;
		this.addrOut = addrOut;
	}
}
