/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.streaming.api.customoperators.CustomOperatorAddress;
import org.apache.flink.streaming.api.customoperators.CustomOperatorDeployed;
import org.apache.flink.streaming.api.customoperators.UserDefinedOperatorInterface;
import org.apache.flink.streaming.api.customoperators.OperatorEventReceiver;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * A {@link StreamOperator} that offloads processing of each incoming element to an operator
 * deployed and managed by the CEPless node manager, rather than executing it on the Flink task
 * thread. Used as the comparison baseline against FlinkE2C's own external-runtime offloading.
 *
 * <p>Implements {@link YieldingOperator} so the runtime injects a {@link MailboxExecutor}:
 * {@link #receivedEvent(String)} runs on CEPless's Redis receive thread, not the task's own
 * mailbox thread, and {@code collector.collect(...)} is only safe to call from the latter —
 * calling it directly from another thread races with the task thread's own writes to the same
 * output/network buffers and can corrupt Flink's internal inter-task stream framing (observed as
 * {@code Corrupt stream, found tag: ...} deserialization failures, typically right as the job
 * winds down and both threads are touching the output around the same time).
 */
@Internal
public class StreamCEPlessOperator<IN> extends AbstractUdfStreamOperator<IN, FilterFunction<IN>> implements OneInputStreamOperator<IN, IN>, CustomOperatorDeployed, OperatorEventReceiver, YieldingOperator<IN> {

	private static final long serialVersionUID = 1L;
	private final String operatorName;
	private final UserDefinedOperatorInterface operatorInterface;
	private transient TimestampedCollector<IN> collector;
	private transient MailboxExecutor mailboxExecutor;

	private volatile CustomOperatorAddress operatorAddress;

	private static final Logger LOG = LoggerFactory.getLogger(StreamCEPlessOperator.class);

	public StreamCEPlessOperator(String operatorName) {
		super(new FilterFunction<IN>() {
			@Override
			public boolean filter(IN value) throws Exception {
				return true;
			}
		});
		this.operatorName = operatorName;
		this.operatorInterface = new UserDefinedOperatorInterface();
	}

	@Override
	public void setMailboxExecutor(MailboxExecutor mailboxExecutor) {
		this.mailboxExecutor = mailboxExecutor;
	}

	/**
	 * Called when the operator was deployed by the Flink engine and will soon start to receive events.
	 */
	@Override
	public void open() throws Exception {
		super.open();
		collector = new TimestampedCollector<>(output);
		LOG.info("Requesting CEPless operator '{}'", operatorName);
		this.operatorInterface.requestOperator(operatorName, this);
	}

	/**
	 * Called when an event was received by the Flink engine for processing.
	 */
	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		String value = "" + element.getValue();
		if (this.operatorAddress == null) {
			LOG.debug("CEPless operator not ready to receive events yet, dropping event");
			return;
		}
		operatorInterface.sendEvent(value, operatorAddress);
	}

	@Override
	public void notifyOperatorDeployed(String requestIdentifier, CustomOperatorAddress address) {
		LOG.info("CEPless operator deployed, adding listener for events");
		operatorInterface.addListener(address, this);
		this.operatorAddress = address;
	}

	/**
	 * Called when an event that was processed by the CEPless operator was received back, on
	 * CEPless's own Redis receive thread. Defers the actual output to the task's mailbox thread
	 * via {@link #mailboxExecutor}, since {@code collector.collect(...)} is not thread-safe to
	 * call directly from here.
	 */
	@Override
	public void receivedEvent(String event) {
		mailboxExecutor.execute(
				() -> {
					if (collector != null) {
						collector.collect((IN) event);
					} else {
						LOG.warn("Received CEPless event before the collector was initialized, dropping it");
					}
				},
				"StreamCEPlessOperator.receivedEvent");
	}
}
