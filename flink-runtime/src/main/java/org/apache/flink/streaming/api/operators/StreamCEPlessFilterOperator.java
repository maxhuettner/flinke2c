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
import org.apache.flink.streaming.api.customoperators.OperatorEventReceiver;
import org.apache.flink.streaming.api.customoperators.UserDefinedOperatorInterface;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Offloads a filter predicate to a CEPless-managed operator and forwards the original element only
 * if the operator responds {@code "true"}.
 *
 * <p>Payloads are sent as {@code "<seq>|<value>"} and the operator has to echo {@code "<seq>|true"}
 * / {@code "<seq>|false"}; responses are matched by seq since the Redis transport does not keep
 * order. Results are emitted via the mailbox executor.
 */
@Internal
public class StreamCEPlessFilterOperator<IN> extends AbstractUdfStreamOperator<IN, FilterFunction<IN>> implements OneInputStreamOperator<IN, IN>, CustomOperatorDeployed, OperatorEventReceiver, YieldingOperator<IN> {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(StreamCEPlessFilterOperator.class);

	private final String operatorName;
	private final UserDefinedOperatorInterface operatorInterface;
	private transient TimestampedCollector<IN> collector;
	private transient Map<Long, StreamRecord<IN>> pending;
	private transient AtomicLong nextSequence;
	private transient MailboxExecutor mailboxExecutor;

	private volatile CustomOperatorAddress operatorAddress;

	public StreamCEPlessFilterOperator(String operatorName) {
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

	@Override
	public void open() throws Exception {
		super.open();
		collector = new TimestampedCollector<>(output);
		pending = new ConcurrentHashMap<>();
		nextSequence = new AtomicLong();
		LOG.info("Requesting CEPless filter operator '{}'", operatorName);
		this.operatorInterface.requestOperator(operatorName, this);
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		if (this.operatorAddress == null) {
			LOG.debug("CEPless operator not ready to receive events yet, dropping event");
			return;
		}
		long seq = nextSequence.getAndIncrement();
		pending.put(seq, element);
		operatorInterface.sendEvent(seq + "|" + element.getValue(), operatorAddress);
	}

	@Override
	public void notifyOperatorDeployed(String requestIdentifier, CustomOperatorAddress address) {
		LOG.info("CEPless filter operator deployed, adding listener for events");
		operatorInterface.addListener(address, this);
		this.operatorAddress = address;
	}

	@Override
	public void receivedEvent(String event) {
		int separator = event.indexOf('|');
		if (separator < 0) {
			LOG.warn("Received a CEPless filter result without a sequence prefix, dropping it: {}", event);
			return;
		}
		long seq;
		try {
			seq = Long.parseLong(event.substring(0, separator));
		} catch (NumberFormatException e) {
			LOG.warn("Received a CEPless filter result with an unparseable sequence prefix, dropping it: {}", event);
			return;
		}
		StreamRecord<IN> element = pending.remove(seq);
		if (element == null) {
			LOG.warn("Received a CEPless filter result for unknown/already-consumed sequence {}, dropping it", seq);
			return;
		}
		boolean keep = "true".equals(event.substring(separator + 1));
		mailboxExecutor.execute(
				() -> {
					if (keep) {
						collector.collect(element.getValue());
					}
				},
				"StreamCEPlessFilterOperator.receivedEvent");
	}
}
