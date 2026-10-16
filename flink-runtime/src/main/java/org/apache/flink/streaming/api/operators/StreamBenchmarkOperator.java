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
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Sink-side operator that measures per-event end-to-end latency (from an embedded source
 * timestamp, expected as the last comma-separated field of each event) and per-second throughput,
 * appending both to {@code eval.csv} / {@code throughput.csv} in the task's working directory.
 * Mirrors the logging format used by the original CEPless eval scripts so results are comparable.
 */
@Internal
public class StreamBenchmarkOperator<IN> extends AbstractUdfStreamOperator<IN, FilterFunction<IN>> implements OneInputStreamOperator<IN, IN> {

	private static final long serialVersionUID = 1L;

	private final int eventRate;
	private int k = 0;
	private transient Timer scheduler;

	public StreamBenchmarkOperator(int eventRate) {
		super(new FilterFunction<IN>() {
			@Override
			public boolean filter(IN value) throws Exception {
				return false;
			}
		});
		this.eventRate = eventRate;
	}

	@Override
	public void open() throws Exception {
		super.open();
		k = 0;
		scheduler = new Timer();
		scheduler.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				writeThroughputToCSV(k);
				k = 0;
			}
		}, 0, 1000);
	}

	@Override
	public void close() throws Exception {
		if (scheduler != null) {
			scheduler.cancel();
		}
		super.close();
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		k++;
		String value = "" + element.getValue();
		String[] values = value.split(",");
		long timestamp = Long.parseLong(values[values.length - 1]);
		output.collect(element);
		writeToCSV(System.currentTimeMillis() - timestamp);
	}

	private void writeToCSV(long latencyMillis) {
		OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();
		double load = bean.getSystemLoadAverage();
		try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter("eval.csv", true)))) {
			writer.write("Flink \t0\t0\t" + latencyMillis + "\t" + load + "\t0\t0\t0\t" + this.eventRate + "\n");
		} catch (IOException e) {
			LOG.warn("Could not write to eval.csv", e);
		}
	}

	private void writeThroughputToCSV(int throughput) {
		try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter("throughput.csv", true)))) {
			writer.write("Flink \t" + throughput + "\t" + eventRate + "\n");
		} catch (IOException e) {
			LOG.warn("Could not write to throughput.csv", e);
		}
	}
}
