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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import org.apache.flink.types.RowKind;

import org.example.udf.TcpTableSink;

/**
 * Flink example query using CEPless
 */
public class Main {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************
	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		boolean serverless = params.getBoolean("serverless");
		String tcpSourceHost = params.get("tcp.source.host");
		int tcpSourcePort = params.getInt("tcp.source.port");
		String tcpSinkHost = params.get("tcp.sink.host");
		int tcpSinkPort = params.getInt("tcp.sink.port");

		DataType payloadType = DataTypes.ROW(DataTypes.FIELD("payload", DataTypes.STRING()));
		RowType rowType = (RowType) payloadType.getLogicalType();

		TcpRowInputFormat inputFormat = new TcpRowInputFormat(tcpSourceHost, tcpSourcePort, rowType);

		DataStream<RowData> rowDataStream = env.createInput(
			inputFormat,
			InternalTypeInfo.of(rowType)
		).name("tcp-source");

			DataStream<String> source = rowDataStream
				.map(row -> row.getString(0).toString())
				.returns(Types.STRING);

			/**
			 * Append timestamp (nanoseconds) to event when received from kafka for processing time measurement
			 */
			DataStream<String> query = source.map(new MapFunction<String, String>() {
			@Override
			public String map(String s) throws Exception {
				s = s.replaceAll("\\s+","");
				String c = new StringBuilder(s).toString();
				String d = "," + String.valueOf(System.nanoTime());
				String a = new StringBuilder(c).append(d).toString();
				return a;
			}
		});

			DataStream<String> tap = query;

		DataStream<String> result;
		if (serverless) {
			result = tap.serverless(params.get("customOperator"));
		} else {
			result = tap;
		}

			DataStream<String> resultWithTap = result;

			DataStream<RowData> outputRows = resultWithTap.map(new MapFunction<String, RowData>() {
				@Override
				public RowData map(String value) {
					GenericRowData row = new GenericRowData(RowKind.INSERT, 1);
					row.setField(0, StringData.fromString(value));
					return row;
				}
			}, InternalTypeInfo.of(rowType));

		TcpTableSink.FastBatchPackerSink sink =
			new TcpTableSink.FastBatchPackerSink(tcpSinkHost, tcpSinkPort, rowType);
		outputRows.addSink(sink).name("tcp-sink");

		env.execute("Streaming CEPLess query");
	}
}
