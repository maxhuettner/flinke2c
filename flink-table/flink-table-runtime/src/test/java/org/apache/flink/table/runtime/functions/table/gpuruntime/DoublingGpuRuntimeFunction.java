/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.functions.table.gpuruntime;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import java.util.ArrayList;
import java.util.List;

/**
 * Reference {@link GpuRuntimeFunction} implementation, standing in for a real GPU/JNI-backed one.
 *
 * <p>This is what a jar dropped into {@code lib/} to satisfy {@code
 * table.exec.gpu-runtime.conf.<functionClass>}'s {@code impl=...} key looks like: a public class,
 * a public no-arg constructor, and {@code open}/{@code processElement}/{@code flush}/{@code
 * close} doing whatever the accelerator side needs. This one just doubles field 0 of a fixed
 * {@code (INT, STRING)} row (the shape {@link GpuRuntimeOperatorTest} builds its harness with),
 * buffering rows up to {@code batchsize} before emitting, so the test can assert on the
 * operator's batching/flushing behavior without needing an actual GPU; a real implementation
 * would replace {@link #flush}'s body with a JNI call handing the batch to native/CUDA code,
 * exactly like {@code RdmaOperator} hands batches to its native RDMA transport.
 */
public final class DoublingGpuRuntimeFunction implements GpuRuntimeFunction {

    private static final long serialVersionUID = 1L;

    private transient List<BufferedRow> buffer;
    private transient Emitter emitter;
    private transient int batchSize;

    @Override
    public void open(GpuRuntimeFunctionContext context, Emitter emitter) {
        this.emitter = emitter;
        this.batchSize = context.getBatchSize();
        this.buffer = new ArrayList<>(batchSize);
    }

    @Override
    public void processElement(RowData row, boolean hasTimestamp, long timestamp) throws Exception {
        buffer.add(new BufferedRow(row, hasTimestamp, timestamp));
        if (buffer.size() >= batchSize) {
            flush();
        }
    }

    @Override
    public void flush() throws Exception {
        for (BufferedRow bufferedRow : buffer) {
            emitter.collect(doubleField0(bufferedRow.row), bufferedRow.hasTimestamp, bufferedRow.timestamp);
        }
        buffer.clear();
    }

    @Override
    public void close() {}

    private static RowData doubleField0(RowData in) {
        final GenericRowData out = new GenericRowData(2);
        out.setRowKind(in.getRowKind());
        out.setField(0, in.isNullAt(0) ? null : in.getInt(0) * 2);
        out.setField(1, in.isNullAt(1) ? null : in.getString(1));
        return out;
    }

    private static final class BufferedRow {
        private final RowData row;
        private final boolean hasTimestamp;
        private final long timestamp;

        private BufferedRow(RowData row, boolean hasTimestamp, long timestamp) {
            this.row = row;
            this.hasTimestamp = hasTimestamp;
            this.timestamp = timestamp;
        }
    }
}
