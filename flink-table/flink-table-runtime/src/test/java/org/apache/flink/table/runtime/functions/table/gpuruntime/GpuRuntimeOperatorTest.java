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

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link GpuRuntimeOperator}, using {@link DoublingGpuRuntimeFunction} as the impl. */
class GpuRuntimeOperatorTest {

    private static final RowType ROW_TYPE =
            RowType.of(new IntType(), new VarCharType(Integer.MAX_VALUE));

    @Test
    void testBatchFlushesOnceFullAndPreservesOrder() throws Exception {
        final OneInputStreamOperatorTestHarness<RowData, RowData> harness =
                createHarness("impl=" + DoublingGpuRuntimeFunction.class.getName() + ";batchsize=2");
        harness.open();

        harness.processElement(insertRecord(1, "a"));
        assertThat(harness.getOutput()).isEmpty();

        harness.processElement(insertRecord(2, "b"));
        assertThat(harness.getOutput()).hasSize(2);

        harness.processElement(insertRecord(3, "c"));
        assertThat(harness.getOutput()).hasSize(2);

        assertThat(extractRows(harness)).containsExactly(row(2, "a"), row(4, "b"));

        harness.close();
    }

    @Test
    void testWatermarkFlushesPartialBatch() throws Exception {
        final OneInputStreamOperatorTestHarness<RowData, RowData> harness =
                createHarness("impl=" + DoublingGpuRuntimeFunction.class.getName() + ";batchsize=64");
        harness.open();

        harness.processElement(insertRecord(5, "x"));
        assertThat(harness.getOutput()).isEmpty();

        harness.processWatermark(new Watermark(100L));

        assertThat(extractRows(harness)).containsExactly(row(10, "x"));

        harness.close();
    }

    @Test
    void testCloseFlushesRemainingRows() throws Exception {
        final OneInputStreamOperatorTestHarness<RowData, RowData> harness =
                createHarness("impl=" + DoublingGpuRuntimeFunction.class.getName() + ";batchsize=64");
        harness.open();

        harness.processElement(insertRecord(7, "y"));
        harness.close();

        assertThat(extractRows(harness)).containsExactly(row(14, "y"));
    }

    @Test
    void testMissingImplKeyFailsFast() throws Exception {
        final OneInputStreamOperatorTestHarness<RowData, RowData> harness =
                createHarness("batchsize=8");

        assertThat(org.assertj.core.api.Assertions.catchThrowable(harness::open))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("impl=");
    }

    private static OneInputStreamOperatorTestHarness<RowData, RowData> createHarness(String conf)
            throws Exception {
        final GpuRuntimeOperator operator = new GpuRuntimeOperator(conf, ROW_TYPE, ROW_TYPE);
        return new OneInputStreamOperatorTestHarness<>(operator);
    }

    private static List<RowData> extractRows(OneInputStreamOperatorTestHarness<RowData, RowData> harness) {
        return new java.util.ArrayList<>(harness.extractOutputValues());
    }
}
