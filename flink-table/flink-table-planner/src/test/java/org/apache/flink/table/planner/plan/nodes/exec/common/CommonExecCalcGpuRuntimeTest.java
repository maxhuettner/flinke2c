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

package org.apache.flink.table.planner.plan.nodes.exec.common;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.runtime.functions.table.gpuruntime.GpuRuntimeFunction;
import org.apache.flink.table.runtime.functions.table.gpuruntime.GpuRuntimeFunctionContext;
import org.apache.flink.table.runtime.functions.table.gpuruntime.GpuRuntimeOperator;

import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces (minus RDMA/JNI specifics) the exact scenario reported against the gpu-runtime
 * feature: {@code CREATE TEMPORARY FUNCTION f AS '<class>'} + {@code table.exec.gpu-runtime.*}
 * SET statements + a plain {@code SELECT f(col)} — to check, with a real Calcite/Flink planning
 * run rather than just reading the matching code, whether {@link CommonExecCalc}'s rewrite
 * actually injects a {@link GpuRuntimeOperator}.
 */
class CommonExecCalcGpuRuntimeTest {

    @Test
    void testGpuRuntimeFunctionClassInjectsGpuRuntimeOperator() {
        final StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        StreamExecutionEnvironment.getExecutionEnvironment(),
                        EnvironmentSettings.newInstance().inStreamingMode().build());

        tEnv.executeSql(
                "CREATE TEMPORARY FUNCTION plusOne AS '" + PlusOneFunction.class.getName() + "'");

        tEnv.getConfig()
                .set("table.exec.gpu-runtime.function-class", PlusOneFunction.class.getName());
        tEnv.getConfig()
                .set(
                        "table.exec.gpu-runtime.conf." + PlusOneFunction.class.getName(),
                        "impl=" + NoopGpuRuntimeFunction.class.getName());

        tEnv.createTemporaryTable(
                "t",
                TableDescriptor.forConnector("values")
                        .schema(Schema.newBuilder().column("i", DataTypes.INT().notNull()).build())
                        .build());

        final Table result = tEnv.sqlQuery("SELECT plusOne(i) AS i FROM t");
        final Transformation<?> transformation = tEnv.toChangelogStream(result).getTransformation();

        assertThat(containsGpuRuntimeOperator(transformation))
                .as("expected a GpuRuntimeOperator somewhere in the transformation graph")
                .isTrue();
    }

    private static boolean containsGpuRuntimeOperator(Transformation<?> root) {
        final Deque<Transformation<?>> stack = new ArrayDeque<>();
        stack.push(root);
        final Set<Transformation<?>> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        while (!stack.isEmpty()) {
            final Transformation<?> t = stack.pop();
            if (!visited.add(t)) {
                continue;
            }
            if (t instanceof OneInputTransformation) {
                final OneInputTransformation<?, ?> oit = (OneInputTransformation<?, ?>) t;
                if (oit.getOperatorFactory() instanceof SimpleOperatorFactory) {
                    final Object op = ((SimpleOperatorFactory<?>) oit.getOperatorFactory()).getOperator();
                    if (op instanceof GpuRuntimeOperator) {
                        return true;
                    }
                }
            }
            stack.addAll(t.getInputs());
        }
        return false;
    }

    /** Plain synchronous scalar function — the CPU baseline the rewrite is supposed to intercept. */
    public static final class PlusOneFunction extends ScalarFunction {
        public int eval(int i) {
            return i + 1;
        }
    }

    /** No-op impl — only its class name (via {@code impl=}) matters for this test; its methods are never invoked during pure translation. */
    public static final class NoopGpuRuntimeFunction implements GpuRuntimeFunction {
        @Override
        public void open(GpuRuntimeFunctionContext context, Emitter emitter) {}

        @Override
        public void processElement(RowData row, boolean hasTimestamp, long timestamp) {}

        @Override
        public void flush() {}

        @Override
        public void close() {}
    }
}
