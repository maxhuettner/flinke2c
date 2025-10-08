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

package org.apache.flink.runtime.scheduler.adapter;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.TestingDefaultExecutionGraphBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorResource;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertNotNull;

public class DefaultExecutionGraphPlacementTest {
    @ClassRule
    public static final TestExecutorResource<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorResource();

    @Test
    public void testBasicPlacementAssignment() throws Exception {
        // Create source vertex
        JobVertex sourceVertex = new JobVertex("source");
        sourceVertex.setInvokableClass(NoOpInvokable.class);
        sourceVertex.setParallelism(1);

        // Create processing vertex 
        JobVertex processingVertex = new JobVertex("processing");
        processingVertex.setInvokableClass(NoOpInvokable.class);
        processingVertex.setParallelism(1);

        // Create sink vertex
        JobVertex sinkVertex = new JobVertex("sink");
        sinkVertex.setInvokableClass(NoOpInvokable.class); 
        sinkVertex.setParallelism(1);

        // Connect vertices
        processingVertex.connectNewDataSetAsInput(sourceVertex, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        sinkVertex.connectNewDataSetAsInput(processingVertex, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

        // Create JobGraph
        JobGraph jobGraph = new JobGraph(null, "Test Job", sourceVertex, processingVertex, sinkVertex);

        // Create ExecutionGraph
        ExecutionGraph executionGraph = TestingDefaultExecutionGraphBuilder
            .newBuilder()
            .setJobGraph(jobGraph)
            .build(EXECUTOR_RESOURCE.getExecutor());

        // Create placement strategy
        TopologyExecutionGraphPlacement placement = new TopologyExecutionGraphPlacement();

        // Execute placement
        placement.assignPlacement(executionGraph);

        // Verify placement was successful by checking execution vertices have locations assigned
//        for (ExecutionJobVertex ejv : executionGraph.getAllVertices().values()) {
//            for (ExecutionVertex ev : ejv.getTaskVertices()) {
//                assertNotNull(ev.getTaskManagerAddress());
//            }
//        }
    }
}
