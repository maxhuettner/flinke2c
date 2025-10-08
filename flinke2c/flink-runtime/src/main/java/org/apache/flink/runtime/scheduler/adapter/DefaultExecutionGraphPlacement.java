package org.apache.flink.runtime.scheduler.adapter;

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.scheduler.strategy.ExecutionGraphPlacement;


/**
 * An empty ExecutionGraphPlacement that does not assign pipeline operators
 */
public class DefaultExecutionGraphPlacement implements ExecutionGraphPlacement {
    public DefaultExecutionGraphPlacement() {
    }

    @Override
    public void assignPlacement(ExecutionGraph executionGraph) {
    }
}
