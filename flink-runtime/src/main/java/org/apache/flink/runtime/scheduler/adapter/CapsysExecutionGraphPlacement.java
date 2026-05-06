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

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.scheduler.strategy.ExecutionGraphPlacement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/** Simple file-driven placement for CAPSYS. */
public class CapsysExecutionGraphPlacement implements ExecutionGraphPlacement {
    private static final Logger LOG = LoggerFactory.getLogger(CapsysExecutionGraphPlacement.class);

    private final String schedulerCfgPath;

    public CapsysExecutionGraphPlacement(String schedulerCfgPath) {
        this.schedulerCfgPath = schedulerCfgPath;
    }

    @Override
    public void assignPlacement(ExecutionGraph executionGraph) {
        final Map<String, String> configuredPlacements = readPlacements(schedulerCfgPath);

        LOG.info("Applying CAPSYS placement from {}: {}", schedulerCfgPath, configuredPlacements);

        for (ExecutionJobVertex executionJobVertex : executionGraph.getVerticesTopologically()) {
            final String operatorName = executionJobVertex.getJobVertex().getName();
            final String taskManagerAddress =
                    findTaskManagerAddress(operatorName, configuredPlacements);

            if (taskManagerAddress == null) {
                LOG.debug("No CAPSYS placement configured for operator '{}'.", operatorName);
                continue;
            }

            executionGraph
                    .getJobVertex(executionJobVertex.getJobVertexId())
                    .getResourceProfile()
                    .setTaskManagerAddress(taskManagerAddress);
            LOG.info(
                    "CAPSYS placement assigned operator '{}' to TaskManager '{}'.",
                    operatorName,
                    taskManagerAddress);
        }
    }

    static Map<String, String> readPlacements(String path) {
        if (path == null || path.trim().isEmpty()) {
            throw new IllegalArgumentException("CAPSYS scheduler config path must be provided.");
        }

        final Map<String, String> placements = new LinkedHashMap<>();
        final Path schedulerCfgPath = Path.of(path);
        try (BufferedReader bufferedReader =
                Files.newBufferedReader(schedulerCfgPath, StandardCharsets.UTF_8)) {
            String line;
            int lineNumber = 0;
            while ((line = bufferedReader.readLine()) != null) {
                lineNumber++;
                final String trimmed = line.trim();
                if (trimmed.isEmpty() || trimmed.startsWith("#") || trimmed.startsWith("//")) {
                    continue;
                }

                final int separatorIndex = findSeparatorIndex(trimmed);
                if (separatorIndex < 0) {
                    throw new IllegalArgumentException(
                            "Invalid CAPSYS placement entry at line "
                                    + lineNumber
                                    + ": expected 'Operator; TaskManager' or 'Operator: TaskManager'.");
                }

                final String operatorName = trimmed.substring(0, separatorIndex).trim();
                final String taskManagerAddress = trimmed.substring(separatorIndex + 1).trim();
                if (operatorName.isEmpty() || taskManagerAddress.isEmpty()) {
                    throw new IllegalArgumentException(
                            "Invalid CAPSYS placement entry at line "
                                    + lineNumber
                                    + ": operator name and TaskManager address must both be set.");
                }

                placements.put(operatorName, taskManagerAddress);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to read CAPSYS scheduler config from " + path, e);
        }

        return placements;
    }

    static String findTaskManagerAddress(
            String operatorName, Map<String, String> configuredPlacements) {
        return configuredPlacements.get(operatorName);
    }

    private static int findSeparatorIndex(String line) {
        final int semicolonIndex = line.indexOf(';');
        if (semicolonIndex >= 0) {
            return semicolonIndex;
        }
        return line.indexOf(':');
    }
}
