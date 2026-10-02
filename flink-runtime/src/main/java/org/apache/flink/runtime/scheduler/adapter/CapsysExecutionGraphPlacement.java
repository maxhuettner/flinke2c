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
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Simple file-driven placement for CAPSYS.
 *
 * <p>Each line of the scheduler config maps an operator name to a TaskManager address, e.g.
 * {@code Calc[4]; 192.168.1.20}. An entry that includes the {@code [id]} suffix (the ExecNode id
 * that Flink appends to simplified operator names) is matched exactly against a single operator.
 * An entry without the suffix, e.g. {@code Calc; 192.168.1.20}, is treated as a name prefix: it
 * matches any operator whose name starts with that prefix and has not already been claimed by an
 * exact-match entry. When several such prefix entries share the same base name, they are handed
 * out in the order they appear in the file to the matching operators in the order those operators
 * are visited (topological order), so the first "Calc" entry goes to the first unclaimed Calc
 * operator, the second to the next one, and so on. Operators for which no entry is left unmatched
 * keep the default placement.
 */
public class CapsysExecutionGraphPlacement implements ExecutionGraphPlacement {
    private static final Logger LOG = LoggerFactory.getLogger(CapsysExecutionGraphPlacement.class);

    private static final Pattern OPERATOR_ID_SUFFIX = Pattern.compile("^(.*)\\[\\d+\\]$");

    private final String schedulerCfgPath;

    public CapsysExecutionGraphPlacement(String schedulerCfgPath) {
        this.schedulerCfgPath = schedulerCfgPath;
    }

    @Override
    public void assignPlacement(ExecutionGraph executionGraph) {
        final PlacementConfig configuredPlacements = readPlacements(schedulerCfgPath);

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

    /** Holds the parsed scheduler config, split into exact-match and prefix-match entries. */
    static final class PlacementConfig {
        private final Map<String, String> exactMatches;
        private final Map<String, Deque<String>> prefixMatches;

        PlacementConfig(
                Map<String, String> exactMatches, Map<String, Deque<String>> prefixMatches) {
            this.exactMatches = exactMatches;
            this.prefixMatches = prefixMatches;
        }

        @Override
        public String toString() {
            return "PlacementConfig{"
                    + "exactMatches="
                    + exactMatches
                    + ", prefixMatches="
                    + prefixMatches
                    + '}';
        }
    }

    static PlacementConfig readPlacements(String path) {
        if (path == null || path.trim().isEmpty()) {
            throw new IllegalArgumentException("CAPSYS scheduler config path must be provided.");
        }

        final Map<String, String> exactMatches = new LinkedHashMap<>();
        final Map<String, Deque<String>> prefixMatches = new LinkedHashMap<>();
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

                if (OPERATOR_ID_SUFFIX.matcher(operatorName).matches()) {
                    exactMatches.put(operatorName, taskManagerAddress);
                } else {
                    prefixMatches
                            .computeIfAbsent(operatorName, key -> new ArrayDeque<>())
                            .addLast(taskManagerAddress);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to read CAPSYS scheduler config from " + path, e);
        }

        return new PlacementConfig(exactMatches, prefixMatches);
    }

    /**
     * Resolves the TaskManager address for an operator, preferring an exact match (an entry that
     * included the {@code [id]} suffix) and otherwise consuming the next address queued for the
     * operator's name prefix, if any.
     */
    static String findTaskManagerAddress(String operatorName, PlacementConfig configuredPlacements) {
        final String exactMatch = configuredPlacements.exactMatches.get(operatorName);
        if (exactMatch != null) {
            return exactMatch;
        }

        final String baseName = stripOperatorIdSuffix(operatorName);
        final Deque<String> candidates = configuredPlacements.prefixMatches.get(baseName);
        if (candidates != null && !candidates.isEmpty()) {
            return candidates.pollFirst();
        }

        return null;
    }

    private static String stripOperatorIdSuffix(String operatorName) {
        final Matcher matcher = OPERATOR_ID_SUFFIX.matcher(operatorName);
        return matcher.matches() ? matcher.group(1) : operatorName;
    }

    private static int findSeparatorIndex(String line) {
        final int semicolonIndex = line.indexOf(';');
        if (semicolonIndex >= 0) {
            return semicolonIndex;
        }
        return line.indexOf(':');
    }
}
