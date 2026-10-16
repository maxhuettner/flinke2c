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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CapsysExecutionGraphPlacement}. */
class CapsysExecutionGraphPlacementTest {

    @TempDir private Path tempDir;

    @Test
    void testReadPlacementsSupportsSemicolonAndColon() throws Exception {
        final Path schedulerCfg = tempDir.resolve("schedulercfg");
        Files.writeString(
                schedulerCfg,
                "# comment\n"
                        + "Source; taskmanager-2\n"
                        + "WatermarkAssigner: taskmanager-2\n"
                        + "Calc; taskmanager-3\n"
                        + "Sink; taskmanager-4\n");

        final CapsysExecutionGraphPlacement.PlacementConfig placements =
                CapsysExecutionGraphPlacement.readPlacements(schedulerCfg.toString());

        assertThat(CapsysExecutionGraphPlacement.findTaskManagerAddress("Source", placements))
                .isEqualTo("taskmanager-2");
        assertThat(
                        CapsysExecutionGraphPlacement.findTaskManagerAddress(
                                "WatermarkAssigner", placements))
                .isEqualTo("taskmanager-2");
        assertThat(CapsysExecutionGraphPlacement.findTaskManagerAddress("Calc", placements))
                .isEqualTo("taskmanager-3");
        assertThat(CapsysExecutionGraphPlacement.findTaskManagerAddress("Sink", placements))
                .isEqualTo("taskmanager-4");
    }

    @Test
    void testFindTaskManagerAddressExactMatchWithOperatorId() throws Exception {
        final Path schedulerCfg = tempDir.resolve("schedulercfg");
        Files.writeString(schedulerCfg, "Calc[4]; taskmanager-2\n");
        final CapsysExecutionGraphPlacement.PlacementConfig placements =
                CapsysExecutionGraphPlacement.readPlacements(schedulerCfg.toString());

        assertThat(CapsysExecutionGraphPlacement.findTaskManagerAddress("Calc[4]", placements))
                .isEqualTo("taskmanager-2");
        // A different id on the same operator name must not match.
        assertThat(CapsysExecutionGraphPlacement.findTaskManagerAddress("Calc[5]", placements))
                .isNull();
        assertThat(
                        CapsysExecutionGraphPlacement.findTaskManagerAddress(
                                "Source: File Source", placements))
                .isNull();
    }

    @Test
    void testFindTaskManagerAddressPrefixMatchWithoutOperatorId() throws Exception {
        final Path schedulerCfg = tempDir.resolve("schedulercfg");
        Files.writeString(schedulerCfg, "Calc; taskmanager-3\n");
        final CapsysExecutionGraphPlacement.PlacementConfig placements =
                CapsysExecutionGraphPlacement.readPlacements(schedulerCfg.toString());

        // Only the first Calc encountered consumes the single "Calc" entry.
        assertThat(CapsysExecutionGraphPlacement.findTaskManagerAddress("Calc[3]", placements))
                .isEqualTo("taskmanager-3");
        assertThat(CapsysExecutionGraphPlacement.findTaskManagerAddress("Calc[4]", placements))
                .isNull();
    }

    @Test
    void testFindTaskManagerAddressPrefersExactMatchOverPrefixMatch() throws Exception {
        final Path schedulerCfg = tempDir.resolve("schedulercfg");
        Files.writeString(schedulerCfg, "Calc[4]; taskmanager-exact\n" + "Calc; taskmanager-pool\n");
        final CapsysExecutionGraphPlacement.PlacementConfig placements =
                CapsysExecutionGraphPlacement.readPlacements(schedulerCfg.toString());

        // Calc[4] hits the exact entry and does not consume the pooled "Calc" entry.
        assertThat(CapsysExecutionGraphPlacement.findTaskManagerAddress("Calc[4]", placements))
                .isEqualTo("taskmanager-exact");
        // The remaining, unclaimed Calc operator gets the pooled entry.
        assertThat(CapsysExecutionGraphPlacement.findTaskManagerAddress("Calc[3]", placements))
                .isEqualTo("taskmanager-pool");
        assertThat(CapsysExecutionGraphPlacement.findTaskManagerAddress("Calc[5]", placements))
                .isNull();
    }

    @Test
    void testFindTaskManagerAddressHandsOutMultiplePrefixEntriesInOrder() throws Exception {
        final Path schedulerCfg = tempDir.resolve("schedulercfg");
        Files.writeString(
                schedulerCfg, "Calc; taskmanager-first\n" + "Calc; taskmanager-second\n");
        final CapsysExecutionGraphPlacement.PlacementConfig placements =
                CapsysExecutionGraphPlacement.readPlacements(schedulerCfg.toString());

        assertThat(CapsysExecutionGraphPlacement.findTaskManagerAddress("Calc[3]", placements))
                .isEqualTo("taskmanager-first");
        assertThat(CapsysExecutionGraphPlacement.findTaskManagerAddress("Calc[4]", placements))
                .isEqualTo("taskmanager-second");
        assertThat(CapsysExecutionGraphPlacement.findTaskManagerAddress("Calc[5]", placements))
                .isNull();
    }
}
