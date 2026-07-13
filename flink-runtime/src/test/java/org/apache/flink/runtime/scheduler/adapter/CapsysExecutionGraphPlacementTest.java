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
import java.util.Map;

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

        final Map<String, String> placements =
                CapsysExecutionGraphPlacement.readPlacements(schedulerCfg.toString());

        assertThat(placements)
                .containsEntry("Source", "taskmanager-2")
                .containsEntry("WatermarkAssigner", "taskmanager-2")
                .containsEntry("Calc", "taskmanager-3")
                .containsEntry("Sink", "taskmanager-4");
    }

    @Test
    void testFindTaskManagerAddressRequiresExactMatch() {
        final Map<String, String> placements =
                Map.of(
                        "Source", "taskmanager-2",
                        "WatermarkAssigner", "taskmanager-2",
                        "Calc", "taskmanager-3",
                        "Sink", "taskmanager-4");

        assertThat(CapsysExecutionGraphPlacement.findTaskManagerAddress("Source", placements))
                .isEqualTo("taskmanager-2");
        assertThat(
                        CapsysExecutionGraphPlacement.findTaskManagerAddress(
                                "WatermarkAssigner", placements))
                .isEqualTo("taskmanager-2");
        assertThat(CapsysExecutionGraphPlacement.findTaskManagerAddress("Sink", placements))
                .isEqualTo("taskmanager-4");
        assertThat(
                        CapsysExecutionGraphPlacement.findTaskManagerAddress(
                                "Source: File Source", placements))
                .isNull();
    }
}
