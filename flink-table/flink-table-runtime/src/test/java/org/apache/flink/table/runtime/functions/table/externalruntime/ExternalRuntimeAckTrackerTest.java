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

package org.apache.flink.table.runtime.functions.table.externalruntime;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ExternalRuntimeAckTracker}. */
class ExternalRuntimeAckTrackerTest {

    @Test
    void testThresholdAckUsesHighestContiguousRowId() {
        final ExternalRuntimeAckTracker tracker = new ExternalRuntimeAckTracker(3, 50, 16);

        tracker.markReceived(0L, 0L);
        tracker.markReceived(2L, 1L);
        tracker.markReceived(1L, 2L);

        assertThat(tracker.pollReadyAck(2L, false)).isEqualTo(2L);
        assertThat(tracker.pollReadyAck(3L, false)).isEqualTo(-1L);
    }

    @Test
    void testTimeoutAckWaitsForContiguousGapToClose() {
        final ExternalRuntimeAckTracker tracker = new ExternalRuntimeAckTracker(10, 5, 16);

        tracker.markReceived(1L, 0L);
        tracker.markReceived(2L, 1_000_000L);

        assertThat(tracker.pollReadyAck(6_000_000L, false)).isEqualTo(-1L);

        tracker.markReceived(0L, 7_000_000L);

        assertThat(tracker.pollReadyAck(10_000_000L, false)).isEqualTo(-1L);
        assertThat(tracker.pollReadyAck(12_000_000L, false)).isEqualTo(2L);
    }

    @Test
    void testForceFlushReturnsLatestAckImmediately() {
        final ExternalRuntimeAckTracker tracker = new ExternalRuntimeAckTracker(100, 1000, 16);

        tracker.markReceived(0L, 0L);
        tracker.markReceived(1L, 1L);

        assertThat(tracker.pollReadyAck(2L, true)).isEqualTo(1L);
        assertThat(tracker.pollReadyAck(3L, true)).isEqualTo(-1L);
    }
}
