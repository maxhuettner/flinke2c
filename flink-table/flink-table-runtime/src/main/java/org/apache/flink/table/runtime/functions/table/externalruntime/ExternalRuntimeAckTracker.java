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

import java.util.Map;
import java.util.TreeMap;

/** Tracks the highest contiguous received rowId and emits cumulative ACK watermarks. */
final class ExternalRuntimeAckTracker {
    private static final long NO_ACK_READY = -1L;

    private final int ackEveryRows;
    private final long ackFlushTimeoutNanos;
    private final int maxPendingIntervals;
    private final TreeMap<Long, Long> pendingIntervals;

    private long highestContiguousReceivedRowId;
    private long lastAckedRowId;
    private long firstUnackedContiguousAtNanos;

    ExternalRuntimeAckTracker(int ackEveryRows, long ackFlushTimeoutMs, int maxPendingIntervals) {
        this.ackEveryRows = Math.max(1, ackEveryRows);
        this.ackFlushTimeoutNanos = Math.max(0L, ackFlushTimeoutMs) * 1_000_000L;
        this.maxPendingIntervals = Math.max(1, maxPendingIntervals);
        this.pendingIntervals = new TreeMap<>();
        this.highestContiguousReceivedRowId = -1L;
        this.lastAckedRowId = -1L;
        this.firstUnackedContiguousAtNanos = Long.MIN_VALUE;
    }

    synchronized void markReceived(long rowId, long nowNanos) {
        if (rowId <= highestContiguousReceivedRowId) {
            return;
        }

        final long previousHighest = highestContiguousReceivedRowId;
        mergeRowId(rowId);
        promoteContiguousPrefix();

        if (highestContiguousReceivedRowId > previousHighest
                && previousHighest <= lastAckedRowId
                && highestContiguousReceivedRowId > lastAckedRowId) {
            firstUnackedContiguousAtNanos = nowNanos;
        }
    }

    synchronized long pollReadyAck(long nowNanos, boolean force) {
        if (highestContiguousReceivedRowId <= lastAckedRowId) {
            return NO_ACK_READY;
        }

        final long ackDelta = highestContiguousReceivedRowId - lastAckedRowId;
        if (!force && ackDelta < ackEveryRows) {
            if (ackFlushTimeoutNanos <= 0L
                    || firstUnackedContiguousAtNanos == Long.MIN_VALUE
                    || nowNanos - firstUnackedContiguousAtNanos < ackFlushTimeoutNanos) {
                return NO_ACK_READY;
            }
        }

        lastAckedRowId = highestContiguousReceivedRowId;
        firstUnackedContiguousAtNanos = Long.MIN_VALUE;
        return lastAckedRowId;
    }

    private void mergeRowId(long rowId) {
        long intervalStart = rowId;
        long intervalEnd = rowId;

        final Map.Entry<Long, Long> floor = pendingIntervals.floorEntry(rowId);
        if (floor != null) {
            final long floorStart = floor.getKey();
            final long floorEnd = floor.getValue();
            if (rowId <= floorEnd) {
                return;
            }
            if (floorEnd + 1L == rowId) {
                intervalStart = floorStart;
                intervalEnd = Math.max(intervalEnd, floorEnd);
                pendingIntervals.remove(floorStart);
            }
        }

        Map.Entry<Long, Long> next = pendingIntervals.ceilingEntry(intervalStart);
        while (next != null && next.getKey() <= intervalEnd + 1L) {
            intervalEnd = Math.max(intervalEnd, next.getValue());
            pendingIntervals.remove(next.getKey());
            next = pendingIntervals.ceilingEntry(intervalStart);
        }

        pendingIntervals.put(intervalStart, intervalEnd);
        if (pendingIntervals.size() > maxPendingIntervals) {
            throw new IllegalStateException(
                    "External runtime cumulative ACK tracker exceeded "
                            + maxPendingIntervals
                            + " pending intervals.");
        }
    }

    private void promoteContiguousPrefix() {
        final long nextExpected = highestContiguousReceivedRowId + 1L;
        final Map.Entry<Long, Long> first = pendingIntervals.firstEntry();
        if (first == null || first.getKey() != nextExpected) {
            return;
        }

        pendingIntervals.pollFirstEntry();
        highestContiguousReceivedRowId = first.getValue();
        while (true) {
            final Map.Entry<Long, Long> next = pendingIntervals.firstEntry();
            if (next == null || next.getKey() != highestContiguousReceivedRowId + 1L) {
                return;
            }
            pendingIntervals.pollFirstEntry();
            highestContiguousReceivedRowId = next.getValue();
        }
    }
}
