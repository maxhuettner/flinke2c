/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Simple implementation of the {@link RequestSlotMatchingStrategy} that matches the pending
 * requests in order as long as the resource profile can be fulfilled.
 */
public enum SimpleRequestSlotMatchingStrategy implements RequestSlotMatchingStrategy {
    INSTANCE;

    private static final Logger log = LoggerFactory.getLogger(SimpleRequestSlotMatchingStrategy.class);

    @Override
    public Collection<RequestSlotMatch> matchRequestsAndSlots(
            Collection<? extends PhysicalSlot> slots, Collection<PendingRequest> pendingRequests) {
        final Collection<RequestSlotMatch> resultingMatches = new ArrayList<>();

        final Collection<PhysicalSlot> availableSlots = new ArrayList<>(slots);

        final LinkedList<PendingRequest> prioritizedRequests = new LinkedList<>();
        final LinkedList<PendingRequest> generalRequests = new LinkedList<>();

        for (PendingRequest pendingRequest : pendingRequests) {
            if (pendingRequest.getResourceProfile().getTaskManagerAddress() != null) {
                prioritizedRequests.add(pendingRequest);
            } else {
                generalRequests.add(pendingRequest);
            }
        }

        log.info("Matching pending requests {} with available slots {}", pendingRequests, slots);

        matchRequests(prioritizedRequests, availableSlots, resultingMatches, true);

        matchRequests(generalRequests, availableSlots, resultingMatches, false);

        return resultingMatches;
    }

    private void matchRequests(
            LinkedList<PendingRequest> pendingRequests,
            Collection<PhysicalSlot> availableSlots,
            Collection<RequestSlotMatch> resultingMatches,
            boolean prioritizeByAddress) {
        final Iterator<PendingRequest> pendingRequestIterator = pendingRequests.iterator();

        while (pendingRequestIterator.hasNext()) {
            final PendingRequest pendingRequest = pendingRequestIterator.next();
            final String taskManagerAddress = pendingRequest.getResourceProfile().getTaskManagerAddress();

            for (Iterator<PhysicalSlot> slotIterator = availableSlots.iterator(); slotIterator.hasNext(); ) {
                PhysicalSlot slot = slotIterator.next();
                final String slotAddress = slot.getTaskManagerLocation().getHostname();

                if (prioritizeByAddress && (taskManagerAddress == null || !slotAddress.contains(taskManagerAddress))) {
                    log.debug(
                            "Skipping slot {} because it does not match the task manager address {}",
                            slot,
                            taskManagerAddress);
                    continue;
                }

                log.debug("Trying to match slot {} with request {}", slot, pendingRequest);

                if (slot.getResourceProfile().isMatching(pendingRequest.getResourceProfile())) {
                    log.debug("Matched slot {} with request {}", slot, pendingRequest);
                    resultingMatches.add(RequestSlotMatch.createFor(pendingRequest, slot));
                    pendingRequestIterator.remove();
                    slotIterator.remove();
                    break;
                }
            }
        }
    }

    @Override
    public String toString() {
        return SimpleRequestSlotMatchingStrategy.class.getSimpleName();
    }
}
