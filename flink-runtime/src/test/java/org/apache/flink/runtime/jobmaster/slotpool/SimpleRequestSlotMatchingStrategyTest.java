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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.scheduler.TestingPhysicalSlot;
import org.apache.flink.runtime.scheduler.loading.DefaultLoadingWeight;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.flink.shaded.guava33.com.google.common.collect.Iterators;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link SimpleRequestSlotMatchingStrategy}. */
@ExtendWith(TestLoggerExtension.class)
public class SimpleRequestSlotMatchingStrategyTest {

    @Test
    public void testSlotRequestsAreMatchedInOrder() {
        final SimpleRequestSlotMatchingStrategy simpleRequestSlotMatchingStrategy =
                SimpleRequestSlotMatchingStrategy.INSTANCE;

        final Collection<PhysicalSlot> slots = Arrays.asList(TestingPhysicalSlot.builder().build());
        final PendingRequest pendingRequest1 =
                PendingRequest.createNormalRequest(
                        new SlotRequestId(),
                        ResourceProfile.UNKNOWN,
                        DefaultLoadingWeight.EMPTY,
                        Collections.emptyList());
        final PendingRequest pendingRequest2 =
                PendingRequest.createNormalRequest(
                        new SlotRequestId(),
                        ResourceProfile.UNKNOWN,
                        DefaultLoadingWeight.EMPTY,
                        Collections.emptyList());
        final Collection<PendingRequest> pendingRequests =
                Arrays.asList(pendingRequest1, pendingRequest2);

        final Collection<RequestSlotMatchingStrategy.RequestSlotMatch> requestSlotMatches =
                simpleRequestSlotMatchingStrategy.matchRequestsAndSlots(
                        slots, pendingRequests, new HashMap<>());

        assertThat(requestSlotMatches).hasSize(1);
        assertThat(
                        Iterators.getOnlyElement(requestSlotMatches.iterator())
                                .getPendingRequest()
                                .getSlotRequestId())
                .isEqualTo(pendingRequest1.getSlotRequestId());
    }

    @Test
    public void testSlotRequestsThatCanBeFulfilledAreMatched() {
        final SimpleRequestSlotMatchingStrategy simpleRequestSlotMatchingStrategy =
                SimpleRequestSlotMatchingStrategy.INSTANCE;

        final ResourceProfile small = ResourceProfile.newBuilder().setCpuCores(1.0).build();
        final ResourceProfile large = ResourceProfile.newBuilder().setCpuCores(2.0).build();

        final Collection<PhysicalSlot> slots =
                Arrays.asList(
                        TestingPhysicalSlot.builder().withResourceProfile(small).build(),
                        TestingPhysicalSlot.builder().withResourceProfile(small).build());

        final PendingRequest pendingRequest1 =
                PendingRequest.createNormalRequest(
                        new SlotRequestId(),
                        large,
                        DefaultLoadingWeight.EMPTY,
                        Collections.emptyList());
        final PendingRequest pendingRequest2 =
                PendingRequest.createNormalRequest(
                        new SlotRequestId(),
                        small,
                        DefaultLoadingWeight.EMPTY,
                        Collections.emptyList());
        final Collection<PendingRequest> pendingRequests =
                Arrays.asList(pendingRequest1, pendingRequest2);

        final Collection<RequestSlotMatchingStrategy.RequestSlotMatch> requestSlotMatches =
                simpleRequestSlotMatchingStrategy.matchRequestsAndSlots(
                        slots, pendingRequests, new HashMap<>());

        assertThat(requestSlotMatches).hasSize(1);
        assertThat(
                        Iterators.getOnlyElement(requestSlotMatches.iterator())
                                .getPendingRequest()
                                .getSlotRequestId())
                .isEqualTo(pendingRequest2.getSlotRequestId());
    }

    @Test
    public void testTaskManagerAddressCanMatchSlotIpWhenHostnameDiffers() throws Exception {
        final SimpleRequestSlotMatchingStrategy simpleRequestSlotMatchingStrategy =
                SimpleRequestSlotMatchingStrategy.INSTANCE;

        final String requestedAddress = "10.10.0.13";
        final PhysicalSlot slot =
                createSlotWithHostAndIp(
                        "ip-10-10-0-13",
                        "ip-10-10-0-13.eu-central-1.compute.internal",
                        requestedAddress);
        final PendingRequest pendingRequest = createPendingRequestWithAddress(requestedAddress);

        final Collection<RequestSlotMatchingStrategy.RequestSlotMatch> requestSlotMatches =
                simpleRequestSlotMatchingStrategy.matchRequestsAndSlots(
                        Collections.singletonList(slot),
                        Collections.singletonList(pendingRequest),
                        new HashMap<>());

        assertThat(requestSlotMatches).hasSize(1);
        assertThat(
                        Iterators.getOnlyElement(requestSlotMatches.iterator())
                                .getPendingRequest()
                                .getSlotRequestId())
                .isEqualTo(pendingRequest.getSlotRequestId());
    }

    @Test
    public void testTaskManagerAddressMismatchRemainsUnmatched() throws Exception {
        final SimpleRequestSlotMatchingStrategy simpleRequestSlotMatchingStrategy =
                SimpleRequestSlotMatchingStrategy.INSTANCE;

        final PhysicalSlot slot =
                createSlotWithHostAndIp(
                        "ip-10-10-0-13",
                        "ip-10-10-0-13.eu-central-1.compute.internal",
                        "10.10.0.13");
        final PendingRequest pendingRequest = createPendingRequestWithAddress("10.10.0.14");

        final Collection<RequestSlotMatchingStrategy.RequestSlotMatch> requestSlotMatches =
                simpleRequestSlotMatchingStrategy.matchRequestsAndSlots(
                        Collections.singletonList(slot),
                        Collections.singletonList(pendingRequest),
                        new HashMap<>());

        assertThat(requestSlotMatches).isEmpty();
    }

    private static PendingRequest createPendingRequestWithAddress(String taskManagerAddress) {
        final ResourceProfile requestedProfile = ResourceProfile.UNKNOWN.clone();
        requestedProfile.setTaskManagerAddress(taskManagerAddress);
        return PendingRequest.createNormalRequest(
                new SlotRequestId(),
                requestedProfile,
                DefaultLoadingWeight.EMPTY,
                Collections.emptyList());
    }

    private static PhysicalSlot createSlotWithHostAndIp(
            String hostName, String fqdnHostName, String ipAddress) throws Exception {
        final TaskManagerLocation taskManagerLocation =
                new TaskManagerLocation(
                        new ResourceID("tm-1"),
                        InetAddress.getByAddress(
                                fqdnHostName, InetAddress.getByName(ipAddress).getAddress()),
                        12345,
                        new TaskManagerLocation.HostNameSupplier() {
                            @Override
                            public String getHostName() {
                                return hostName;
                            }

                            @Override
                            public String getFqdnHostName() {
                                return fqdnHostName;
                            }
                        },
                        hostName);

        return TestingPhysicalSlot.builder()
                .withTaskManagerLocation(taskManagerLocation)
                .withResourceProfile(ResourceProfile.ANY)
                .build();
    }
}
