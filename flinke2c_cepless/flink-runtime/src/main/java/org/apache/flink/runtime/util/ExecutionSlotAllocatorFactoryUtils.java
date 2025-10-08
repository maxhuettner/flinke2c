/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.util;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequestBulkChecker;

import org.apache.flink.runtime.scheduler.ExecutionSlotAllocatorFactory;

import org.apache.flink.runtime.scheduler.SimpleExecutionSlotAllocator;

import org.apache.flink.runtime.scheduler.SlotSharingExecutionSlotAllocatorFactory;

import static org.apache.flink.configuration.ClusterOptions.EXECUTION_SLOT_ALLOCATOR_TYPE;

/** Utility class for selecting {@link ExecutionSlotAllocatorFactory}. */
public class ExecutionSlotAllocatorFactoryUtils {

    public static ExecutionSlotAllocatorFactory selectExecutionSlotAllocatorFactory(
            final JobType jobType, final Configuration configuration, PhysicalSlotProvider physicalSlotProvider, PhysicalSlotRequestBulkChecker bulkChecker, Time slotRequestTimeout) {
        final ClusterOptions.ExecutionSlotAllocatorType slotAllocationType =
                configuration.get(EXECUTION_SLOT_ALLOCATOR_TYPE);

        switch (slotAllocationType) {
            case SIMPLE:
                return new SimpleExecutionSlotAllocator.Factory(
                        physicalSlotProvider, jobType == JobType.STREAMING);
            case SLOT_SHARING:
            default:
                return new SlotSharingExecutionSlotAllocatorFactory(
                        physicalSlotProvider,
                        jobType == JobType.STREAMING,
                        bulkChecker,
                        slotRequestTimeout);
        }
    }

    /** Private default constructor to avoid being instantiated. */
    private ExecutionSlotAllocatorFactoryUtils() {}
}
