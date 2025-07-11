/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.stats;

import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.wlm.ResourceType;

import java.util.EnumMap;
import java.util.Map;

/**
 * This class will keep the point in time view of the workload group stats
 */
public class WorkloadGroupState {
    /**
     * co-ordinator level completions at the workload group level, this is a cumulative counter since the Opensearch start time
     */
    public final CounterMetric totalCompletions = new CounterMetric();

    /**
     * rejections at the workload group level, this is a cumulative counter since the OpenSearch start time
     */
    public final CounterMetric totalRejections = new CounterMetric();

    /**
     * this will track the cumulative failures in a workload group
     */
    public final CounterMetric failures = new CounterMetric();

    /**
     * This will track total number of cancellations in the workload group due to all resource type breaches
     */
    public final CounterMetric totalCancellations = new CounterMetric();

    /**
     * This is used to store the resource type state both for CPU and MEMORY
     */
    private final Map<ResourceType, ResourceTypeState> resourceState;

    public WorkloadGroupState() {
        resourceState = new EnumMap<>(ResourceType.class);
        for (ResourceType resourceType : ResourceType.values()) {
            if (resourceType.hasStatsEnabled()) {
                resourceState.put(resourceType, new ResourceTypeState(resourceType));
            }
        }
    }

    /**
     *
     * @return co-ordinator completions in the workload group
     */
    public long getTotalCompletions() {
        return totalCompletions.count();
    }

    /**
     *
     * @return rejections in the workload group
     */
    public long getTotalRejections() {
        return totalRejections.count();
    }

    /**
     *
     * @return failures in the workload group
     */
    public long getFailures() {
        return failures.count();
    }

    public long getTotalCancellations() {
        return totalCancellations.count();
    }

    /**
     * getter for workload group resource state
     * @return the workload group resource state
     */
    public Map<ResourceType, ResourceTypeState> getResourceState() {
        return resourceState;
    }

    /**
     * This class holds the resource level stats for the workload group
     */
    public static class ResourceTypeState {
        public final ResourceType resourceType;
        public final CounterMetric cancellations = new CounterMetric();
        public final CounterMetric rejections = new CounterMetric();
        private double lastRecordedUsage = 0;

        public ResourceTypeState(ResourceType resourceType) {
            this.resourceType = resourceType;
        }

        public void setLastRecordedUsage(double recordedUsage) {
            lastRecordedUsage = recordedUsage;
        }

        public double getLastRecordedUsage() {
            return lastRecordedUsage;
        }
    }
}
