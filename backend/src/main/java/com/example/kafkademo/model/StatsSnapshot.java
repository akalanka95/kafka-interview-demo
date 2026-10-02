package com.example.kafkademo.model;

import java.util.Map;

/**
 * Response of GET /api/stats. {@code consumersReady} is true once both listeners own their
 * partitions; records sent before that are skipped by the listeners and therefore count as lost.
 */
public record StatsSnapshot(
        Map<DeliveryMode, ModeStats> modes,
        boolean inFlight,
        long droppedForUi,
        boolean consumersReady) {

    /** lost = max(0, sent − received); only meaningful once a batch has settled. */
    public record ModeStats(long sent, long received, long lost, long duplicates, long aborted) {}
}
