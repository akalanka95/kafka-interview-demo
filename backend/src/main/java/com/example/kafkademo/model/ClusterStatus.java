package com.example.kafkademo.model;

import java.util.List;

/** Response of GET /api/cluster. {@code controllerId} is null when the cluster call failed. */
public record ClusterStatus(List<BrokerStatus> brokers, Integer controllerId) {

    /** {@code up} is null (unknown) when the cluster call itself failed. */
    public record BrokerStatus(int id, String host, Boolean up) {}
}
