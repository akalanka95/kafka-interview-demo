package com.example.kafkademo.service;

import com.example.kafkademo.config.KafkaProps;
import com.example.kafkademo.model.ClusterStatus;
import com.example.kafkademo.model.ClusterStatus.BrokerStatus;
import com.example.kafkademo.service.ClusterService.Snapshot;
import org.apache.kafka.common.Node;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterServiceTest {

    private final KafkaProps props = new KafkaProps("localhost:19092,localhost:29092,localhost:39092",
            "web-app", "pw", "web.messages", List.of(1, 2, 3));
    private final AtomicLong now = new AtomicLong(1_000);

    @Test
    void marksMissingBrokerDown() {
        ClusterService service = new ClusterService(props,
                () -> new Snapshot(List.of(node(1, 19092), node(3, 39092)), 1), now::get);

        ClusterStatus status = service.status();

        assertThat(status.brokers()).containsExactly(
                new BrokerStatus(1, "localhost:19092", true),
                new BrokerStatus(2, "localhost:29092", false),
                new BrokerStatus(3, "localhost:39092", true));
        assertThat(status.controllerId()).isEqualTo(1);
    }

    @Test
    void reportsUnknownWhenTheCallFails() {
        ClusterService service = new ClusterService(props, () -> {
            throw new java.util.concurrent.TimeoutException("no quorum");
        }, now::get);

        ClusterStatus status = service.status();

        assertThat(status.brokers()).extracting(BrokerStatus::up).containsOnlyNulls();
        assertThat(status.controllerId()).isNull();
    }

    @Test
    void cachesForTwoSeconds() {
        AtomicInteger calls = new AtomicInteger();
        ClusterService service = new ClusterService(props, () -> {
            calls.incrementAndGet();
            return new Snapshot(List.of(node(1, 19092)), 1);
        }, now::get);

        service.status();
        now.addAndGet(ClusterService.CACHE_MS - 1);
        service.status();
        assertThat(calls).hasValue(1);

        now.addAndGet(1);
        service.status();
        assertThat(calls).hasValue(2);
    }

    private static Node node(int id, int port) {
        return new Node(id, "localhost", port);
    }
}
