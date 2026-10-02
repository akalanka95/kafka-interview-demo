package com.example.kafkademo.service;

import com.example.kafkademo.config.KafkaProps;
import com.example.kafkademo.model.ClusterStatus;
import com.example.kafkademo.model.ClusterStatus.BrokerStatus;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Which of the expected brokers are alive, from AdminClient.describeCluster(). A broker is up if
 * the cluster lists it among its live nodes. Cached for {@link #CACHE_MS}: several browser tabs
 * polling every 2 s cost one cluster call.
 */
@Service
public class ClusterService {

    private static final Logger log = LoggerFactory.getLogger(ClusterService.class);

    static final long CACHE_MS = 2_000;
    static final int TIMEOUT_MS = 2_000;

    /** The admin call, separated so tests can stub it. Returns live nodes and the controller id. */
    interface Describer {
        Snapshot describe() throws Exception;
    }

    record Snapshot(Collection<Node> nodes, Integer controllerId) {}

    private final List<Integer> expectedIds;
    private final Describer describer;
    private final LongSupplier clock;
    /** Last known host:port per broker id, so a broker that went down keeps its label. */
    private final Map<Integer, String> hosts = new ConcurrentHashMap<>();

    private volatile ClusterStatus cached;
    private volatile long cachedAt;

    @Autowired
    public ClusterService(KafkaProps props, AdminClient admin) {
        this(props, () -> {
            DescribeClusterResult r = admin.describeCluster(new DescribeClusterOptions().timeoutMs(TIMEOUT_MS));
            Collection<Node> nodes = r.nodes().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            Node controller = r.controller().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            return new Snapshot(nodes, controller == null || controller.isEmpty() ? null : controller.id());
        }, System::currentTimeMillis);
    }

    ClusterService(KafkaProps props, Describer describer, LongSupplier clock) {
        this.expectedIds = props.expectedBrokerIds();
        this.describer = describer;
        this.clock = clock;
        // Until a broker has been seen, label it with the bootstrap entry at the same position.
        String[] bootstrap = props.bootstrapServers().split(",");
        if (bootstrap.length == expectedIds.size()) {
            for (int i = 0; i < bootstrap.length; i++) hosts.put(expectedIds.get(i), bootstrap[i].trim());
        }
    }

    public synchronized ClusterStatus status() {
        long now = clock.getAsLong();
        if (cached == null || now - cachedAt >= CACHE_MS) {
            cached = fetch();
            cachedAt = now;
        }
        return cached;
    }

    private ClusterStatus fetch() {
        try {
            Snapshot s = describer.describe();
            Map<Integer, Node> live = new HashMap<>();
            for (Node n : s.nodes()) {
                live.put(n.id(), n);
                hosts.put(n.id(), n.host() + ":" + n.port());
            }
            List<BrokerStatus> brokers = new ArrayList<>();
            for (int id : expectedIds) brokers.add(new BrokerStatus(id, host(id), live.containsKey(id)));
            return new ClusterStatus(brokers, s.controllerId());
        } catch (Exception e) {
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            log.debug("describeCluster failed: {}", e.toString());
            List<BrokerStatus> brokers = new ArrayList<>();
            for (int id : expectedIds) brokers.add(new BrokerStatus(id, host(id), null));
            return new ClusterStatus(brokers, null);
        }
    }

    private String host(int id) {
        return hosts.getOrDefault(id, "broker-" + id);
    }
}
