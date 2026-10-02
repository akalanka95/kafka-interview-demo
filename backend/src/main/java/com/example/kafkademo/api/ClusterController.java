package com.example.kafkademo.api;

import com.example.kafkademo.model.ClusterStatus;
import com.example.kafkademo.service.ClusterService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "Cluster", description = "Broker health for the header chips")
@RestController
@RequestMapping("/api/cluster")
public class ClusterController {

    private final ClusterService cluster;

    public ClusterController(ClusterService cluster) {
        this.cluster = cluster;
    }

    @Operation(summary = "Which brokers are up",
            description = "AdminClient.describeCluster() with a 2 s timeout, cached 2 s. "
                    + "up = null (unknown) for every broker when the call itself fails.")
    @GetMapping
    public ClusterStatus get() {
        return cluster.status();
    }
}
