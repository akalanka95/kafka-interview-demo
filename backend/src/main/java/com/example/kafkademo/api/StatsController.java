package com.example.kafkademo.api;

import com.example.kafkademo.consumer.ListenerReadiness;
import com.example.kafkademo.consumer.SseBroadcaster;
import com.example.kafkademo.model.StatsSnapshot;
import com.example.kafkademo.service.AbortedRegistry;
import com.example.kafkademo.service.DuplicateTracker;
import com.example.kafkademo.service.StatsService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "Stats", description = "Per-mode sent / received / lost / duplicate counters")
@RestController
@RequestMapping("/api/stats")
public class StatsController {

    private final StatsService stats;
    private final DuplicateTracker duplicates;
    private final AbortedRegistry abortedRegistry;
    private final SseBroadcaster broadcaster;
    private final ListenerReadiness readiness;

    public StatsController(StatsService stats, DuplicateTracker duplicates, AbortedRegistry abortedRegistry,
                           SseBroadcaster broadcaster, ListenerReadiness readiness) {
        this.stats = stats;
        this.duplicates = duplicates;
        this.abortedRegistry = abortedRegistry;
        this.broadcaster = broadcaster;
        this.readiness = readiness;
    }

    @Operation(summary = "Current counters",
            description = "received / duplicates come from the read_committed listener. "
                    + "lost = max(0, sent − received), meaningful once inFlight is false and the batch has settled.")
    @GetMapping
    public StatsSnapshot get() {
        return new StatsSnapshot(stats.modes(), stats.inFlight(), broadcaster.droppedForUi(), readiness.ready());
    }

    @Operation(summary = "Reset all counters, duplicate trackers and the aborted registry")
    @DeleteMapping
    public ResponseEntity<Void> reset() {
        stats.reset();
        duplicates.reset();
        abortedRegistry.reset();
        broadcaster.resetDropped();
        return ResponseEntity.noContent().build();
    }
}
