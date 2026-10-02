package com.example.kafkademo.service;

import com.example.kafkademo.model.DeliveryMode;
import com.example.kafkademo.model.StatsSnapshot.ModeStats;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StatsServiceTest {

    private final StatsService stats = new StatsService();

    @Test
    void lostIsSentMinusReceived() {
        stats.sent(DeliveryMode.AT_MOST_ONCE, 10);
        for (int i = 0; i < 7; i++) stats.received(DeliveryMode.AT_MOST_ONCE);

        assertThat(stats.modes().get(DeliveryMode.AT_MOST_ONCE))
                .isEqualTo(new ModeStats(10, 7, 3, 0, 0));
    }

    @Test
    void lostNeverNegativeAndDuplicatesDoNotCountAsReceived() {
        stats.sent(DeliveryMode.AT_LEAST_ONCE, 1);
        stats.received(DeliveryMode.AT_LEAST_ONCE);
        stats.duplicate(DeliveryMode.AT_LEAST_ONCE);
        stats.received(DeliveryMode.AT_LEAST_ONCE);   // e.g. a record from before a reset

        ModeStats alo = stats.modes().get(DeliveryMode.AT_LEAST_ONCE);
        assertThat(alo.lost()).isZero();
        assertThat(alo.duplicates()).isEqualTo(1);
    }

    @Test
    void listsAllModesInOrderAndResets() {
        stats.sent(DeliveryMode.EXACTLY_ONCE, 5);
        assertThat(stats.modes()).containsOnlyKeys(DeliveryMode.values());
        assertThat(stats.modes().keySet()).containsExactly(DeliveryMode.values());

        stats.reset();
        assertThat(stats.modes().get(DeliveryMode.EXACTLY_ONCE)).isEqualTo(new ModeStats(0, 0, 0, 0, 0));
    }

    @Test
    void inFlightUntilActivitySettles() {
        long[] now = {1_000_000};
        StatsService s = new StatsService(() -> now[0]);
        now[0] += StatsService.SETTLE_MS;
        assertThat(s.inFlight()).isFalse();

        s.requestStarted();
        now[0] += 60_000;
        assertThat(s.inFlight()).as("request still running").isTrue();
        s.requestFinished();
        assertThat(s.inFlight()).as("consumers may still be catching up").isTrue();

        now[0] += StatsService.SETTLE_MS - 1;
        s.received(DeliveryMode.AT_MOST_ONCE);   // a late record restarts the settle window
        now[0] += StatsService.SETTLE_MS - 1;
        assertThat(s.inFlight()).isTrue();
        now[0] += 1;
        assertThat(s.inFlight()).isFalse();
    }
}
