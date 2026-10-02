package com.example.kafkademo.service;

import com.example.kafkademo.model.View;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DuplicateTrackerTest {

    @Test
    void flagsSecondSightingPerView() {
        DuplicateTracker t = new DuplicateTracker();
        assertThat(t.seenBefore(View.COMMITTED, "a")).isFalse();
        assertThat(t.seenBefore(View.COMMITTED, "a")).isTrue();
        // views are independent: each listener sees every record once
        assertThat(t.seenBefore(View.UNCOMMITTED, "a")).isFalse();
    }

    @Test
    void evictsOldestBeyondCapacity() {
        DuplicateTracker t = new DuplicateTracker(2);
        t.seenBefore(View.COMMITTED, "a");
        t.seenBefore(View.COMMITTED, "b");
        t.seenBefore(View.COMMITTED, "c");   // evicts "a"
        assertThat(t.seenBefore(View.COMMITTED, "c")).isTrue();
        assertThat(t.seenBefore(View.COMMITTED, "a")).isFalse();
    }

    @Test
    void resetForgetsEverything() {
        DuplicateTracker t = new DuplicateTracker();
        t.seenBefore(View.COMMITTED, "a");
        t.reset();
        assertThat(t.seenBefore(View.COMMITTED, "a")).isFalse();
    }
}
