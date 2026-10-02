package com.example.kafkademo.service;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class AbortedRegistryTest {

    @Test
    void remembersRegisteredIdsUntilReset() {
        AbortedRegistry registry = new AbortedRegistry();
        registry.registerAll(List.of("a", "b"));

        assertThat(registry.contains("a")).isTrue();
        assertThat(registry.contains("c")).isFalse();

        registry.reset();
        assertThat(registry.contains("a")).isFalse();
    }

    @Test
    void evictsOldestPastCapacity() {
        AbortedRegistry registry = new AbortedRegistry(2);
        registry.registerAll(List.of("a", "b", "c"));

        assertThat(registry.contains("a")).isFalse();
        assertThat(registry.contains("b")).isTrue();
        assertThat(registry.contains("c")).isTrue();
    }
}
