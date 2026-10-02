package com.example.kafkademo.service;

import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Message-ids written in a transaction that the producer is about to abort. The read_uncommitted
 * listener sees those records (the abort marker only hides them from read_committed), and uses this
 * registry to tag them. Ids are registered before the records are sent, so the registry is always
 * ahead of the consumer. Bounded (oldest evicted first).
 */
@Component
public class AbortedRegistry {

    static final int DEFAULT_CAPACITY = 100_000;

    private final Map<String, Boolean> ids;

    public AbortedRegistry() {
        this(DEFAULT_CAPACITY);
    }

    AbortedRegistry(int capacity) {
        ids = new LinkedHashMap<>(1024, 0.75f, false) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                return size() > capacity;
            }
        };
    }

    public synchronized void registerAll(Collection<String> messageIds) {
        messageIds.forEach(id -> ids.put(id, Boolean.TRUE));
    }

    public synchronized boolean contains(String messageId) {
        return ids.containsKey(messageId);
    }

    public synchronized void reset() {
        ids.clear();
    }
}
