package com.example.kafkademo.service;

import com.example.kafkademo.model.View;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Remembers recently seen message-ids per view, so a second record with the same id is flagged as
 * a duplicate. Bounded (oldest evicted first) so a long demo cannot exhaust memory.
 */
@Component
public class DuplicateTracker {

    static final int DEFAULT_CAPACITY = 200_000;

    private final Map<View, Map<String, Boolean>> seen = new EnumMap<>(View.class);

    public DuplicateTracker() {
        this(DEFAULT_CAPACITY);
    }

    DuplicateTracker(int capacity) {
        for (View v : View.values()) {
            seen.put(v, new LinkedHashMap<>(1024, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                    return size() > capacity;
                }
            });
        }
    }

    /** Records the id and returns true if this view had already seen it. */
    public boolean seenBefore(View view, String messageId) {
        Map<String, Boolean> ids = seen.get(view);
        synchronized (ids) {   // each listener runs one thread per partition
            return ids.put(messageId, Boolean.TRUE) != null;
        }
    }

    public void reset() {
        seen.values().forEach(ids -> {
            synchronized (ids) {
                ids.clear();
            }
        });
    }
}
