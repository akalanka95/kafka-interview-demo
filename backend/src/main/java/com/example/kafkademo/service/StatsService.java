package com.example.kafkademo.service;

import com.example.kafkademo.model.DeliveryMode;
import com.example.kafkademo.model.StatsSnapshot.ModeStats;
import org.springframework.stereotype.Service;

import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

/**
 * Per-mode counters. {@code sent} comes from the producer; {@code received} and {@code duplicates}
 * come from the read_committed listener only, so aborted records never count as received.
 */
@Service
public class StatsService {

    private static final class Counters {
        final LongAdder sent = new LongAdder();
        final LongAdder received = new LongAdder();
        final LongAdder duplicates = new LongAdder();
        final LongAdder aborted = new LongAdder();
    }

    /**
     * After the last send or receive, keep reporting inFlight this long. The listeners trail the
     * producer by up to ~4 s on a loaded laptop (measured), and without this the UI would briefly show the
     * not-yet-consumed records as lost.
     */
    static final long SETTLE_MS = 5_000;

    private final Map<DeliveryMode, Counters> counters = new EnumMap<>(DeliveryMode.class);
    private final AtomicInteger activeRequests = new AtomicInteger();
    private final AtomicLong lastActivityMs = new AtomicLong();
    private final LongSupplier clock;

    public StatsService() {
        this(System::currentTimeMillis);
    }

    StatsService(LongSupplier clock) {
        this.clock = clock;
        for (DeliveryMode m : DeliveryMode.values()) counters.put(m, new Counters());
    }

    public void sent(DeliveryMode mode, int n) { counters.get(mode).sent.add(n); touch(); }
    public void received(DeliveryMode mode) { counters.get(mode).received.increment(); touch(); }
    public void duplicate(DeliveryMode mode) { counters.get(mode).duplicates.increment(); touch(); }
    public void aborted(DeliveryMode mode, int n) { counters.get(mode).aborted.add(n); }

    public void requestStarted() { activeRequests.incrementAndGet(); touch(); }
    public void requestFinished() { activeRequests.decrementAndGet(); touch(); }

    /** A request is running, or records were sent / received within the last {@link #SETTLE_MS}. */
    public boolean inFlight() {
        return activeRequests.get() > 0 || clock.getAsLong() - lastActivityMs.get() < SETTLE_MS;
    }

    private void touch() { lastActivityMs.set(clock.getAsLong()); }

    public Map<DeliveryMode, ModeStats> modes() {
        Map<DeliveryMode, ModeStats> out = new LinkedHashMap<>();
        counters.forEach((mode, c) -> {
            long sent = c.sent.sum();
            long received = c.received.sum();
            out.put(mode, new ModeStats(sent, received, Math.max(0, sent - received),
                    c.duplicates.sum(), c.aborted.sum()));
        });
        return out;
    }

    public void reset() {
        counters.values().forEach(c -> {
            c.sent.reset();
            c.received.reset();
            c.duplicates.reset();
            c.aborted.reset();
        });
    }
}
