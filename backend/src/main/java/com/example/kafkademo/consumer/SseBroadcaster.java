package com.example.kafkademo.consumer;

import com.example.kafkademo.model.MessageEvent;
import com.example.kafkademo.model.View;
import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.LongAdder;

/**
 * Fans consumed records out to every open /api/stream connection. Events are batched every
 * 100 ms so a 10 000-message run does not flood the browser; past the per-view cap they are
 * dropped for the UI only (stats are counted by the listener and stay exact).
 */
@Component
public class SseBroadcaster {

    /** Per view per flush, so one column can't starve the other. 2 × 250 = the 500/batch in the doc. */
    static final int MAX_PER_VIEW = 250;

    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();
    private final Queue<MessageEvent> queue = new ConcurrentLinkedQueue<>();
    private final LongAdder dropped = new LongAdder();

    public SseEmitter register() {
        SseEmitter emitter = new SseEmitter(0L);   // 0 = never time out
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(e -> emitters.remove(emitter));
        emitters.add(emitter);
        send(emitter, "ping", "connected");   // flushes headers so the client sees the stream open
        return emitter;
    }

    public void enqueue(MessageEvent event) {
        if (!emitters.isEmpty()) queue.add(event);   // nobody listening → don't buffer
    }

    public long droppedForUi() { return dropped.sum(); }

    public void resetDropped() { dropped.reset(); }

    @Scheduled(fixedDelay = 100)
    void flush() {
        List<MessageEvent> batch = drainBatch();
        if (!batch.isEmpty()) emitters.forEach(e -> send(e, "batch", batch));
    }

    @Scheduled(fixedRate = 15_000)
    void ping() {
        emitters.forEach(e -> send(e, "ping", "keepalive"));
    }

    /** Takes everything queued so far: up to MAX_PER_VIEW per view, the rest is counted as dropped. */
    List<MessageEvent> drainBatch() {
        Map<View, Integer> taken = new EnumMap<>(View.class);
        List<MessageEvent> batch = new ArrayList<>();
        for (int n = queue.size(); n > 0; n--) {
            MessageEvent e = queue.poll();
            if (e == null) break;
            int count = taken.merge(e.view(), 1, Integer::sum);
            if (count <= MAX_PER_VIEW) batch.add(e);
            else dropped.increment();
        }
        return batch;
    }

    private void send(SseEmitter emitter, String name, Object data) {
        try {
            emitter.send(SseEmitter.event().name(name).data(data, MediaType.APPLICATION_JSON));
        } catch (IOException | IllegalStateException e) {
            emitters.remove(emitter);   // client went away
        }
    }
}
