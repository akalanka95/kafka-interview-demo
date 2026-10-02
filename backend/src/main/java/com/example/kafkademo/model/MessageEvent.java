package com.example.kafkademo.model;

/**
 * One consumed record as pushed to the UI over SSE. {@code seq}, {@code worker} and
 * {@code outOfOrder} belong to UI_v2 and stay null / false until then.
 */
public record MessageEvent(
        View view,
        String messageId,
        String text,
        String key,
        DeliveryMode mode,
        String topic,
        int partition,
        long offset,
        long timestamp,
        boolean duplicate,
        boolean aborted,
        Integer seq,
        Integer worker,
        boolean outOfOrder) {
}
