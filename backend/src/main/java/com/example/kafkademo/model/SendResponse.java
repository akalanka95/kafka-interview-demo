package com.example.kafkademo.model;

import java.util.List;

/**
 * Response of POST /api/messages. Named SendResponse (not SendResult as in the design doc)
 * to avoid clashing with Spring Kafka's {@code org.springframework.kafka.support.SendResult}.
 */
public record SendResponse(
        DeliveryMode mode,
        int requested,
        int acked,
        int failed,
        int duplicatesInjected,
        int aborted,
        long elapsedMs,
        List<RecordResult> sample) {

    /** offset is a string because acks=0 never learns it: reported as "unknown". */
    public record RecordResult(String messageId, int partition, String offset) {}
}
