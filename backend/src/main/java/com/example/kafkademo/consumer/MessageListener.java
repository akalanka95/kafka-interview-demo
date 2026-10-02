package com.example.kafkademo.consumer;

import com.example.kafkademo.model.DeliveryMode;
import com.example.kafkademo.model.MessageEvent;
import com.example.kafkademo.model.View;
import com.example.kafkademo.service.AbortedRegistry;
import com.example.kafkademo.service.DuplicateTracker;
import com.example.kafkademo.service.StatsService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Two consumer groups on web.messages, identical except for isolation.level. Each record is
 * checked for duplicates, counted (committed view only) and handed to the SSE broadcaster.
 */
@Component
public class MessageListener implements ConsumerSeekAware {

    public static final String UNCOMMITTED_ID = "uncommitted";
    public static final String COMMITTED_ID = "committed";

    private static final Logger log = LoggerFactory.getLogger(MessageListener.class);

    private final DuplicateTracker duplicates;
    private final AbortedRegistry abortedRegistry;
    private final StatsService stats;
    private final SseBroadcaster broadcaster;
    private final ObjectMapper json;

    /** Per consumer thread: has this thread already been positioned at the end of its partitions? */
    private final ThreadLocal<Boolean> positioned = ThreadLocal.withInitial(() -> false);

    public MessageListener(DuplicateTracker duplicates, AbortedRegistry abortedRegistry, StatsService stats,
                           SseBroadcaster broadcaster, ObjectMapper json) {
        this.duplicates = duplicates;
        this.abortedRegistry = abortedRegistry;
        this.stats = stats;
        this.broadcaster = broadcaster;
        this.json = json;
    }

    @KafkaListener(id = UNCOMMITTED_ID, groupId = "web-consumer-uncommitted",
            topics = "${demo.kafka.topic}", containerFactory = "uncommittedFactory")
    void onUncommitted(ConsumerRecord<String, String> record) {
        handle(View.UNCOMMITTED, record);
    }

    @KafkaListener(id = COMMITTED_ID, groupId = "web-consumer-committed",
            topics = "${demo.kafka.topic}", containerFactory = "committedFactory")
    void onCommitted(ConsumerRecord<String, String> record) {
        handle(View.COMMITTED, record);
    }

    /**
     * Start every backend run at the end of the topic. Otherwise the groups' committed offsets would
     * replay old records after a restart and count them as received against a fresh sent=0.
     * Only the first assignment of each consumer thread seeks: a later rebalance (e.g. during a
     * broker kill) resumes from committed offsets, so nothing in flight is skipped.
     */
    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        if (!positioned.get() && !assignments.isEmpty()) {
            callback.seekToEnd(assignments.keySet());
            positioned.set(true);
            log.info("positioned at end of {}", assignments.keySet());
        }
    }

    void handle(View view, ConsumerRecord<String, String> record) {
        String messageId = header(record, "message-id");
        DeliveryMode mode = parseMode(header(record, "mode"));
        if (messageId == null || mode == null) {
            // e.g. records written by smoke-test.ps1 without our headers
            log.debug("skipping foreign record {}-{}@{}", record.topic(), record.partition(), record.offset());
            return;
        }

        boolean duplicate = duplicates.seenBefore(view, messageId);
        if (view == View.COMMITTED) {
            if (duplicate) stats.duplicate(mode);
            else stats.received(mode);
        }

        // Only read_uncommitted ever sees a record of an aborted transaction.
        boolean aborted = view == View.UNCOMMITTED && abortedRegistry.contains(messageId);
        broadcaster.enqueue(new MessageEvent(view, messageId, text(record.value()), record.key(), mode,
                record.topic(), record.partition(), record.offset(), record.timestamp(),
                duplicate, aborted, null, null, false));
    }

    private String text(String value) {
        if (value == null) return null;
        try {
            JsonNode node = json.readTree(value);
            return node.hasNonNull("text") ? node.get("text").asText() : value;
        } catch (Exception e) {
            return value;
        }
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    private static DeliveryMode parseMode(String raw) {
        if (raw == null) return null;
        try {
            return DeliveryMode.valueOf(raw);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
