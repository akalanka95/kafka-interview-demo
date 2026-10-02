package com.example.kafkademo.consumer;

import com.example.kafkademo.model.DeliveryMode;
import com.example.kafkademo.model.MessageEvent;
import com.example.kafkademo.model.View;
import com.example.kafkademo.service.DuplicateTracker;
import com.example.kafkademo.service.StatsService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MessageListenerTest {

    private final StatsService stats = new StatsService();
    private final SseBroadcaster broadcaster = new SseBroadcaster();
    private final MessageListener listener =
            new MessageListener(new DuplicateTracker(), stats, broadcaster, new ObjectMapper());

    @BeforeEach
    void subscribe() {
        broadcaster.register();   // so events are buffered and can be inspected
    }

    @Test
    void countsOnlyCommittedViewAndFlagsDuplicates() {
        listener.handle(View.UNCOMMITTED, record(10, "m1"));
        listener.handle(View.COMMITTED, record(10, "m1"));
        listener.handle(View.COMMITTED, record(11, "m1"));   // same id, new offset

        var amo = stats.modes().get(DeliveryMode.AT_MOST_ONCE);
        assertThat(amo.received()).isEqualTo(1);
        assertThat(amo.duplicates()).isEqualTo(1);

        List<MessageEvent> events = broadcaster.drainBatch();
        assertThat(events).extracting(MessageEvent::view, MessageEvent::offset, MessageEvent::duplicate)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple(View.UNCOMMITTED, 10L, false),
                        org.assertj.core.groups.Tuple.tuple(View.COMMITTED, 10L, false),
                        org.assertj.core.groups.Tuple.tuple(View.COMMITTED, 11L, true));
        assertThat(events.get(0).text()).isEqualTo("Order #1");
        assertThat(events.get(0).key()).isEqualTo("ORD-1");
    }

    @Test
    void skipsRecordsWithoutOurHeaders() {
        listener.handle(View.COMMITTED, new ConsumerRecord<>("web.messages", 0, 1, null, "smoke-token"));

        assertThat(stats.modes().get(DeliveryMode.AT_MOST_ONCE).received()).isZero();
        assertThat(broadcaster.drainBatch()).isEmpty();
    }

    private static ConsumerRecord<String, String> record(long offset, String messageId) {
        ConsumerRecord<String, String> r =
                new ConsumerRecord<>("web.messages", 1, offset, "ORD-1", "{\"text\":\"Order #1\"}");
        r.headers().add("message-id", messageId.getBytes(StandardCharsets.UTF_8));
        r.headers().add("mode", "AT_MOST_ONCE".getBytes(StandardCharsets.UTF_8));
        return r;
    }
}
