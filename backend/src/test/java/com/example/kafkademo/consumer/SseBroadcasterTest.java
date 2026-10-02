package com.example.kafkademo.consumer;

import com.example.kafkademo.model.DeliveryMode;
import com.example.kafkademo.model.MessageEvent;
import com.example.kafkademo.model.View;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SseBroadcasterTest {

    @Test
    void doesNotBufferWithoutSubscribers() {
        SseBroadcaster b = new SseBroadcaster();
        b.enqueue(event(View.COMMITTED, 0));
        assertThat(b.drainBatch()).isEmpty();
    }

    @Test
    void capsEachViewSeparatelyAndCountsDropped() {
        SseBroadcaster b = new SseBroadcaster();
        b.register();
        int extra = 10;
        for (int i = 0; i < SseBroadcaster.MAX_PER_VIEW + extra; i++) b.enqueue(event(View.UNCOMMITTED, i));
        for (int i = 0; i < 5; i++) b.enqueue(event(View.COMMITTED, i));

        List<MessageEvent> batch = b.drainBatch();

        assertThat(batch).filteredOn(e -> e.view() == View.UNCOMMITTED).hasSize(SseBroadcaster.MAX_PER_VIEW);
        assertThat(batch).filteredOn(e -> e.view() == View.COMMITTED).hasSize(5);
        assertThat(b.droppedForUi()).isEqualTo(extra);
        assertThat(b.drainBatch()).isEmpty();   // queue fully drained, overflow discarded
    }

    static MessageEvent event(View view, long offset) {
        return new MessageEvent(view, "id-" + offset, "t", null, DeliveryMode.AT_MOST_ONCE,
                "web.messages", 0, offset, 0L, false, false, null, null, false);
    }
}
