package com.example.kafkademo.service;

import com.example.kafkademo.config.KafkaProps;
import com.example.kafkademo.model.DeliveryMode;
import com.example.kafkademo.model.MessageRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ProducerServiceTest {

    private final KafkaProps props = new KafkaProps("localhost:19092", "web-app", "pw", "web.messages", List.of(1, 2, 3));
    private final ProducerService service = new ProducerService(props, new ObjectMapper(), new StatsService(), new AbortedRegistry(), null, null, null);

    @Test
    void buildsNumberedRecordsWithHeaders() {
        List<ProducerRecord<String, String>> records =
                service.buildRecords(new MessageRequest("Order", "ORD-1", DeliveryMode.AT_MOST_ONCE, 3, null));

        assertThat(records).hasSize(3);
        assertThat(records).allSatisfy(r -> {
            assertThat(r.topic()).isEqualTo("web.messages");
            assertThat(r.key()).isEqualTo("ORD-1");
            assertThat(header(r, "mode")).isEqualTo("AT_MOST_ONCE");
            assertThat(header(r, "produced-at")).isNotBlank();
        });
        assertThat(records.get(0).value()).isEqualTo("{\"text\":\"Order #1\"}");
        assertThat(records.get(2).value()).isEqualTo("{\"text\":\"Order #3\"}");
        assertThat(records.stream().map(r -> header(r, "message-id")).distinct()).hasSize(3);
    }

    @Test
    void singleRecordKeepsTextUnchanged() {
        var records = service.buildRecords(new MessageRequest("hi", null, DeliveryMode.AT_MOST_ONCE, 1, null));
        assertThat(records.get(0).value()).isEqualTo("{\"text\":\"hi\"}");
        assertThat(records.get(0).key()).isNull();
    }

    @Test
    void duplicateIndicesPickTwoPercentOfAckedRecordsSpreadEvenly() {
        int[] partitions = new int[500];
        assertThat(ProducerService.duplicateIndices(partitions)).containsExactly(0, 50, 100, 150, 200, 250, 300, 350, 400, 450);
    }

    @Test
    void duplicateIndicesPickAtLeastOneAndSkipFailedRecords() {
        assertThat(ProducerService.duplicateIndices(new int[] {-1, 2, -1})).containsExactly(1);
        assertThat(ProducerService.duplicateIndices(new int[] {-1, -1})).isEmpty();
        assertThat(ProducerService.duplicateIndices(new int[0])).isEmpty();
    }

    private static String header(ProducerRecord<?, ?> r, String name) {
        return new String(r.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }
}
