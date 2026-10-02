package com.example.kafkademo.service;

import com.example.kafkademo.config.KafkaProps;
import com.example.kafkademo.model.DeliveryMode;
import com.example.kafkademo.model.MessageRequest;
import com.example.kafkademo.model.SendResponse;
import com.example.kafkademo.model.SendResponse.RecordResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Builds records, picks the template for the requested mode, sends and collects results. */
@Service
public class ProducerService {

    static final int SAMPLE_SIZE = 20;
    private static final long AWAIT_TIMEOUT_MS = 130_000;   // > default delivery.timeout.ms (120 s)

    private final KafkaProps props;
    private final ObjectMapper json;
    private final StatsService stats;
    private final KafkaTemplate<String, String> atMostOnceTemplate;

    public ProducerService(KafkaProps props,
                           ObjectMapper json,
                           StatsService stats,
                           @Qualifier("atMostOnceTemplate") KafkaTemplate<String, String> atMostOnceTemplate) {
        this.props = props;
        this.json = json;
        this.stats = stats;
        this.atMostOnceTemplate = atMostOnceTemplate;
    }

    public SendResponse send(MessageRequest req) {
        if (req.mode() != DeliveryMode.AT_MOST_ONCE) {
            throw new UnsupportedOperationException(req.mode() + " is not implemented yet");
        }
        stats.requestStarted();
        try {
            return sendAsync(atMostOnceTemplate, req);
        } finally {
            stats.requestFinished();
        }
    }

    /** Pipelined async sends; awaits every future and counts acked / failed. */
    private SendResponse sendAsync(KafkaTemplate<String, String> template, MessageRequest req) {
        long start = System.nanoTime();
        List<ProducerRecord<String, String>> records = buildRecords(req);
        // Counted before sending, so a fast consumer can't make received > sent. A record that fails
        // client-side still counts as sent: it was attempted and never arrives, i.e. it's lost.
        stats.sent(req.mode(), records.size());

        List<CompletableFuture<SendResult<String, String>>> futures = new ArrayList<>(records.size());
        for (ProducerRecord<String, String> record : records) {
            futures.add(template.send(record));
        }

        int acked = 0;
        int failed = 0;
        Throwable firstError = null;
        List<RecordResult> sample = new ArrayList<>(SAMPLE_SIZE);
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;

        for (int i = 0; i < futures.size(); i++) {
            try {
                long remaining = Math.max(0, deadline - System.currentTimeMillis());
                RecordMetadata md = futures.get(i).get(remaining, TimeUnit.MILLISECONDS).getRecordMetadata();
                acked++;
                if (sample.size() < SAMPLE_SIZE) {
                    // acks=0: the future completes once the record is handed to the socket, and
                    // the broker never returns an offset (metadata reports -1).
                    String offset = md.hasOffset() ? Long.toString(md.offset()) : "unknown";
                    sample.add(new RecordResult(messageId(records.get(i)), md.partition(), offset));
                }
            } catch (ExecutionException e) {
                failed++;
                if (firstError == null) firstError = e.getCause();
            } catch (TimeoutException e) {
                failed++;
                if (firstError == null) firstError = e;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while awaiting sends", e);
            }
        }

        if (acked == 0 && firstError != null) {
            throw new ProducerFailedException(firstError);
        }
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        return new SendResponse(req.mode(), records.size(), acked, failed, 0, 0, elapsedMs, sample);
    }

    List<ProducerRecord<String, String>> buildRecords(MessageRequest req) {
        List<ProducerRecord<String, String>> records = new ArrayList<>(req.count());
        String producedAt = Instant.now().toString();
        for (int i = 1; i <= req.count(); i++) {
            String text = req.count() > 1 ? req.text() + " #" + i : req.text();
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(props.topic(), req.key(), toJson(text));
            record.headers()
                    .add("message-id", UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8))
                    .add("mode", req.mode().name().getBytes(StandardCharsets.UTF_8))
                    .add("produced-at", producedAt.getBytes(StandardCharsets.UTF_8));
            records.add(record);
        }
        return records;
    }

    private String toJson(String text) {
        try {
            return json.writeValueAsString(Map.of("text", text));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String messageId(ProducerRecord<?, ?> record) {
        return new String(record.headers().lastHeader("message-id").value(), StandardCharsets.UTF_8);
    }
}
