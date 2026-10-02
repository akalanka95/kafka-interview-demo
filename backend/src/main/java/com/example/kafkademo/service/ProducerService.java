package com.example.kafkademo.service;

import com.example.kafkademo.config.KafkaProps;
import com.example.kafkademo.model.DeliveryMode;
import com.example.kafkademo.model.MessageRequest;
import com.example.kafkademo.model.RunAllRequest;
import com.example.kafkademo.model.SendResponse;
import com.example.kafkademo.model.SendResponse.RecordResult;
import com.example.kafkademo.model.Simulate;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaOperations;
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
    /** Records per transaction in EXACTLY_ONCE. */
    static final int TX_CHUNK = 100;
    /** Share of acked records re-sent by Simulate.DUPLICATE (at least one). */
    static final double DUPLICATE_RATIO = 0.02;
    private static final long AWAIT_TIMEOUT_MS = 130_000;   // > default delivery.timeout.ms (120 s)

    private final KafkaProps props;
    private final ObjectMapper json;
    private final StatsService stats;
    private final AbortedRegistry abortedRegistry;
    private final KafkaTemplate<String, String> atMostOnceTemplate;
    private final KafkaTemplate<String, String> atLeastOnceTemplate;
    private final KafkaTemplate<String, String> exactlyOnceTemplate;

    public ProducerService(KafkaProps props,
                           ObjectMapper json,
                           StatsService stats,
                           AbortedRegistry abortedRegistry,
                           @Qualifier("atMostOnceTemplate") KafkaTemplate<String, String> atMostOnceTemplate,
                           @Qualifier("atLeastOnceTemplate") KafkaTemplate<String, String> atLeastOnceTemplate,
                           @Qualifier("exactlyOnceTemplate") KafkaTemplate<String, String> exactlyOnceTemplate) {
        this.props = props;
        this.json = json;
        this.stats = stats;
        this.abortedRegistry = abortedRegistry;
        this.atMostOnceTemplate = atMostOnceTemplate;
        this.atLeastOnceTemplate = atLeastOnceTemplate;
        this.exactlyOnceTemplate = exactlyOnceTemplate;
    }

    public SendResponse send(MessageRequest req) {
        stats.requestStarted();
        try {
            return switch (req.mode()) {
                case AT_MOST_ONCE -> sendAsync(atMostOnceTemplate, req);
                case AT_LEAST_ONCE -> sendAsync(atLeastOnceTemplate, req);
                case EXACTLY_ONCE -> sendTransactional(req);
            };
        } finally {
            stats.requestFinished();
        }
    }

    /** The three modes one after another, simulate=NONE. Backs the "Run all 3 modes" button. */
    public List<SendResponse> runAll(RunAllRequest req) {
        List<SendResponse> results = new ArrayList<>(DeliveryMode.values().length);
        for (DeliveryMode mode : DeliveryMode.values()) {
            results.add(send(new MessageRequest(req.text(), req.key(), mode, req.count(), Simulate.NONE)));
        }
        return results;
    }

    // --- AT_MOST_ONCE / AT_LEAST_ONCE ---------------------------------------------------------

    /** Pipelined async sends; awaits every future and counts acked / failed. */
    private SendResponse sendAsync(KafkaTemplate<String, String> template, MessageRequest req) {
        long start = System.nanoTime();
        List<ProducerRecord<String, String>> records = buildRecords(req);
        // Counted before sending, so a fast consumer can't make received > sent. A record that fails
        // client-side still counts as sent: it was attempted and never arrives, i.e. it's lost.
        stats.sent(req.mode(), records.size());

        Outcome outcome = awaitAll(records, sendAll(template, records));
        if (outcome.acked == 0 && outcome.firstError != null) {
            throw new ProducerFailedException(outcome.firstError);
        }

        int duplicatesInjected = req.simulate() == Simulate.DUPLICATE
                ? injectDuplicates(template, records, outcome)
                : 0;
        return new SendResponse(req.mode(), records.size(), outcome.acked, outcome.failed,
                duplicatesInjected, 0, elapsedMs(start), outcome.sample);
    }

    /**
     * Re-sends ~2% of the acked records with the SAME message-id to the same partition. The copy
     * gets a new offset, which is exactly what a producer-retry duplicate looks like to a consumer.
     * Not counted as sent: the number of logical messages is unchanged.
     */
    private int injectDuplicates(KafkaTemplate<String, String> template,
                                 List<ProducerRecord<String, String>> records, Outcome original) {
        List<ProducerRecord<String, String>> copies = new ArrayList<>();
        for (int i : duplicateIndices(original.partitions)) {
            copies.add(copyOf(records.get(i), original.partitions[i]));
        }
        return awaitAll(copies, sendAll(template, copies)).acked;
    }

    /** Evenly spread indices of acked records ({@code partitions[i] >= 0}), DUPLICATE_RATIO of them, at least one. */
    static List<Integer> duplicateIndices(int[] partitions) {
        List<Integer> acked = new ArrayList<>();
        for (int i = 0; i < partitions.length; i++) {
            if (partitions[i] >= 0) acked.add(i);
        }
        if (acked.isEmpty()) return List.of();
        int n = Math.max(1, (int) Math.round(acked.size() * DUPLICATE_RATIO));
        double step = (double) acked.size() / n;
        List<Integer> picked = new ArrayList<>(n);
        for (int k = 0; k < n; k++) picked.add(acked.get((int) (k * step)));
        return picked;
    }

    private static ProducerRecord<String, String> copyOf(ProducerRecord<String, String> r, int partition) {
        return new ProducerRecord<>(r.topic(), partition, r.key(), r.value(), new RecordHeaders(r.headers().toArray()));
    }

    // --- EXACTLY_ONCE -------------------------------------------------------------------------

    /**
     * Chunks of TX_CHUNK records, one transaction each. With Simulate.ABORT the last chunk is
     * written and then rolled back (count=1 → the single message aborts). A real failure aborts the
     * current chunk and stops; earlier chunks stay committed.
     */
    private SendResponse sendTransactional(MessageRequest req) {
        long start = System.nanoTime();
        List<ProducerRecord<String, String>> records = buildRecords(req);
        List<RecordResult> sample = new ArrayList<>(SAMPLE_SIZE);
        int committed = 0;
        int aborted = 0;
        Throwable error = null;

        for (int from = 0; from < records.size(); from += TX_CHUNK) {
            List<ProducerRecord<String, String>> chunk = records.subList(from, Math.min(from + TX_CHUNK, records.size()));
            boolean abort = req.simulate() == Simulate.ABORT && from + TX_CHUNK >= records.size();
            try {
                List<RecordMetadata> written = exactlyOnceTemplate.executeInTransaction(ops -> {
                    // Registered before the records exist, so the read_uncommitted listener can never
                    // see one of them without knowing it will be aborted.
                    if (abort) abortedRegistry.registerAll(chunk.stream().map(ProducerService::messageId).toList());
                    List<CompletableFuture<SendResult<String, String>>> futures = sendAll(ops, chunk);
                    // Push the batch to the brokers now: records still in the client buffer at abort
                    // time are discarded unsent, and read_uncommitted would have nothing to show.
                    ops.flush();
                    Outcome outcome = awaitAll(chunk, futures);
                    if (outcome.firstError != null) throw new ProducerFailedException(outcome.firstError);
                    if (abort) throw new SimulatedAbortException();
                    return outcome.metadata;
                });
                // After the commit: only committed records count as sent. The read_committed listener
                // may get there a moment earlier; lost is clamped at 0 and the UI dims it while in flight.
                stats.sent(req.mode(), chunk.size());
                committed += chunk.size();
                for (int i = 0; i < written.size() && sample.size() < SAMPLE_SIZE; i++) {
                    RecordMetadata md = written.get(i);
                    sample.add(new RecordResult(messageId(chunk.get(i)), md.partition(), Long.toString(md.offset())));
                }
            } catch (RuntimeException e) {
                if (hasCause(e, SimulatedAbortException.class)) {
                    stats.aborted(req.mode(), chunk.size());
                    aborted += chunk.size();
                } else {
                    error = e instanceof ProducerFailedException pfe ? pfe.getCause() : e;
                    break;
                }
            }
        }

        if (committed == 0 && aborted == 0 && error != null) {
            throw new ProducerFailedException(error);
        }
        int failed = records.size() - committed - aborted;
        return new SendResponse(req.mode(), records.size(), committed, failed, 0, aborted, elapsedMs(start), sample);
    }

    /** Thrown inside a transaction callback to make Spring abort it. */
    static final class SimulatedAbortException extends RuntimeException {
        SimulatedAbortException() {
            super("simulated abort", null, false, false);
        }
    }

    private static boolean hasCause(Throwable t, Class<? extends Throwable> type) {
        for (; t != null; t = t.getCause()) {
            if (type.isInstance(t)) return true;
        }
        return false;
    }

    // --- shared -------------------------------------------------------------------------------

    private static List<CompletableFuture<SendResult<String, String>>> sendAll(
            KafkaOperations<String, String> template,
            List<ProducerRecord<String, String>> records) {
        List<CompletableFuture<SendResult<String, String>>> futures = new ArrayList<>(records.size());
        for (ProducerRecord<String, String> record : records) {
            futures.add(template.send(record));
        }
        return futures;
    }

    /** Result of awaiting one batch of futures. */
    private static final class Outcome {
        int acked;
        int failed;
        Throwable firstError;
        final List<RecordResult> sample = new ArrayList<>(SAMPLE_SIZE);
        /** Per record: partition it landed on, or -1 if it failed. */
        final int[] partitions;
        /** Metadata of acked records, in order (complete only when nothing failed). */
        final List<RecordMetadata> metadata = new ArrayList<>();

        Outcome(int size) {
            partitions = new int[size];
        }
    }

    private static Outcome awaitAll(List<ProducerRecord<String, String>> records,
                                    List<CompletableFuture<SendResult<String, String>>> futures) {
        Outcome o = new Outcome(futures.size());
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;

        for (int i = 0; i < futures.size(); i++) {
            o.partitions[i] = -1;
            try {
                long remaining = Math.max(0, deadline - System.currentTimeMillis());
                RecordMetadata md = futures.get(i).get(remaining, TimeUnit.MILLISECONDS).getRecordMetadata();
                o.acked++;
                o.partitions[i] = md.partition();
                o.metadata.add(md);
                if (o.sample.size() < SAMPLE_SIZE) {
                    // acks=0: the future completes once the record is handed to the socket, and
                    // the broker never returns an offset (metadata reports -1).
                    String offset = md.hasOffset() ? Long.toString(md.offset()) : "unknown";
                    o.sample.add(new RecordResult(messageId(records.get(i)), md.partition(), offset));
                }
            } catch (ExecutionException e) {
                o.failed++;
                if (o.firstError == null) o.firstError = e.getCause();
            } catch (TimeoutException e) {
                o.failed++;
                if (o.firstError == null) o.firstError = e;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while awaiting sends", e);
            }
        }
        return o;
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

    private static long elapsedMs(long startNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }

    static String messageId(ProducerRecord<?, ?> record) {
        return new String(record.headers().lastHeader("message-id").value(), StandardCharsets.UTF_8);
    }
}
