package com.example.kafkademo.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Commits one empty transaction in the background at startup. That initialises a transactional
 * producer (and, on a fresh cluster, makes the brokers create __transaction_state) before the first
 * EXACTLY_ONCE request, so that request doesn't pay the multi-second setup or time out.
 */
@Component
public class TransactionWarmup {

    private static final Logger log = LoggerFactory.getLogger(TransactionWarmup.class);

    private final KafkaTemplate<String, String> exactlyOnceTemplate;

    public TransactionWarmup(@Qualifier("exactlyOnceTemplate") KafkaTemplate<String, String> exactlyOnceTemplate) {
        this.exactlyOnceTemplate = exactlyOnceTemplate;
    }

    @EventListener(ApplicationReadyEvent.class)
    void warmUp() {
        Thread thread = new Thread(() -> {
            try {
                exactlyOnceTemplate.executeInTransaction(ops -> null);
                log.info("transactional producer ready");
            } catch (Exception e) {
                // Not fatal: the first EXACTLY_ONCE request will retry the setup.
                log.warn("transaction warm-up failed: {}", e.toString());
            }
        }, "tx-warmup");
        thread.setDaemon(true);
        thread.start();
    }
}
