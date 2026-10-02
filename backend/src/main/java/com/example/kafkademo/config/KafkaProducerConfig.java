package com.example.kafkademo.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

/** One producer factory + template per delivery mode. See docs/backend.md §4. */
@Configuration
public class KafkaProducerConfig {

    private final KafkaProps props;

    public KafkaProducerConfig(KafkaProps props) {
        this.props = props;
    }

    // --- At-most-once: fire and forget -------------------------------------------------------

    @Bean
    public ProducerFactory<String, String> atMostOnceProducerFactory() {
        Map<String, Object> cfg = baseProducerProps();
        cfg.put(ProducerConfig.ACKS_CONFIG, "0");
        cfg.put(ProducerConfig.RETRIES_CONFIG, 0);
        // kafka-clients >= 3.0 defaults idempotence to true, which requires acks=all
        // and fails with a ConfigException next to acks=0. Must be disabled explicitly.
        cfg.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        // Batching keeps records in the client buffer for a moment, which widens the
        // window in which a leader kill loses data (the loss demo, F6).
        cfg.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        cfg.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
        return new DefaultKafkaProducerFactory<>(cfg);
    }

    @Bean
    public KafkaTemplate<String, String> atMostOnceTemplate(
            @Qualifier("atMostOnceProducerFactory") ProducerFactory<String, String> pf) {
        return new KafkaTemplate<>(pf);
    }

    // -----------------------------------------------------------------------------------------

    private Map<String, Object> baseProducerProps() {
        Map<String, Object> cfg = new HashMap<>(props.commonClientProps());
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // Fail a send after 10 s (instead of 60 s) when no broker can supply topic metadata.
        cfg.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000);
        return cfg;
    }
}
