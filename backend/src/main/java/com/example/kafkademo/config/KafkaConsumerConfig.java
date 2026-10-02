package com.example.kafkademo.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;

import java.util.HashMap;
import java.util.Map;

/** Two listener factories that differ only in isolation.level. See docs/backend.md §7. */
@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    /** One consumer thread per partition of web.messages. */
    static final int CONCURRENCY = 3;

    private final KafkaProps props;

    public KafkaConsumerConfig(KafkaProps props) {
        this.props = props;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> uncommittedFactory() {
        return factory("read_uncommitted");
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> committedFactory() {
        return factory("read_committed");
    }

    private ConcurrentKafkaListenerContainerFactory<String, String> factory(String isolationLevel) {
        Map<String, Object> cfg = new HashMap<>(props.commonClientProps());
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        cfg.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);

        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(cfg));
        factory.setConcurrency(CONCURRENCY);
        // Commit once per poll, after every record in it was handled: still at-least-once, but one
        // commit per batch instead of one per record (RECORD made 10 000-message runs lag).
        factory.getContainerProperties().setAckMode(AckMode.BATCH);
        return factory;
    }
}
