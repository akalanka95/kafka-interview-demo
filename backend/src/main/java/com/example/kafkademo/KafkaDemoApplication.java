package com.example.kafkademo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

// Kafka auto-config is excluded: each delivery mode gets its own explicitly configured
// producer factory and template (see config/), and Boot's single-factory beans would clash.
@SpringBootApplication(exclude = KafkaAutoConfiguration.class)
@ConfigurationPropertiesScan
@EnableScheduling   // SSE batch flush + ping
public class KafkaDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaDemoApplication.class, args);
    }
}
