package com.example.kafkademo.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.util.List;
import java.util.Map;

@Validated
@ConfigurationProperties("demo.kafka")
public record KafkaProps(
        @NotBlank String bootstrapServers,
        @NotBlank String username,
        @NotBlank String password,
        @NotBlank String topic,
        @NotEmpty List<Integer> expectedBrokerIds) {

    /** Bootstrap + SASL/SCRAM settings shared by every producer, consumer and admin client. */
    public Map<String, Object> commonClientProps() {
        String jaas = "org.apache.kafka.common.security.scram.ScramLoginModule required "
                + "username=\"" + username + "\" password=\"" + password + "\";";
        return Map.of(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT",
                SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512",
                SaslConfigs.SASL_JAAS_CONFIG, jaas);
    }
}
