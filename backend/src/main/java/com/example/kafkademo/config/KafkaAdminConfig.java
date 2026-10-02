package com.example.kafkademo.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

/** AdminClient for the broker health chips (GET /api/cluster). */
@Configuration
public class KafkaAdminConfig {

    @Bean(destroyMethod = "close")
    public AdminClient adminClient(KafkaProps props) {
        Map<String, Object> cfg = new HashMap<>(props.commonClientProps());
        // Polled every 2 s: give up quickly instead of piling up calls while the cluster is down.
        cfg.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 2_000);
        cfg.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 2_000);
        return AdminClient.create(cfg);
    }
}
