package com.example.kafkademo.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Swagger UI at http://localhost:8080/swagger-ui.html, spec at /v3/api-docs. */
@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI kafkaDemoOpenApi(KafkaProps props) {
        return new OpenAPI().info(new Info()
                .title("Kafka delivery lab API")
                .version("0.0.1")
                .description("Produces to `" + props.topic() + "` as Kafka user `" + props.username()
                        + "` with a selectable delivery mode. See docs/backend.md."));
    }
}
