package com.example.kafkademo.model;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/** Body of POST /api/messages/run-all: a MessageRequest without mode and simulate. */
public record RunAllRequest(
        @Schema(example = "Order #1042 created", description = "With count > 1 the server appends \" #n\"")
        @NotBlank @Size(max = 500) String text,
        @Schema(example = "ORD-1042", description = "Optional. Null spreads records across partitions")
        @Size(max = 200) String key,
        @Schema(example = "5", defaultValue = "1")
        @Min(1) @Max(10_000) Integer count) {

    public RunAllRequest {
        if (key != null && key.isBlank()) key = null;
        if (count == null) count = 1;
    }
}
