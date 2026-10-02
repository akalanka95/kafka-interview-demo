package com.example.kafkademo.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/** Body of POST /api/messages. */
public record MessageRequest(
        @Schema(example = "Order #1042 created", description = "With count > 1 the server appends \" #n\"")
        @NotBlank @Size(max = 500) String text,
        @Schema(example = "ORD-1042", description = "Optional. Null spreads records across partitions")
        @Size(max = 200) String key,
        @Schema(example = "AT_MOST_ONCE")
        @NotNull DeliveryMode mode,
        @Schema(example = "5", defaultValue = "1")
        @Min(1) @Max(10_000) Integer count,
        @Schema(example = "NONE", defaultValue = "NONE",
                description = "DUPLICATE needs AT_LEAST_ONCE, ABORT needs EXACTLY_ONCE")
        Simulate simulate) {

    public MessageRequest {
        if (key != null && key.isBlank()) key = null;   // blank key → default partitioner
        if (count == null) count = 1;
        if (simulate == null) simulate = Simulate.NONE;
    }

    @JsonIgnore
    @AssertTrue(message = "DUPLICATE requires AT_LEAST_ONCE, ABORT requires EXACTLY_ONCE")
    public boolean isSimulateValidForMode() {
        return switch (simulate) {
            case NONE -> true;
            case DUPLICATE -> mode == DeliveryMode.AT_LEAST_ONCE;
            case ABORT -> mode == DeliveryMode.EXACTLY_ONCE;
        };
    }
}
