package com.example.kafkademo.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/** Body of POST /api/messages. */
public record MessageRequest(
        @NotBlank @Size(max = 500) String text,
        @Size(max = 200) String key,
        @NotNull DeliveryMode mode,
        @Min(1) @Max(10_000) Integer count,
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
