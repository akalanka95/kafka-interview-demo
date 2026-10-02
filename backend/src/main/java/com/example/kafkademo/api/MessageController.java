package com.example.kafkademo.api;

import com.example.kafkademo.api.ApiExceptionHandler.ApiError;
import com.example.kafkademo.model.MessageRequest;
import com.example.kafkademo.model.RunAllRequest;
import com.example.kafkademo.model.SendResponse;
import com.example.kafkademo.service.ProducerService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Tag(name = "Messages", description = "Produce to web.messages with a chosen delivery mode")
@RestController
@RequestMapping("/api/messages")
public class MessageController {

    private final ProducerService producer;

    public MessageController(ProducerService producer) {
        this.producer = producer;
    }

    @Operation(summary = "Send 1–10 000 messages",
            description = "AT_MOST_ONCE: acks=0, offsets come back as \"unknown\". "
                    + "AT_LEAST_ONCE + DUPLICATE re-sends ~2% with the same message-id. "
                    + "EXACTLY_ONCE: transactions of 100; + ABORT rolls back the last one. "
                    + "Returns 200 with failed > 0 on partial failure; an error status only when every record fails.")
    @ApiResponse(responseCode = "200", description = "Batch sent (check acked / failed)")
    @ApiResponse(responseCode = "400", description = "Validation failed",
            content = @Content(schema = @Schema(implementation = ApiError.class)))
    @ApiResponse(responseCode = "403", description = "Kafka ACL denied the write",
            content = @Content(schema = @Schema(implementation = ApiError.class)))
    @ApiResponse(responseCode = "503", description = "Not enough in-sync replicas",
            content = @Content(schema = @Schema(implementation = ApiError.class)))
    @ApiResponse(responseCode = "504", description = "Broker / quorum unavailable",
            content = @Content(schema = @Schema(implementation = ApiError.class)))
    @PostMapping
    public SendResponse send(@Valid @RequestBody MessageRequest request) {
        return producer.send(request);
    }

    @Operation(summary = "Send the same batch in all 3 modes",
            description = "AT_MOST_ONCE, AT_LEAST_ONCE, EXACTLY_ONCE one after another with simulate=NONE. "
                    + "Returns one result per mode, in that order. Fails as a whole if one mode fails completely.")
    @ApiResponse(responseCode = "200", description = "One SendResponse per mode")
    @ApiResponse(responseCode = "400", description = "Validation failed",
            content = @Content(schema = @Schema(implementation = ApiError.class)))
    @PostMapping("/run-all")
    public List<SendResponse> runAll(@Valid @RequestBody RunAllRequest request) {
        return producer.runAll(request);
    }
}
