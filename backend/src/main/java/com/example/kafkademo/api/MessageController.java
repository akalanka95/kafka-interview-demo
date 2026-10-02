package com.example.kafkademo.api;

import com.example.kafkademo.api.ApiExceptionHandler.ApiError;
import com.example.kafkademo.model.MessageRequest;
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
                    + "Returns 200 with failed > 0 on partial failure; an error status only when every record fails.")
    @ApiResponse(responseCode = "200", description = "Batch sent (check acked / failed)")
    @ApiResponse(responseCode = "400", description = "Validation failed",
            content = @Content(schema = @Schema(implementation = ApiError.class)))
    @ApiResponse(responseCode = "403", description = "Kafka ACL denied the write",
            content = @Content(schema = @Schema(implementation = ApiError.class)))
    @ApiResponse(responseCode = "501", description = "Mode not implemented yet",
            content = @Content(schema = @Schema(implementation = ApiError.class)))
    @ApiResponse(responseCode = "503", description = "Not enough in-sync replicas",
            content = @Content(schema = @Schema(implementation = ApiError.class)))
    @ApiResponse(responseCode = "504", description = "Broker / quorum unavailable",
            content = @Content(schema = @Schema(implementation = ApiError.class)))
    @PostMapping
    public SendResponse send(@Valid @RequestBody MessageRequest request) {
        return producer.send(request);
    }
}
