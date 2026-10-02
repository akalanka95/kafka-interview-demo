package com.example.kafkademo.api;

import com.example.kafkademo.consumer.SseBroadcaster;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@Tag(name = "Stream", description = "Consumed records, pushed to the browser")
@RestController
public class StreamController {

    private final SseBroadcaster broadcaster;

    public StreamController(SseBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
    }

    @Operation(summary = "Server-Sent Events stream",
            description = "`event: batch` → JSON array of MessageEvent, every 100 ms when non-empty. "
                    + "`event: ping` → every 15 s. Swagger UI can't render a stream: use `curl.exe -N http://localhost:8080/api/stream`.")
    @GetMapping(path = "/api/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        return broadcaster.register();
    }
}
