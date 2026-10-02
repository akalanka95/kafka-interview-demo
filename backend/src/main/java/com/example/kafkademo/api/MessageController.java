package com.example.kafkademo.api;

import com.example.kafkademo.model.MessageRequest;
import com.example.kafkademo.model.SendResponse;
import com.example.kafkademo.service.ProducerService;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/messages")
public class MessageController {

    private final ProducerService producer;

    public MessageController(ProducerService producer) {
        this.producer = producer;
    }

    @PostMapping
    public SendResponse send(@Valid @RequestBody MessageRequest request) {
        return producer.send(request);
    }
}
