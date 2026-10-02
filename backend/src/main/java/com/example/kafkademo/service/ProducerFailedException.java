package com.example.kafkademo.service;

/** Every record of a request failed. The cause is the first Kafka error seen. */
public class ProducerFailedException extends RuntimeException {

    public ProducerFailedException(Throwable cause) {
        super(cause.getMessage(), cause);
    }
}
