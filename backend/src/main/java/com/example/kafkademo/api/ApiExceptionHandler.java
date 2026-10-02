package com.example.kafkademo.api;

import com.example.kafkademo.service.ProducerFailedException;
import org.apache.kafka.common.errors.NotEnoughReplicasAfterAppendException;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.stream.Collectors;

/** Maps failures to {@code {"error": "<simple class name>", "message": "..."}}. */
@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    public record ApiError(String error, String message) {}

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ResponseEntity<ApiError> validation(MethodArgumentNotValidException e) {
        String message = e.getBindingResult().getAllErrors().stream()
                .map(err -> err instanceof org.springframework.validation.FieldError fe
                        ? fe.getField() + ": " + fe.getDefaultMessage()
                        : err.getDefaultMessage())
                .sorted()
                .collect(Collectors.joining("; "));
        return error(HttpStatus.BAD_REQUEST, "ValidationException", message);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    ResponseEntity<ApiError> unreadable(HttpMessageNotReadableException e) {
        return error(HttpStatus.BAD_REQUEST, "ValidationException", "malformed JSON or unknown enum value");
    }

    @ExceptionHandler(UnsupportedOperationException.class)
    ResponseEntity<ApiError> notImplemented(UnsupportedOperationException e) {
        return error(HttpStatus.NOT_IMPLEMENTED, "NotImplemented", e.getMessage());
    }

    @ExceptionHandler(ProducerFailedException.class)
    ResponseEntity<ApiError> kafka(ProducerFailedException e) {
        // Spring Kafka wraps the client error (KafkaProducerException), so walk the cause chain.
        for (Throwable t = e.getCause(); t != null; t = t.getCause()) {
            if (t instanceof TopicAuthorizationException) {
                return error(HttpStatus.FORBIDDEN, t, "not authorized: " + t.getMessage());
            }
            if (t instanceof NotEnoughReplicasException || t instanceof NotEnoughReplicasAfterAppendException) {
                return error(HttpStatus.SERVICE_UNAVAILABLE, t, "not enough in-sync replicas: " + t.getMessage());
            }
            if (t instanceof TimeoutException || t instanceof java.util.concurrent.TimeoutException) {
                return error(HttpStatus.GATEWAY_TIMEOUT, t, "broker / quorum unavailable: " + t.getMessage());
            }
        }
        log.error("produce failed", e);
        Throwable root = rootCause(e);
        return error(HttpStatus.INTERNAL_SERVER_ERROR, root, root.getMessage());
    }

    private static Throwable rootCause(Throwable t) {
        while (t.getCause() != null && t.getCause() != t) t = t.getCause();
        return t;
    }

    private static ResponseEntity<ApiError> error(HttpStatus status, Throwable t, String message) {
        return error(status, t.getClass().getSimpleName(), message);
    }

    private static ResponseEntity<ApiError> error(HttpStatus status, String error, String message) {
        return ResponseEntity.status(status).body(new ApiError(error, message));
    }
}
