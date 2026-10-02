package com.example.kafkademo.model;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class MessageRequestTest {

    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    @Test
    void appliesDefaults() {
        MessageRequest r = new MessageRequest("hi", "  ", DeliveryMode.AT_MOST_ONCE, null, null);
        assertThat(r.count()).isEqualTo(1);
        assertThat(r.simulate()).isEqualTo(Simulate.NONE);
        assertThat(r.key()).isNull();
        assertThat(validator.validate(r)).isEmpty();
    }

    @Test
    void rejectsBlankTextAndOutOfRangeCount() {
        assertThat(paths(new MessageRequest("", null, DeliveryMode.AT_MOST_ONCE, 0, null)))
                .containsExactlyInAnyOrder("text", "count");
        assertThat(paths(new MessageRequest("x", null, DeliveryMode.AT_MOST_ONCE, 10_001, null)))
                .containsExactly("count");
    }

    @Test
    void rejectsSimulateNotAllowedForMode() {
        assertThat(paths(new MessageRequest("x", null, DeliveryMode.AT_MOST_ONCE, 1, Simulate.DUPLICATE)))
                .containsExactly("simulateValidForMode");
        assertThat(paths(new MessageRequest("x", null, DeliveryMode.AT_LEAST_ONCE, 1, Simulate.ABORT)))
                .containsExactly("simulateValidForMode");
        assertThat(validator.validate(new MessageRequest("x", null, DeliveryMode.AT_LEAST_ONCE, 1, Simulate.DUPLICATE)))
                .isEmpty();
        assertThat(validator.validate(new MessageRequest("x", null, DeliveryMode.EXACTLY_ONCE, 1, Simulate.ABORT)))
                .isEmpty();
    }

    private Set<String> paths(MessageRequest r) {
        return validator.validate(r).stream()
                .map(ConstraintViolation::getPropertyPath)
                .map(Object::toString)
                .collect(java.util.stream.Collectors.toSet());
    }
}
