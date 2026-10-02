package com.example.kafkademo.model;

public enum Simulate {
    NONE,
    /** Re-send some records with the same message-id. AT_LEAST_ONCE only. */
    DUPLICATE,
    /** Roll back the last transaction. EXACTLY_ONCE only. */
    ABORT
}
