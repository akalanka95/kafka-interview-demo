package com.example.kafkademo.model;

/** Which listener saw a record: its consumer's isolation.level. */
public enum View {
    /** read_uncommitted: sees every record written, including aborted transactions. */
    UNCOMMITTED,
    /** read_committed: sees committed records only. Source of truth for stats. */
    COMMITTED
}
